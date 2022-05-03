package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"time"
	"sync"
)

// ApplyLoop keep fetching command or snapshot from applyCha
func (kv *ShardKV) ApplyLoop() {

	for msg := range kv.applyCh {
		//get command from applyCh either command or snapshot
		if msg.CommandValid {
			kv.ApplyCommand(msg)
		} else if msg.SnapshotValid {
			kv.ApplySnapShot(msg)
		}
	}
}

// ApplySnapShot GetSnapShot from rf.applyCh
func (kv *ShardKV) ApplySnapShot(msg raft.ApplyMsg) {
	//do not use kv.mu.lock before operation raft
	D2Printf("[=]applySnapShot")
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		snapshot := msg.Snapshot
		D2Printf("[=============]DecodeSnapShot[================]")
		kv.DecodeSnapshot(snapshot)
		kv.lastIncludedIndex = msg.SnapshotIndex
	}
}

// DecodeSnapshot according to input
func (kv *ShardKV) DecodeSnapshot(snapshot []byte) {
	// DPrintf("KVserver %d, Reading Snapshot", kv.me)
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistKVDB [shardctrler.NShards]Shard
	var latestConfig shardctrler.Config
	var lastConfig shardctrler.Config
	var shardsInclude []int

	if d.Decode(&persistKVDB) != nil ||
		 d.Decode(&latestConfig) != nil || 
		 d.Decode(&lastConfig) != nil ||
		 d.Decode(&shardsInclude) != nil {
		// DPrintf("kv server %d cannot decode", kv.me)
	} else {
		kv.kvDB = persistKVDB
		kv.latestConfig = latestConfig
		kv.lastConfig = lastConfig
		kv.shardsInclude = shardsInclude
		// DPrintf("KVserver: %d, KVDB: %v, lastRequestId: %d", kv.me, persistKVDB, persistLastRequestId)
	}
}

// EncodeSnapshot according to KvDB
// SnapShot include KvDB, lastRequestId map
func (kv *ShardKV) EncodeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.kvDB)
	e.Encode(kv.latestConfig)
	e.Encode(kv.lastConfig)
	e.Encode(kv.shardsInclude)

	data := w.Bytes()
	return data
}

// ExecuteGet fetch the value from kvDB if exists
func (kv *ShardKV) ExecuteGet(op Op) (string, bool) {
	kv.mu.Lock()
	shard := key2shard(op.Key)
	value, exist := kv.kvDB[shard].Data[op.Key]
	kv.kvDB[shard].LastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	if exist {
		DPrintf("GET ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId, op.RequestId, op.Key, value)
	} else {
		DPrintf("GET ClientId :%d ,RequestID :%d ,Key : %v, key not found", op.ClientId, op.RequestId, op.Key)
	}
	return value, exist
}

//check if request is duplicated, if yes return true
func (kv *ShardKV) checkDuplicateRequest(newClientId int64, newRequestId int, shard int) bool {
	kv.mu.Lock()
	// return true if message is duplicate
	lastRequestId, ifClientInRecord := kv.kvDB[shard].LastRequestId[newClientId]
	kv.mu.Unlock()
	if !ifClientInRecord {
		DPrintf("[=]request from[client:%d]with[requestId:%d] duplicate!", newClientId, newRequestId)
		return false
	}
	return newRequestId <= lastRequestId
}

// ApplyCommand execute putAppend, and take snapshot if needed
func (kv *ShardKV) ApplyCommand(message raft.ApplyMsg) {

	//ignore dummy command
	if _, ok := message.Command.(int); ok {
		// a no-op log
		// DPrintf("I AM INT")
		DPrintf("[=][server %d] apply recieve a no-op log", kv.me)
		return
	}

	op := message.Command.(Op)
	DPrintf("[RaftApplyCommand]Server %d, Got Command -> Index:%d, ClientId %d, RequestId %d, Command %v, Key: %v, Value: %v", kv.me, message.CommandIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)

	if message.CommandIndex <= kv.lastIncludedIndex {
		return
	}

	shard := key2shard(op.Key)
	// duplicate command will not be executed
	isRepeated := kv.checkDuplicateRequest(op.ClientId, op.RequestId, shard)
	// if !kv.checkDuplicateRequest(op.ClientId, op.RequestId, shard) {
		// execute command
		if op.Command == "Put" {
			if !isRepeated {
				kv.Put(op)
				shardId := key2shard(op.Key)
				kv.kvDB[shardId].LastRequestId[op.ClientId] = op.RequestId
			}
		
		} else if op.Command == "Append" {
			if !isRepeated {
				kv.Append(op)
				shardId := key2shard(op.Key)
				kv.kvDB[shardId].LastRequestId[op.ClientId] = op.RequestId
			}

		} else if op.Command == "UpdateConfig" {
			kv.UpdateConfig(op)
		} else if op.Command == "InsertShard" {
			kv.InsertShard(op)
		} else if op.Command == "ShardsRecieved" {
			kv.ShardsRecieved(op)
		}
		// else if op.Command == "DeleteShard" {
		// 	kv.deleteShard(op)
		// } else if op.Command == "ShardRecieved" {
		// 	kv.shardSent(op)
		// }
	//}
	//check if raft server needs to make snapshot
	if kv.maxraftstate != -1 {
		if kv.rf.GetRaftSize() > (kv.maxraftstate * 9 / 10) {
			// Send SnapShot Command
			snapshot := kv.EncodeSnapshot()
			kv.rf.Snapshot(message.CommandIndex, snapshot)
		}
	}
	// Send message to the chan of op.ClientId
	kv.SendMsgToWaitChan(op, message.CommandIndex)
}

// SendMsgToWaitChan send each op to waitChan with raftIndex waiting to be fetched
func (kv *ShardKV) SendMsgToWaitChan(op Op, raftIndex int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		// DPrintf("[RaftApplyMsgSendToWaitChan]Server %d, Send CommandIndex:%d, ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)
		ch <- op
	}
	return exist
}

// Put value means replace the value according to key
func (kv *ShardKV) Put(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(op.Key)
	kv.kvDB[shard].Data[op.Key] = op.Value
	kv.kvDB[shard].LastRequestId[op.ClientId] = op.RequestId
	DPrintf("[KVServerExePUT]ClientId :%d ,RequestID :%d ,Key: %v, value: %v", op.ClientId, op.RequestId, op.Key, op.Value)
}

// Append op to kvDB
func (kv *ShardKV) Append(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(op.Key)
	value, exist := kv.kvDB[shard].Data[op.Key]
	if exist {
		kv.kvDB[shard].Data[op.Key] = value + op.Value
	} else {
		kv.kvDB[shard].Data[op.Key] = op.Value
	}
	kv.kvDB[shard].LastRequestId[op.ClientId] = op.RequestId
	DPrintf("[KVServerExeAPPEND]ClientId :%d ,RequestID:%d ,Key: %v, value: %v", op.ClientId, op.RequestId, op.Key, op.Value)
}

func (kv *ShardKV) UpdateConfig(op Op) {
	// kv.setUpdateConcig()
	// defer kv.unsetUpdateConcig()
	newConfig := op.Config
	kv.mu.Lock()
	curConfig := kv.latestConfig.Copy()
	kv.mu.Unlock()

	if newConfig.Num <= curConfig.Num {
		return 
	}
	
	if newConfig.Num > curConfig.Num + 1 {
		DPrintf("[=][newConfig.Num:%d][curConfig.Num:%d]", newConfig.Num, curConfig.Num)
		panic("UpdateConfig get wrong newConfig!")
		return 
	}

	gid := kv.gid
	newShardsInclude := make([]int, shardctrler.NShards)

	// update status of each shard
	kv.mu.Lock()
	for k, shardId :=range newConfig.Shards {
		if shardId == gid {
			newShardsInclude[k] = 1
			if curConfig.Shards[k] != 0 && curConfig.Shards[k] != gid {
				kv.kvDB[k].Status = Recieving
			}
		} else {
			newShardsInclude[k] = 0
			if curConfig.Shards[k] != 0 && curConfig.Shards[k] == gid {
				kv.kvDB[k].Status = Sending
			}
		}
	}

	kv.shardsInclude = newShardsInclude
	// if newConfig.Num == 1 {
	// 	for k:=0; k< shardctrler.NShards; k++ {
	// 		kv.kvDB[k].Status = Normal
	// 	}
	// }
	// kv.configs = append(kv.configs, newConfig)
	kv.lastConfig = kv.latestConfig.Copy()
	kv.latestConfig = newConfig.Copy()
	kv.mu.Unlock()
	DPrintf("=======shard status==========")
	for k:=0; k<shardctrler.NShards; k++ {
		DPrintf("[%d:%d]",k, kv.kvDB[k].Status)
	}
	DPrintf("=============================")
	DPrintf("[=][server:%d] update [config:%d] successfully",kv.me, newConfig.Num)
}

func (kv *ShardKV) InsertShard(op Op) {
	kv.mu.Lock()
	newShard := op.Shard
	newShard.Status = Normal
	id := newShard.Id
	kv.kvDB[id] = newShard
	kv.mu.Unlock()
}

func (kv *ShardKV) ShardsRecieved(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id := op.ShardId
	kv.kvDB[id].Status = Normal
}


// delete part
// 
// func (kv *ShardKV) shardSent(op Op) {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()
// 	id := op.ShardId
// 	kv.kvDB[id].Status = toBeDelete
// }
// func (kv *ShardKV) deleteShard(op Op) {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()
// 	id := op.ShardId
// 	kv.kvDB[id].Status = Normal
// }



func (kv *ShardKV) fetchConfigsLoop() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			if !kv.checkCanUpdateConfig() {
				time.Sleep(fetchConfigsInterval)
				continue
			}
			kv.mu.Lock()
			curConfigNum := kv.latestConfig.Num
			kv.mu.Unlock()
			// D2Printf("[=][server:%d]try to fetch new config", kv.me, )
			newConfig := kv.mck.Query(curConfigNum + 1)
			// try to fetch the new config
			if newConfig.Num == curConfigNum + 1 {
				// kv.setUpdateConcig()
				D2Printf("[=][server:%d]start a updateConfig into raft",kv.me)
				op := Op{
					Command: "UpdateConfig",
					Config: newConfig,
				}
				kv.rf.Start(op)
			}
		}
		time.Sleep(fetchConfigsInterval)
	}
}


// return shardId -> []servers
func (kv *ShardKV) getCurPullingShards() map[int][]string {
	res := map[int][]string{}
	newConfig := kv.latestConfig.Copy()
	lastConfig := kv.lastConfig.Copy()
	DPrintf("[=][newConfig:%v][lastConfig:%v]", newConfig, lastConfig)
	for i, shard := range kv.kvDB {
		if shard.Status == Recieving {
			gid := lastConfig.Shards[i]
			res[i] = append([]string{}, lastConfig.Groups[gid]...)
		}
	}
	return res
}


func (kv *ShardKV) pullingShardsLoop() {
	for !kv.killed() {	
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			shards := kv.getCurPullingShards()
			if len(shards) > 0 {
				// D2Printf("[=][server:%d]try pull new shard [len(shrads):%d][%v]", kv.me, len(shards), shards)
				curConfigNum := kv.latestConfig.Num
				var waitGroup sync.WaitGroup
				waitGroup.Add(1)
				go func(shards map[int][]string, curConfigNum int){ 
					defer waitGroup.Done()
					for shardId, servers := range shards {
						for _, server := range servers {
							// D2Printf("[=][server:%d]send [shard:%d]request to [server:%s]", kv.me, shardId, server)
							dst := kv.make_end(server)
							args := TransferShardsArgs{shardId, curConfigNum}
							reply := TransferShardsReply{}
							if dst.Call("ShardKV.TransferShards", &args, &reply) && reply.Err == OK {
								DPrintf("OK[=][server:%d]recieved [shard:%d] from [server:%s]",kv.me, shardId, server)
								op := Op{
									Command: "InsertShard",
									Shard: reply.Shard,
								}
								kv.rf.Start(op)
							}
						}
					}
				}(shards, curConfigNum)

				kv.mu.Unlock()
				waitGroup.Wait()

				//var waitGroup sync.WaitGroup
				// curConfig := kv.getConfig(-1)
				// for gid, sendShards := range shards {
				// 	go func(gid int, shards []Shard, servers []string){
				// 		waitGroup.Add(1)
				// 		defer waitGroup.Done()
				// 		for _, shard := range shards {
				// 			for _, server := range servers {
				// 				dst := kv.make_end(server)
				// 				reply := TransferShardsReply{}
				// 				args := TransferShardsArgs{shard}
				// 				if dst.Call("ShardKV.TransferShards", &args, &reply) && reply.Err == OK {
				// 					// op := Op{
				// 					// 	Command : "ShardSent",
				// 					// 	ShardId : shard.id,
				// 					// }
				// 					// kv.rf.Start(op)
				// 				}
				// 			}
				// 		}
				// 	}(gid, sendShards, curConfig.Groups[gid])
				// }
				//waitGroup.Wait()
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(sendingShardsInterval)
	}
}


func (kv *ShardKV) checkCanUpdateConfig() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, shard := range kv.kvDB {
		// fix me later when considering delete part
		if shard.Status == Recieving {
			return false
		}
	}
	return true
}
