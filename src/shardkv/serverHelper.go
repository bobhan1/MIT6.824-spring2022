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
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		snapshot := msg.Snapshot
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

	if d.Decode(&persistKVDB) != nil  {
		// DPrintf("kv server %d cannot decode", kv.me)
	} else {
		kv.kvDB = persistKVDB
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
	data := w.Bytes()
	return data
}

// ExecuteGet fetch the value from kvDB if exists
func (kv *ShardKV) ExecuteGet(op Op) (string, bool) {
	kv.mu.Lock()
	shard := key2shard(op.Key)
	value, exist := kv.kvDB[shard].data[op.Key]
	kv.kvDB[shard].lastRequestId[op.ClientId] = op.RequestId
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
	lastRequestId, ifClientInRecord := kv.kvDB[shard].lastRequestId[newClientId]
	kv.mu.Unlock()
	if !ifClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}

// ApplyCommand execute putAppend, and take snapshot if needed
func (kv *ShardKV) ApplyCommand(message raft.ApplyMsg) {

	//ignore dummy command
	if _, ok := message.Command.(int); ok {
		// DPrintf("I AM INT")
		return
	}

	op := message.Command.(Op)
	// DPrintf("[RaftApplyCommand]Server %d, Got Command -> Index:%d, ClientId %d, RequestId %d, Command %v, Key: %v, Value: %v", kv.me, message.CommandIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)

	if message.CommandIndex <= kv.lastIncludedIndex {
		return
	}

	shard := key2shard(op.Key)
	// duplicate command will not be executed
	if !kv.checkDuplicateRequest(op.ClientId, op.RequestId, shard) {
		// execute command
		if op.Command == "Put" {
			kv.Put(op)
		} else if op.Command == "Append" {
			kv.Append(op)
		} else if op.Command == "UpdateConfig" {
			kv.updateConfig(op)
		} else if op.Command == "InsertShard" {
			kv.insertShard(op)
		} else if op.Command == "DelteShard" {
			kv.deleteShard(op)
		} else if op.Command == "ShardSent" {
			kv.shardSent(op)
		}
	}
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
	kv.kvDB[shard].data[op.Key] = op.Value
	kv.kvDB[shard].lastRequestId[op.ClientId] = op.RequestId
	// DPrintf("[KVServerExePUT]ClientId :%d ,RequestID :%d ,Key: %v, value: %v", op.ClientId, op.RequestId, op.Key, op.Value)
}

// Append op to kvDB
func (kv *ShardKV) Append(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(op.Key)
	value, exist := kv.kvDB[shard].data[op.Key]
	if exist {
		kv.kvDB[shard].data[op.Key] = value + op.Value
	} else {
		kv.kvDB[shard].data[op.Key] = op.Value
	}
	kv.kvDB[shard].lastRequestId[op.ClientId] = op.RequestId
	// DPrintf("[KVServerExeAPPEND]ClientId :%d ,RequestID:%d ,Key: %v, value: %v", op.ClientId, op.RequestId, op.Key, op.Value)
}


func (kv *ShardKV) fetchConfigsLoop() {
	for !kv.killed() {
		
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			curConfigNum := kv.getConfig(-1).Num
			kv.mu.Unlock()
		
			newConfig := kv.mck.Query(curConfigNum + 1)
			if newConfig.Num == curConfigNum + 1 {
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


func (kv *ShardKV) updateConfig(op Op) {
	newConfig := op.Config
	kv.mu.Lock()
	curConfig := kv.getConfig(-1)
	kv.mu.Unlock()

	if newConfig.Num <= curConfig.Num {
		return 
	}
	
	if newConfig.Num > curConfig.Num + 1 {
		panic("updateconfig get wrong newConfig!")
	}
	gid := kv.gid
	newShardsInclude := make([]int, 10)

	kv.mu.Lock()
	for _, k := range newConfig.Shards {
		if curConfig.Shards[k] != gid{
			// toSend[k] = kv.kvDB[k].Copy()
			kv.kvDB[k].status = Sending
			newShardsInclude[k] = 0
		} else {
			newShardsInclude[k] = 1
		}
	}
	
	for _, k := range curConfig.Shards {
		if newConfig.Shards[k] != gid {
			kv.kvDB[k].status = Sending
		}
	}
	kv.shardsInclude = newShardsInclude

	
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return 
	}

	// for k, v := range toSend {
	// 	newGid := newConfig.Shards[k]
	// }



	kv.mu.Unlock()
}

func (kv *ShardKV) getCurSendingShards() map[int][]Shard {
	res := map[int][]Shard{}
	newConfig := kv.getConfig(-1)
	for i, shard := range kv.kvDB {
		if shard.status == Sending {
			newGid := newConfig.Shards[i]
			if _, ok := res[newGid]; ok {
				res[newGid] = append(res[newGid], shard)
			} else {
				res[newGid] = append([]Shard{}, shard)
			}
		}
	}
	return res
}


func (kv *ShardKV) sendingShardsLoop() {
	for !kv.killed() {
		
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			var waitGroup sync.WaitGroup
			shards := kv.getCurSendingShards()
			if len(shards) > 0 {
				curConfig := kv.getConfig(-1)
				for gid, sendShards := range shards {

					go func(gid int, shards []Shard, servers []string){
						waitGroup.Add(1)
						defer waitGroup.Done()
						for _, shard := range shards {
							for _, server := range servers {
								dst := kv.make_end(server)
								reply := TransferShardsReply{}
								args := TransferShardsArgs{shard}
								if dst.Call("ShardKV.TransferShards", &args, &reply) && reply.Err == OK {
									op := Op{
										Command : "ShardSent",
										ShardId : shard.id,
									}
									kv.rf.Start(op)
								}
							}
						}
					}(gid, sendShards, curConfig.Groups[gid])
				}
			}
			waitGroup.Wait()
			kv.mu.Unlock()
		}
		
		time.Sleep(sendingShardsInterval)
	}
}


func (kv *ShardKV) shardSent(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id := op.ShardId
	kv.kvDB[id].status = toBeDelete
}

func (kv *ShardKV) insertShard(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newShard := op.Shard
	newShard.status = Normal
	id := newShard.id
	kv.kvDB[id] = newShard
}

func (kv *ShardKV) deleteShard(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id := op.ShardId

	kv.kvDB[id].status = Normal
}