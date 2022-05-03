package shardctrler

import (
	"6.824/raft"
	"sort"
)

// ApplyLoop keep fetching command or snapshot from applyCha
func (sc *ShardCtrler) ApplyLoop() {

	for msg := range sc.applyCh {
		//get command from applyCh either command or snapshot
		if msg.CommandValid {
			// DPrintf("apply a msg.command")
			sc.ApplyCommand(msg)
		}
	}
}




// check if request is duplicated, if yes return true
// return true if message is duplicate
func (sc *ShardCtrler) checkDuplicateRequest(newClientId int64, newRequestId int) bool {
	sc.mu.Lock()
	lastRequestId, ifClientInRecord := sc.lastRequestId[newClientId]
	sc.mu.Unlock()
	
	if !ifClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}

// ApplyCommand execute putAppend, and take snapshot if needed
func (sc *ShardCtrler) ApplyCommand(message raft.ApplyMsg) {

	//ignore dummy command
	if _, ok := message.Command.(int); ok {
		DPrintf("I AM INT")
		return
	}

	op := message.Command.(Op)
	// duplicate command will not be executed
	if !sc.checkDuplicateRequest(op.ClientId, op.RequestId) {
		// execute command
		sc.mu.Lock()
		switch op.Command {
		case "Join":
			DPrintf("handle a join command")
			
			sc.join(op.Servers)
			sc.lastRequestId[op.ClientId] = op.RequestId
			//sc.mu.Unlock()
			DPrintf("join command handled finished")
		case "Leave":
			//sc.mu.Lock()
			sc.leave(op.GIDs)
			sc.lastRequestId[op.ClientId] = op.RequestId
			//sc.mu.Unlock()
		case "Move":
			//sc.mu.Lock()
			sc.move(op.Shard, op.GID)
			sc.lastRequestId[op.ClientId] = op.RequestId
			//sc.mu.Unlock()
		case "Query":
			// sc.lastRequestId[op.ClientId] = op.RequestId
		}
		sc.mu.Unlock()
	}
	// Send message to the chan of op.ClientId
	sc.SendMsgToWaitChan(op, message.CommandIndex)
}

// SendMsgToWaitChan send each op to waitChan with raftIndex waiting to be fetched
func (sc *ShardCtrler) SendMsgToWaitChan(op Op, raftIndex int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitApplyCh[raftIndex]
	if exist {
		// DPrintf("[RaftApplyMsgSendToWaitChan]Server %d, Send CommandIndex:%d, ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", sc.me, raftIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)
		ch <- op
	}
	return exist
}

func (sc *ShardCtrler) getConfig(num int) Config {
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	} else{
		return sc.configs[num].Copy()
	}
}

func (config *Config) getGIDCountMap() ([]int, map[int]int){
	count := map[int]int{}
	keys := []int{}
	for gid, _ := range config.Groups {
		keys = append(keys, gid)
		count[gid] = 0
	}
	for _, gid := range config.Shards{
		if gid == 0 {
			continue
		}
		count[gid] = count[gid] + 1
	}
	sort.Ints(keys)
	// DPrintf("len(keys):[%d]", len(keys))
	// ensure that the operations executed the same results on every raft node
	return keys, count
}


// get the largest group currently 
func (config *Config) getMaxGroup() (int, int){
	maxGid, maxSize := -1, 0
	keys, count := config.getGIDCountMap()
	for _, gid := range keys{
		val := count[gid]
		if val > maxSize {
			maxSize = val
			maxGid = gid
		}
	}
	return maxGid, maxSize
}

func (config *Config) getMinGroup() (int, int){
	minGid, minSize := -1, 20
	keys, count := config.getGIDCountMap()
	for _, gid := range keys{
		val := count[gid]
		if val < minSize {
			minSize = val
			minGid = gid
		}
	}
	return minGid, minSize
}


// migrate a shard from srcGroup to dstGroup
func (config *Config) shardMigration(srcGroup int, dstGroup int){
	for shard, gid := range config.Shards{
		if gid == srcGroup {
			config.Shards[shard] = dstGroup
			return 
		}
	}	
}

// balance the shards between groups until maxSize <= minSize + 1
func (config *Config) shardRebalance(){
	if len(config.Groups) == 0 {
		return 
	}
	for {
		// migrate a shard from a largest group to a smallest group
		maxGid, maxSize := config.getMaxGroup()
		minGid, minSize := config.getMinGroup()
		// DPrintf("min:[%d], max[%d]", minSize, maxSize)
		if maxSize <= minSize + 1 {
			return 
		} 
		config.shardMigration(maxGid, minGid)
	}
}


func (sc *ShardCtrler) join(servers map[int][]string) {
	newConfig := sc.getConfig(-1)
	newConfig.Num++
	for k, v := range servers {
		// DPrintf("append for [%d]",k)
		newConfig.Groups[k] = append([]string{}, v...)
	}
	newShards := make([]int, 10)
	for shard, gid := range newConfig.Shards {
		// DPrintf("[%d]:[%d]",shard, gid)
		if gid == 0{
			newShards = append(newShards, shard)
		}
	}
	for _, shard := range newShards {
		minGid, _ := newConfig.getMinGroup()
		// DPrintf("minGid[%d]", minGid)
		newConfig.Shards[shard] = minGid
	}
	// DPrintf("before shardRebalance");
	newConfig.shardRebalance()
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) leave(GIDs []int) {
	newConfig := sc.getConfig(-1)
	newConfig.Num++
	newShards := make([]int, len(GIDs))
	for _, gid := range GIDs {
		delete(newConfig.Groups, gid)
		for shard, gid_ := range newConfig.Shards {
			if gid_ == gid {
				newConfig.Shards[shard] = 0
				newShards = append(newShards, shard)
			}
		}
	}
	if len(newConfig.Groups) > 0{
		for _, shard := range newShards {
			minGid, _ := newConfig.getMinGroup()
			newConfig.Shards[shard] = minGid
		} 
	}
	newConfig.shardRebalance()
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) move(shard int, GID int) {
	newConfig := sc.getConfig(-1)
	newConfig.Num++
	newConfig.Shards[shard] = GID
	newConfig.shardRebalance()
	sc.configs = append(sc.configs, newConfig)
}
