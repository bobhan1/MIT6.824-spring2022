package shardctrler

import (
	"6.824/raft"
)

// ApplyLoop keep fetching command or snapshot from applyCha
func (sc *ShardCtrler) ApplyLoop() {

	for msg := range sc.applyCh {
		//get command from applyCh either command or snapshot
		if msg.CommandValid {
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
		switch op.Command {
		case "Join":

		case "Leave":

		case "Move":
			
		}
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
	if num <= 0 || num > len(sc.configs) {
		return sc.configs[len(sc.configs)-1].copy()
	} else{
		return sc.configs[num].copy()
	}
}