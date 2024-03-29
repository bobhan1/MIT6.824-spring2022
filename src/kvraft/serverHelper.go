package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

// ApplyLoop keep fetching command or snapshot from applyCha
func (kv *KVServer) ApplyLoop() {

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
func (kv *KVServer) ApplySnapShot(msg raft.ApplyMsg) {
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
func (kv *KVServer) DecodeSnapshot(snapshot []byte) {
	DPrintf("KVserver %d, Reading Snapshot", kv.me)
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistKVDB map[string]string
	var persistLastRequestId map[int64]int

	if d.Decode(&persistKVDB) != nil || d.Decode(&persistLastRequestId) != nil {
		DPrintf("kv server %d cannot decode", kv.me)
	} else {
		kv.kvDB = persistKVDB
		kv.lastRequestId = persistLastRequestId
		DPrintf("KVserver: %d, KVDB: %v, lastRequestId: %d", kv.me, persistKVDB, persistLastRequestId)
	}
}

// EncodeSnapshot according to KvDB
// SnapShot include KvDB, lastRequestId map
func (kv *KVServer) EncodeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequestId)
	data := w.Bytes()
	return data
}

// ExecuteGet fetch the value from kvDB if exists
func (kv *KVServer) ExecuteGet(op Op) (string, bool) {
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	if exist {
		DPrintf("GET ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId, op.RequestId, op.Key, value)
	} else {
		DPrintf("GET ClientId :%d ,RequestID :%d ,Key : %v, key not found", op.ClientId, op.RequestId, op.Key)
	}
	return value, exist
}

//check if request is duplicated, if yes return true
func (kv *KVServer) checkDuplicateRequest(newClientId int64, newRequestId int) bool {
	kv.mu.Lock()
	// return true if message is duplicate
	lastRequestId, ifClientInRecord := kv.lastRequestId[newClientId]
	kv.mu.Unlock()
	if !ifClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}

// ApplyCommand execute putAppend, and take snapshot if needed
func (kv *KVServer) ApplyCommand(message raft.ApplyMsg) {

	//ignore dummy command
	if _, ok := message.Command.(int); ok {
		DPrintf("I AM INT")
		return
	}

	op := message.Command.(Op)
	DPrintf("[RaftApplyCommand]Server %d, Got Command -> Index:%d, ClientId %d, RequestId %d, Command %v, Key: %v, Value: %v",
		kv.me, message.CommandIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)

	if message.CommandIndex <= kv.lastIncludedIndex {
		return
	}

	// duplicate command will not be executed
	if !kv.checkDuplicateRequest(op.ClientId, op.RequestId) {
		// execute command
		if op.Command == "put" {
			kv.Put(op)
		}
		if op.Command == "append" {
			kv.Append(op)
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
func (kv *KVServer) SendMsgToWaitChan(op Op, raftIndex int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		DPrintf("[RaftApplyMsgSendToWaitChan]Server %d, Send CommandIndex:%d, ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",
			kv.me, raftIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)
		ch <- op
	}
	return exist
}

// Put value means replace the value according to key
func (kv *KVServer) Put(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kvDB[op.Key] = op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
	//kv.mu.Unlock()
	DPrintf("[KVServerExePUT]ClientId :%d ,RequestID :%d ,Key: %v, value: %v", op.ClientId, op.RequestId, op.Key, op.Value)
}

// Append op to kvDB
func (kv *KVServer) Append(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exist := kv.kvDB[op.Key]
	if exist {
		kv.kvDB[op.Key] = value + op.Value
	} else {
		kv.kvDB[op.Key] = op.Value
	}
	kv.lastRequestId[op.ClientId] = op.RequestId
	//kv.mu.Unlock()

	DPrintf("[KVServerExeAPPEND]ClientId :%d ,RequestID:%d ,Key: %v, value: %v", op.ClientId, op.RequestId, op.Key, op.Value)
}
