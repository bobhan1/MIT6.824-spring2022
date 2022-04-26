package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)


const (
	timeoutIntervals = time.Millisecond * 200
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string // "get" "put" "append"
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB          map[string]string // store client key/value
	waitApplyCh   map[int]chan Op   // index(raft) -> chan, waiting to get the applied msg from raft
	lastRequestId map[int64]int     // clientId -> requestID,//make sure operation only executed once

	lastIncludedIndex int
}

// Get RPC Handler for request from clerk
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[GET Request]From Client %d (RequestId %d) To Server %d", args.ClientId, args.RequestId, kv.me)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		// DPrintf("[GET SendToWrongLeader]From Client %d, Request %d To Server %d", args.ClientId, args.RequestId, kv.me)
		return
	}
	op := Op{
		Command:   "get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// DPrintf("[GET SendToWrongLeader]From Client %d, Request %d To Server %d", args.ClientId, args.RequestId, kv.me)
		return
	}
	DPrintf("GET From Client %d (Request %d) To Server %d, key %v, raftIndex %d",
		args.ClientId, args.RequestId, kv.me, op.Key, index)

	//create waitForCh if not find for raft index, then create new one
	kv.mu.Lock()
	waitCh, exist := kv.waitApplyCh[index]
	if !exist {
		kv.waitApplyCh[index] = make(chan Op, 1)
		waitCh = kv.waitApplyCh[index]
	}
	kv.mu.Unlock()

	// timeout
	select {
	case <-time.After(timeoutIntervals):
		// DPrintf("GET timeout From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, index)
		reply.Err = ErrTimeOut

	case raftCommitOp := <-waitCh:
		DPrintf("waitChannel Server %d, Index:%d , ClientId %d, RequestId %d, Command %v, Key :%v, Value :%v",
			kv.me, index, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)

		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {

			value, exist := kv.ExecuteGet(op)

			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, index)
	kv.mu.Unlock()
	return

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[PUTAPPEND Request]From Client %d (Request %d) To Server %d", args.ClientId, args.RequestId, kv.me)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Command:   args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	raftIndex, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[PUTAPPEND SendToWrongLeader]From Client %d (Request %d) To Server %d", args.ClientId, args.RequestId, kv.me)
		return
	}
	DPrintf("PutAppend From Client:%d, Request %d, To Server %d, key %v, raft"+
		"Index %d",
		args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

	// create waitForCh
	kv.mu.Lock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		ch = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(timeoutIntervals):
		reply.Err = ErrTimeOut

	case raftCommitOp := <-ch:
		DPrintf("WaitCha Server %d,Index:%d, ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",
			kv.me, raftIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.//t.lenovo.cn/3EzqQn

//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	DPrintf("[InitKVServer---]Server %d", me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 5)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()

	if len(snapshot) > 0 {
		kv.DecodeSnapshot(snapshot)
	}

	go kv.ApplyLoop()
	return kv

}
