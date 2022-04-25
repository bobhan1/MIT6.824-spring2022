package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "sync/atomic"
import "time"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead    int32 // set by Kill()

	waitApplyCh   map[int]chan Op   // index(raft) -> chan, waiting to get the applied msg from raft
	lastRequestId map[int64]int     // clientId -> requestID,//make sure operation only executed once

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	Command   string // "Join" "Leave" "Move" "Query"
	
	Servers map[int][]string // for "Join", new GID -> servers mappings
	GIDs []int // for "Leave"
	Shard int // for "Move"
	GID   int // for "Move"
	Num int // for "Query", desired config number

	ClientId  int64
	RequestId int
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	// DPrintf("[GET Request]From Client %d (RequestId %d) To Server %d", args.ClientId, args.RequestId, kv.me)
	if sc.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Command:   "Join",
		Servers: args.Servers,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// DPrintf("[GET SendToWrongLeader]From Client %d, Request %d To Server %d", args.ClientId, args.RequestId, kv.me)
		return
	}
	// DPrintf("GET From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, index)

	//create waitForCh if not find for raft index, then create new one
	sc.mu.Lock()
	waitCh, exist := sc.waitApplyCh[index]
	if !exist {
		sc.waitApplyCh[index] = make(chan Op, 1)
		waitCh = sc.waitApplyCh[index]
	}
	sc.mu.Unlock()

	// timeout
	select {
	case <-time.After(time.Millisecond * 200):
		// DPrintf("GET timeout From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, index)

		_, isLeader := sc.rf.GetState()

		//find duplicate
		if sc.checkDuplicateRequest(op.ClientId, op.RequestId) && isLeader {
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
	case raftCommitOp := <-waitCh:
		// DPrintf("waitChannel Server %d, Index:%d , ClientId %d, RequestId %d, Command %v, Key :%v, Value :%v", kv.me, index, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)

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

	sc.mu.Lock()
	delete(sc.waitApplyCh, index)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}


// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
