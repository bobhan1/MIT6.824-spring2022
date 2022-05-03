package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "time"
import "sync/atomic"
import "6.824/shardctrler"

const (
	raftTimeOutInterval = time.Millisecond * 200
	fetchConfigsInterval = time.Millisecond * 80
	sendingShardsInterval = time.Millisecond * 40
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string // "Get"/"Put"/"Append"/"UpdateConfig"/"TransferShrads"/"DeleteShards"

	Key       string
	Value     string
	Shard     Shard
	ShardId   int  
	Config    shardctrler.Config 

	ClientId  int64
	RequestId int
}

const (
	Normal int = iota
	Sending
	toBeDelete
	deleting
) 

type Shard struct {
	Id int
	Data map[string]string
	ConfigNum int
	LastRequestId map[int64]int     // clientId -> requestID
	Status int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead    	 int32

	updating int32 // if the kv server is updating config 

	// Your definitions here.

	kvDB          [shardctrler.NShards] Shard // store client key/value
	waitApplyCh   map[int]chan Op   // index(raft) -> chan, waiting to get the applied msg from raft
	mck           *shardctrler.Clerk
	lastIncludedIndex int
	configs []shardctrler.Config

	shardsInclude []int  // which shard I'm resiponsible for 

}

func (shard *Shard) Copy() Shard{
	newData := map[string]string{}
	newLastRequestId := map[int64]int{}
	for k, v := range shard.Data {
		newData[k] = v
	} 
	for k, v := range shard.LastRequestId {
		newLastRequestId[k] = v
	}
	newShard := Shard{
		Id : shard.Id,
		Data: newData,
		ConfigNum : shard.ConfigNum,
		LastRequestId : newLastRequestId,    
		Status : Normal,
	}
	return newShard
}

func (kv *ShardKV) getConfig(num int) shardctrler.Config {
	if num < 0 || num >= len(kv.configs) {
		return kv.configs[len(kv.configs)-1].Copy()
	} else{
		return kv.configs[num].Copy()
	}
}


func (kv *ShardKV) containsShard(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.shardsInclude[shard] == 1
}

func (kv *ShardKV) shardIsNormal(shard int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.kvDB[shard].Status == Normal
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	curConfigNum := kv.getConfig(-1).Num
	kv.mu.Unlock() 
	if args.ConfigNum < curConfigNum {
		reply.Err = ErrConfigNumOutDated
		return 
	}
	if args.ConfigNum > curConfigNum {
		reply.Err = ErrTimeOut
		return 
	}
	shard := key2shard(args.Key)
	if !kv.containsShard(shard) {
		reply.Err = ErrWrongGroup
		return 
	}

	if !kv.shardIsNormal(shard) {
		reply.Err = ErrTimeOut
		return 
	}

	DPrintf("[=]recieved Get ")
	// DPrintf("[GET Request]From Client %d (RequestId %d) To Server %d", args.ClientId, args.RequestId, kv.me)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		// DPrintf("[GET SendToWrongLeader]From Client %d, Request %d To Server %d", args.ClientId, args.RequestId, kv.me)
		return
	}
	op := Op{
		Command:   "Get",
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
	// DPrintf("GET From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, index)

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
	case <-time.After(raftTimeOutInterval):
		// DPrintf("GET timeout From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, index)
		reply.Err = ErrTimeOut

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
	kv.mu.Lock()
	delete(kv.waitApplyCh, index)
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[PUTAPPEND Request]From Client %d (Request %d) To Server %d", args.ClientId, args.RequestId, kv.me)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	curConfigNum := kv.getConfig(-1).Num
	kv.mu.Unlock() 
	if args.ConfigNum < curConfigNum {
		reply.Err = ErrConfigNumOutDated
		return 
	}
	if args.ConfigNum > curConfigNum {
		reply.Err = ErrTimeOut
		return 
	}
	shard := key2shard(args.Key)
	if !kv.containsShard(shard) {
		reply.Err = ErrWrongGroup
		return 
	}

	if !kv.shardIsNormal(shard) {
		reply.Err = ErrTimeOut
		return 
	}

	
	if _, isLeader := kv.rf.GetState(); !isLeader {
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
		// DPrintf("[PUTAPPEND SendToWrongLeader]From Client %d (Request %d) To Server %d", args.ClientId, args.RequestId, kv.me)
		return
	}
	// DPrintf("PutAppend From Client:%d, Request %d, To Server %d, key %v, raft"+ "Index %d",args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

	// create waitForCh
	kv.mu.Lock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		ch = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(raftTimeOutInterval):
		reply.Err = ErrTimeOut

	case raftCommitOp := <-ch:
		// DPrintf("WaitCha Server %d,Index:%d, ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)
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


// the sender call this rpc
func (kv *ShardKV) TransferShards(args *TransferShardsArgs, reply *TransferShardsReply) {

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {	
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	curConfigNum := kv.getConfig(-1).Num
	kv.mu.Unlock() 
	newShard := args.Shard
	if newShard.ConfigNum != curConfigNum{
		reply.Err = ErrTimeOut
		return 
	}
	kv.mu.Lock()
	if kv.kvDB[newShard.Id].Status != Sending {
		reply.Err = ErrTimeOut
		kv.mu.Unlock() 
		return 
	}
	kv.mu.Unlock() 

	op := Op{
		Command : "InsertShard",
		Shard : newShard,
	}
	raftIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// DPrintf("[PUTAPPEND SendToWrongLeader]From Client %d (Request %d) To Server %d", args.ClientId, args.RequestId, kv.me)
		return
	}

	kv.mu.Lock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		ch = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(raftTimeOutInterval):
		reply.Err = ErrTimeOut

	case <-ch:
		// DPrintf("WaitCha Server %d,Index:%d, ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)
		reply.Err = OK

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) DeleteShards(args *TransferShardsArgs, reply *TransferShardsReply) {

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {	
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	curConfigNum := kv.getConfig(-1).Num
	kv.mu.Unlock() 
	newShard := args.Shard
	if newShard.ConfigNum != curConfigNum{
		reply.Err = ErrTimeOut
		return 
	}
	kv.mu.Lock()
	if kv.kvDB[newShard.Id].Status != Sending {
		reply.Err = ErrTimeOut
		kv.mu.Unlock() 
		return 
	}
	kv.mu.Unlock() 

	op := Op{
		Command : "DeleteShard",
		ShardId : newShard.Id,
	}
	raftIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// DPrintf("[PUTAPPEND SendToWrongLeader]From Client %d (Request %d) To Server %d", args.ClientId, args.RequestId, kv.me)
		return
	}

	kv.mu.Lock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		ch = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(raftTimeOutInterval):
		reply.Err = ErrTimeOut

	case <-ch:
		// DPrintf("WaitCha Server %d,Index:%d, ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Command, op.Key, op.Value)
		reply.Err = OK

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) isUpdateConfig() bool {
	z := atomic.LoadInt32(&kv.updating)
	return z == 1
}

func (kv *ShardKV)setUpdateConcig() {
	atomic.StoreInt32(&kv.updating, 1)
}

func (kv *ShardKV)unsetUpdateConcig() {
	atomic.StoreInt32(&kv.updating, 0)
}


func init(){
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// DPrintf("==========")

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg, 10)
	kv.configs = make([]shardctrler.Config, 1)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for index, _ := range kv.kvDB{
		kv.kvDB[index] = Shard{
			Id: index,
			Data: map[string]string{},
			ConfigNum: 0,
			LastRequestId: map[int64]int{},
		}
	}
	kv.waitApplyCh = make(map[int]chan Op)

	snapshot := persister.ReadSnapshot()

	if len(snapshot) > 0 {
		kv.DecodeSnapshot(snapshot)
	}

	go kv.ApplyLoop()
	go kv.fetchConfigsLoop()
	go kv.sendingShardsLoop()
	
	return kv
}
