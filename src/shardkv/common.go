package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeout"
	ErrConfigNumOutDated = "ErrConfigNumOutDated"
)

type Err string

type CommonArgsFields struct {
	ClientId  int64
	RequestId int
	ConfigNum int 
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommonArgsFields
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommonArgsFields
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferShardsArgs struct {
	Shard Shard
}

type TransferShardsReply struct {
	Err Err
}

type DeleteShardsArgs struct {
	ShardId int
}

type DeleteShardsReply struct {
	Err Err
}

type ShardsSentArgs struct {
	shard int
}

type ShardsSentReply struct {
	Err Err
}