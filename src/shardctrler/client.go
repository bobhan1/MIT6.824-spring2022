package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64
	requestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.requestId = 0
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.requestId++

	args.Num = num
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok {
				if reply.Err == OK{
					return reply.Config
				} else if reply.Err == ErrWrongLeader{
					
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.requestId++

	args.Servers = servers
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok {
				if reply.Err == OK{
					return 
				} else if reply.Err == ErrWrongLeader{
					
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.requestId++

	args.GIDs = gids
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok {
				if reply.Err == OK{
					return 
				} else if reply.Err == ErrWrongLeader{
					
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.requestId++

	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok {
				if reply.Err == OK{
					return 
				} else if reply.Err == ErrWrongLeader{
					
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
