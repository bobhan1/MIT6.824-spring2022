package kvraft

import (
	"6.824/labrpc"
	mathrand "math/rand"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	requestId int
	leaderId  int
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
	ck.requestId = 0
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.requestId++
	requestId := ck.requestId
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: requestId,
	}
	DPrintf("Client[%d] Get starts, Key = %s ", ck.clientId, key)
	leaderId := ck.getCurLeader()

	for {
		DPrintf("Client: %d Send LEADER ID: %d", ck.clientId, leaderId)
		reply := GetReply{}

		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == ErrNoKey {
				DPrintf("NO KEY FOUND")
				return ""
			} else if reply.Err == OK {
				// request is sent successfully
				DPrintf("GET THE VALUE SUCCEED! value; %v", reply.Value)
				return reply.Value
			} else {
				DPrintf("WRONG LEADER!")
			}
		}
		leaderId = ck.nextLeader()
	}
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//requestId := ck.lastRequestId + 1
	ck.requestId++
	requestId := ck.requestId
	leaderId := ck.getCurLeader()
	for {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientId:  ck.clientId,
			RequestId: requestId,
		}

		DPrintf("Client[%d] PutAppend, Key = %s, Value = %s, leaderId: %d ", ck.clientId, key, value, leaderId)

		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			if op == "append" {
				DPrintf("PUT SUCCEED! with leader: %d", leaderId)
			} else if op == "put" {
				DPrintf("PUT SUCCEED! with leader: %d", leaderId)
			}

			ck.leaderId = leaderId
			break
		}
		//change to next leader keep asking
		leaderId = ck.nextLeader()
	}
}

//get current leader id
func (ck *Clerk) getCurLeader() (leaderId int) {
	leaderId = ck.leaderId
	return leaderId
}

//if not the leader change find next server as leader
func (ck *Clerk) nextLeader() (leaderId int) {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	return ck.leaderId
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "append")
}
