package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// import "bytes"
// import "6.824/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry holds information about each log
type LogEntry struct {
	command interface{}
	term    int
}

//enum for different server states
const (
	LEADER     = "Leader"
	FOLLOWER   = "Follower"
	CANDIDATES = "Candidates"
)

//enum timeout
const ()

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//server states
	state           string
	leaderId        int
	lastReceiveTime int64 //record time of the last received rpc

	//Persistent state
	currentTerm int
	votedFor    int //candidateId that received vote in current term(or null if none)
	log         []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means that candidate received vote
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []logEntry
	leaderCommit int
}

type AppendEntryReply struct {
	Term    int  //current term, for leader update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) changeState(state string) {

	if state == FOLLOWER {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.lastReceiveTime = time.Now().Unix()
	} else if state == LEADER {
		rf.state = LEADER
		rf.lastReceiveTime = time.Now().Unix()
	} else if state == CANDIDATES {
		rf.state = CANDIDATES
		rf.votedFor = rf.me
		rf.currentTerm++
		rf.lastReceiveTime = time.Now().Unix()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote
// example RequestVote RPC handler.
// rf is receiver, args is candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//candidate term cannot smaller than receiver's id
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//if candidate terms bigger than receiver,
	//receiver change its terms and becomes follower of this term's leader
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
		rf.leaderId = -1
	}
	//if receiver has not voted yet or votedFor is candidateId
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//candidate log is at lease as up-to-date as receivers log

		//if receiver's latest log term is bigger than candidates term
		//Or if receiver's latest log term is equal to candidates term
		//but its log longer than candidates log, Reject to vote
		lastLogIndexTerm := 0
		if len(rf.log) != 0 {
			lastLogIndexTerm = rf.log[len(rf.log)-1].term
		}
		if lastLogIndexTerm > args.LastLogTerm ||
			(lastLogIndexTerm == args.LastLogTerm && (len(rf.log) > args.LastLogIndex)) {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
		//else granted to vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		rf.lastReceiveTime = time.Now().Unix()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
	timeout := time.Duration(200+rand.Int63n(150)) * time.Millisecond
	startTickTime := time.Now().Unix()

	for rf.killed() == false {
		//sleep timeout period of time
		time.Sleep(timeout)
		//have lock
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			lastStartTickTime := startTickTime
			//update current start tick time to know
			startTickTime = time.Now().Unix()
			endSleepTime := rf.lastReceiveTime + 200 + rand.Int63n(150)
			//if time of recent raft receive RPC smaller than lastStartTickTime, means no new RPC receive
			//EndSleep time has to bigger than startTime
			if rf.lastReceiveTime < lastStartTickTime || endSleepTime <= startTickTime {
				//enter leader selection
				rf.leaderSelection()
			} else {
				//get new RPC without timeout, reset sleep time
				timeout = time.Duration(endSleepTime - startTickTime)
			}

		}()

	}
}

func (rf *Raft) leaderSelection() {

	voteCount := 1 //num of vote this candidate received(1 is his own vote)
	totalVote := 1 //total number of vote
	rf.changeState(CANDIDATES)

	cond := sync.NewCond(&rf.mu)

	//loop through every peer ask for votes
	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId == rf.me {
			continue
		}
		//send request vote
		go func(id int) {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log),
				LastLogTerm:  -1,
			}
			reply := RequestVoteReply{}
			DPrintf("[%d]: term: [%d], send request vote to: [%d]", rf.me, rf.currentTerm, id)
			//send request to peers
			ok := rf.sendRequestVote(id, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Lock()
			totalVote++

			if !ok {
				return
			}
			//vote granted and reply term equal to current term(to avoid old rpc)
			if reply.VoteGranted && rf.currentTerm == reply.Term {
				voteCount++
			} else if reply.Term > rf.currentTerm {
				//if receiver term bigger than candidate term, candidate change to follower
				rf.changeState(FOLLOWER)
				//change term
				rf.currentTerm = reply.Term
			}
			cond.Broadcast() //tell counting request vote goroutine to start counting
		}(peerId)

		//keep waiting for the end of voting, then counting request vote
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != CANDIDATES {
				return
			}
			//keep waiting until every peer sends vote
			for voteCount <= len(rf.peers)/2 && totalVote < len(rf.peers) {
				cond.Wait()
			}
			//if current candidate get more than half votes, change to leader
			if voteCount > len(rf.peers)/2 {
				rf.changeState(LEADER)
			}
		}()

	}
}

func (rf *Raft) sendHeartBeat() {
	for rf.killed() == false {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//if not leader just return
			if rf.state != LEADER {
				return
			}
			//loop through every peer
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
				//args := AppendEntryArgs{
				//	Term:     rf.currentTerm,
				//	LeaderId: rf.me,
				//}
				//go func(id int) {
				//	reply := AppendEntryReply{}
				//	ok := rf.sendAppendEntry
				//}(peerId)

			}

		}()
		//send heartbeat no more than 10 times per second
		//means 100ms send once
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	//initialize current Raft
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastReceiveTime = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//heart beat of leader
	go rf.sendHeartBeat()

	return rf
}
