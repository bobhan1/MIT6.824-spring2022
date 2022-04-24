package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

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
	Command interface{}
	Term    int //ensure consistency if leader crash
}

//enum for different server states
const (
	LEADER     = "Leader"
	FOLLOWER   = "Follower"
	CANDIDATES = "Candidates"
)

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

	//if leader crash, new leader needs to have entire committed log
	//that is if one log entry has been committed and this log entry must exist in higher term leader's log
	commitIndex int // the highest log entry that has been committed
	lastApplied int

	//volatile state on leaders
	//help leader maintain other server's log
	nextIndex  []int //index of next log entry to send to that server
	matchIndex []int //index of the highest log entry known to be replicated on server

	applyChan    chan ApplyMsg //once logEntry is committed, it sends to applyChan right away
	applyChecker bool          // set to true when server needs to apply log to state machine
	applyCond    *sync.Cond    //control apply go routine

	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int

	//check if server apply snapshot first after crash
	//applyChecker bool

	restart bool
}

//get the size of log after snapshot (lastIncludeIndex + length of log)
func (rf *Raft) getSize() int {
	return rf.lastIncludedIndex + len(rf.log)
}

//get log Entry (after snapshot) according to origin log index
func (rf *Raft) getEntry(index int) LogEntry {
	log.Printf("Raft %d, index: %d, lastIncludedIndex: %d,  Inside getEntry, len(log) : %d ", rf.me, index, rf.lastIncludedIndex, len(rf.log))
	if index < rf.lastIncludedIndex {
		panic("Index  <  lastIncludeIndex")
	}

	return rf.log[index-rf.lastIncludedIndex]
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
	Term     int
	LeaderId int
	//used to let follower check if its log same with leader
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int  //current term, for leader update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

	//Fast back up mechanism, if follower missed long log, won't need to back up step by step
	//leader the can jump back entire log that needs to delete
	XTerm  int // the term of conflicting entry
	XIndex int //index of the first entry with XTerm
	XLen   int // length of log

	LastApplied       int
	LastIncludedIndex int
}

func (rf *Raft) changeState(state string) {

	if state == FOLLOWER {
		log.Printf("Raft ID: %v change state from %s to Follower", rf.me, rf.state)
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.lastReceiveTime = time.Now().Unix()
	} else if state == LEADER {
		log.Printf("Raft ID: %v change state from %s to Leader", rf.me, rf.state)
		rf.state = LEADER
		rf.leaderId = rf.me
		//initialize nextIndex and matchIndex
		rf.lastReceiveTime = time.Now().Unix()
		//for i := 0; i < len(rf.peers); i++ {
		//	rf.nextIndex[i] = rf.getSize()
		//	rf.matchIndex[i] = 0
		//}

	} else if state == CANDIDATES {
		log.Printf("Raft ID: %v change state from %s to Candidate", rf.me, rf.state)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Getting stage, Raft ID %d, stage: %s ", rf.me, rf.state)
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistData() []byte {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) GetRaftSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {

		log.Fatal("Decode Failed!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.mu.Unlock()
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	if rf == nil {
		log.Printf("CondInstall rf is null!")
		return true
	}

	log.Printf("rf %d, CondInstall after null", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		log.Printf("KILLED COND")
		return false
	}

	log.Printf("CONDInstallSnapshot: raft %d, lastIncludedIndex: %d, commitIndex: %d, getSize: %d", rf.me, lastIncludedIndex, rf.commitIndex, rf.getSize())

	//if lastIncludedIndex smaller than actual size, means just alive from crash,
	//log still has entry, cannot set it to null
	//log.Printf("rf:%d, term: %d, lastInTerm: %d", rf.me, rf.getEntry(lastIncludedIndex).Term, lastIncludedTerm)

	//if lastIncludedIndex < rf.getSize() {
	//	//log.Printf("rf:%d, term: %d, lastInTerm: %d", rf.me, rf.getEntry(lastIncludedIndex).Term, lastIncludedTerm)
	//} else {
	//	rf.log = nil
	//}

	if rf.restart {
		rf.restart = false
		return true
	}

	//rf: follower
	if lastIncludedIndex <= rf.commitIndex {
		log.Printf("raft : %d,  lastIncludeIndex %d, commitIndex %d, getSize %d", rf.me, lastIncludedIndex, rf.commitIndex, rf.getSize())
		return false
	}

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	//rf.applyChecker = true
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
	log.Printf("CondInstallSnapshot rf.log : %v", rf.log)
	log.Printf("lastInclude Index: %d, commitIndex: %d, lastApplied: %d", lastIncludedIndex, rf.commitIndex, rf.lastApplied)
	rf.log = nil

	log.Printf("CURRENT LOG!!!!! %v", rf.log)

	//rf.applyCond.Broadcast()
	rf.updateCommitIndex()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf == nil {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	//rf: follower, index: lastIncludedIndex
	log.Printf("Sever %d creates snapshot", rf.me)
	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		log.Printf("Snapshot already there")
		return
	}
	//trim log(after trim the log, the 0 index stores lastIncludeIndex LogEntry )
	log.Printf("raft %d: Log before snapshot %v", rf.me, rf.log)

	//initialize lastIncludedTerm
	rf.lastIncludedTerm = rf.getEntry(index).Term
	//rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.log = append([]LogEntry(nil), rf.log[index-rf.lastIncludedIndex:]...)

	//set dummy node
	rf.log[0] = LogEntry{Command: 0, Term: 0}
	log.Printf("rf.lastIncludeIndex before snapshot %d", rf.lastIncludedIndex)
	log.Printf("Raft %d: log after snapshot: %v", rf.me, rf.log)
	rf.lastIncludedIndex = index

	log.Printf("rf.lastIncludeIndex after snapshot %d", rf.lastIncludedIndex)
	rf.snapshot = snapshot
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
	rf.persist()

}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("server %d receive InstallSnapshot from %d. ", rf.me, args.LeaderId)

	if rf.killed() {
		return
	}
	//after return
	defer func() { reply.Term = rf.currentTerm }()

	//args: leader
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.lastIncludedIndex {
		log.Printf("server %d: InstallSanpshot, Leader's term %d < folower term %d", rf.me, args.Term, rf.currentTerm)
		log.Printf("or, leader's LastincludedIndex %d <= follower lastIndex %d,",
			args.LastIncludedIndex, rf.lastIncludedIndex)
		//rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		//rf.persist()
	}

	rf.lastReceiveTime = time.Now().Unix()
	rf.leaderId = args.LeaderId
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	//rf.persist()

	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	go func() { rf.applyChan <- msg }()
}

func (rf *Raft) sendSnapshot(server int) {
	//current raft: leader
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		return
	}
	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != LEADER || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			log.Printf("raft %d: snapshot receiver has bigger Term than sender!", rf.me)
			rf.changeState(FOLLOWER)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.leaderId = -1
			rf.persist()
			return
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
		log.Printf("Raft: %d, Sending Snapshot: matchIndex: %v, nextIndex: %v, log: %v", rf.me, rf.matchIndex, rf.nextIndex, rf.log)
		rf.updateCommitIndex()
	}
}

// RequestVote
// example RequestVote RPC handler.
// rf is receiver, args is candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Raft %d: received vote request from candidate %d", rf.me, args.CandidateId)
	//candidate term cannot smaller than receiver's id
	if args.Term < rf.currentTerm {
		log.Printf("candidate Term %d has smaller term than receiver term %d denied vote", rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//if candidate terms bigger than receiver,
	//receiver change its terms and becomes follower of this term's leader
	if args.Term > rf.currentTerm {
		log.Printf("candidate Term %d has bigger term than receiver term %d", args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
		rf.votedFor = -1
		rf.leaderId = -1
	}
	//if receiver has not voted yet or votedFor is candidateId
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//candidate log is at lease as up-to-date as receivers log
		//
		//if receiver's latest log term is bigger than candidates term
		//Or if receiver's latest log term is equal to candidates term
		//but its log longer than candidates log, Reject to vote
		lastLogIndexTerm := -1
		lastLogIndex := -1
		if rf.getSize() != 0 && len(rf.log) != 0 {
			lastLogIndex = rf.getSize() - 1
			log.Printf("raft %d, lastLogIndex: %d, lastIncludedIndex %d", rf.me, lastLogIndex, rf.lastIncludedIndex)
			lastLogIndexTerm = rf.getEntry(lastLogIndex).Term
			log.Printf("last lastLogIndexTerm: %d", lastLogIndexTerm)
		}
		if lastLogIndexTerm > args.LastLogTerm ||
			(lastLogIndexTerm == args.LastLogTerm && (lastLogIndex > args.LastLogIndex)) {

			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			log.Printf("candidate args.LastLogIndex: %d, follower lastLogIndex %d", args.LastLogIndex, lastLogIndex)

			log.Printf("Voting failed!")
			return
		}
		//else granted to vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		rf.persist()
		rf.lastReceiveTime = time.Now().Unix()
		log.Printf("Raft ID %d: voted to ID %d", rf.me, args.CandidateId)
	}
}
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	//XTerm: the term of conflicting entry
	//XIndex: index of the first entry with XTerm
	//XLen: length of follower log
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.LastApplied = rf.lastApplied
	reply.LastIncludedIndex = rf.lastIncludedIndex

	//after crash until install snapshot alwasy return false
	if rf == nil || rf.lastApplied+1 <= rf.lastIncludedIndex {
		rf.persist()
		rf.leaderId = args.LeaderId
		rf.lastReceiveTime = time.Now().Unix()
		return
	}

	log.Printf("RaftID: [%d]: received appendEntry from %d, argsTerm: %d,"+
		" LeaderCommit: %d, prevLogIndex: %d, prevLogTerm: %d, len(Entry): %d",
		rf.me, args.LeaderId, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))

	log.Printf("AppendEntry: Leader Log Entry %v, Follower Log Entry %v", args.Entries, rf.log)

	//log.Printf("Follower Term: %d, PreLogIndex")

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm || rf.state != FOLLOWER {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
	}

	//check if preLogIndex and prevLogTerm equal to leader
	if len(rf.log) == 0 {
		//rf.log = make([]LogEntry, 0)
		rf.log = append(rf.log, LogEntry{Command: 0, Term: 0})
	}

	// 1. no entry with prevLogIndex or term conflicting
	if args.PrevLogIndex >= rf.getSize() ||
		(args.PrevLogIndex > 0 && args.PrevLogIndex > rf.lastIncludedIndex && rf.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm) {
		log.Printf("raft %d, cannot pair, return, rf.getSize = %d, lastIncudIndex: %d", rf.me, rf.getSize(), rf.lastIncludedIndex)
		//no entry case: s1: 4
		//       (leader)s2: 4 6 6 6
		reply.XLen = rf.getSize()
		//if it's Term conflicting
		if args.PrevLogIndex >= 0 && args.PrevLogIndex < rf.getSize() {
			//case: s1 : 4 4 4
			//		s2 : 4 6 6 6
			reply.XTerm = rf.getEntry(args.PrevLogIndex).Term // XTerm = 4 here
			// loop back followers' log to find XIndex starts
			for i := args.PrevLogIndex; i >= 0; i-- {
				if i >= rf.lastIncludedIndex && rf.getEntry(i).Term == reply.XTerm {
					reply.XIndex = i
				} else {
					break
				}
			}
		}
		return
	}

	//if log matches prevLogIndex and preLogTerm
	//        s1: 4 6 6 6 6 6
	//(leader)s2: 4 6 6 7 new entry is 7, conflict with s1 6
	// needs to delete s1 6 6 6 three entry and append 7 to the nextIndex
	for i := 0; i < len(args.Entries); i++ {
		//log.Printf("Log Match!")
		index := args.PrevLogIndex + 1 + i
		//if no conflict
		// s1: 4 6 6
		// s2: 4 6 6 7 8
		//here args.Entries only contain 7 8
		if index >= rf.getSize() {
			rf.log = append(rf.log, args.Entries[i:]...)
			//should persist it for lab2c
			rf.persist()
			break
		}
		//        s1: 4 6 6 6 6 6
		//(leader)s2: 4 6 6 7 new entry is 7, conflict with s1 6
		log.Printf("APPEND ENTRY INDEX: %v", index)
		if index >= rf.lastIncludedIndex && rf.getEntry(index).Term != args.Entries[i].Term {
			//delete current conflict index entry and all that follow it
			rf.log = rf.log[:(index - rf.lastIncludedIndex)]
			rf.log = append(rf.log, args.Entries[i:]...)

			//原本没有
			rf.persist()
			break
		}
	}
	log.Printf("Raft: %d, New Follower Log: %v", rf.me, rf.log)

	//update follower's commitIndex
	//make sure lastApplied + 1 > rf.lastIncludedIndex make sure it succeeded to applied snapshot
	//after crash
	if args.LeaderCommit > rf.commitIndex && rf.lastApplied+1 > rf.lastIncludedIndex {
		log.Printf("follower commitIndex %d", rf.commitIndex)
		rf.commitIndex = args.LeaderCommit
		if rf.getSize()-1 < rf.commitIndex {
			rf.commitIndex = rf.getSize() - 1
		}
		log.Printf("Raft %d follower commitIndex Update to %d", rf.me, rf.commitIndex)

		rf.applyChecker = true
		rf.applyCond.Broadcast()

	}

	rf.persist()
	rf.leaderId = args.LeaderId
	rf.lastReceiveTime = time.Now().Unix()
	reply.Success = true
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

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
	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state == LEADER
	log.Printf("Start func: Current raft %d state %s", rf.me, rf.state)
	if isLeader {
		//append command to leader's log
		term = rf.currentTerm

		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		//rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		index = rf.getSize() - 1
		//update leader's nextIndex and matchIndex
		rf.nextIndex[rf.me] = rf.getSize()
		rf.matchIndex[rf.me] = rf.getSize() - 1
		log.Printf("Leader ID %d received command at index %d, currentTerm %d !!!!!!!!!!!!!!!!!!!", rf.me, index, term)
		//log.Printf("Leader Log: %v", rf.log)
		log.Printf("Leader matchIndex: %v, nextIndex: %v", rf.matchIndex, rf.nextIndex)

		log.Printf("LEADER LOG %v", rf.log)
	}
	rf.persist()
	//log.Printf("CURRENT TIME IN START: %v", time.Now().UnixMilli())
	//rf.mu.Unlock()

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
// heartbeats recently.
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
			//EndSleep time has to bigger than startTime. Enter leader election
			if rf.lastReceiveTime < lastStartTickTime || endSleepTime <= startTickTime {
				//enter leader selection
				if rf.state != LEADER {
					rf.leaderSelection()
				}
				//update sleep time, candidate waiting time
				timeout = time.Duration(200+rand.Int63n(150)) * time.Millisecond
			} else {
				//get new RPC without timeout, reset sleep time
				if rf.state == FOLLOWER {
					log.Printf("Raft ID: %d get Rpc from raft Leader ID %d, keep sleep. ", rf.me, rf.leaderId)
				} else if rf.state == CANDIDATES {
					log.Printf("Raft candidate ID: %d send vote request Rpc, Waiting response. Leader Id: %d",
						rf.me, rf.leaderId)
				}
				log.Printf("Last Receive time %v", rf.lastReceiveTime)
				timeout = time.Duration(endSleepTime-startTickTime) * time.Millisecond
			}
			log.Printf("raft id: %d lock release ", rf.me)
		}()

	}
}

func (rf *Raft) leaderSelection() {
	log.Printf("raft: %d start election", rf.me)

	grantedVote := 1 //num of vote this candidate received(1 is his own vote)
	totalVote := 1   //total number of vote
	rf.changeState(CANDIDATES)
	rf.persist()

	cond := sync.NewCond(&rf.mu)

	//loop through every peer ask for votes
	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId == rf.me {
			continue
		}
		//send request vote
		go func(id int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getSize() - 1,
				LastLogTerm:  -1,
			}
			//if lastLogIndex >= 0 {
			//	lastLogTerm = rf.log[lastLogIndex].Term
			//}
			if args.LastLogIndex >= 0 && args.LastLogIndex >= rf.lastIncludedIndex {
				log.Printf("lastLogIndex: %d", args.LastLogIndex)
				log.Printf("lastIncludeIndex: %d", rf.lastIncludedIndex)
				log.Printf("getSize: %d ", rf.getSize())
				if args.LastLogIndex != 0 && args.LastLogIndex == rf.lastIncludedIndex {
					args.LastLogTerm = rf.lastIncludedTerm
				} else {
					args.LastLogTerm = rf.getEntry(args.LastLogIndex).Term
				}
			}
			rf.mu.Unlock()
			//args := RequestVoteArgs{term, candidateID, lastLogIndex, lastLogTerm}
			reply := RequestVoteReply{}
			log.Printf("Raft ID %d in term: %d, send request vote to: %d", rf.me, rf.currentTerm, id)
			//send request to peer
			//cannot add lock here otherwise cause deadlock
			ok := rf.sendRequestVote(id, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			totalVote++
			log.Printf("Raft ID %d in term: %d, totalvote %d, total granted Vote %d PeerID %d",
				rf.me, rf.currentTerm, totalVote, grantedVote, id)
			if !ok {
				cond.Broadcast()
				return
			}
			//vote granted and reply term equal to current term(to avoid old rpc)
			if reply.VoteGranted && rf.currentTerm == reply.Term {
				grantedVote++
				log.Printf("\"Raft ID %d in term: %d vote granted, total granted Vote %d, peerId: %d",
					rf.me, rf.currentTerm, grantedVote, id)
			} else if reply.Term > rf.currentTerm {
				//if receiver term bigger than candidate term, candidate change to follower
				rf.changeState(FOLLOWER)
				//change term
				rf.currentTerm = reply.Term
				rf.persist()
			}

			cond.Broadcast() //tell counting request vote goroutine to start counting
		}(peerId)
	}

	log.Printf("Raft ID %d in term: %d, total granted Vote %d!!!!!",
		rf.me, rf.currentTerm, grantedVote)

	//keep waiting for the end of voting, then counting request vote
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != CANDIDATES {
			return
		}
		//keep waiting until every peer sends vote
		for rf.state == CANDIDATES && grantedVote <= len(rf.peers)/2 && totalVote < len(rf.peers) {
			log.Printf("wating to finish vote")
			cond.Wait()
		}
		//if current candidate get more than half votes, change to leader
		if grantedVote > len(rf.peers)/2 {
			rf.changeState(LEADER)
			log.Printf("Raft Id %d, I AM LEADER NOW, Leader ID %d", rf.me, rf.leaderId)
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.getSize()
				rf.matchIndex[i] = 0
			}
			rf.persist()
			go rf.sendHeartBeat()
		}
	}()

}

func (rf *Raft) sendHeartBeat() {
	for rf.killed() == false {

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.killed() {
				return
			}
			//if not leader just return
			log.Printf("send heart beat!!!!!!!!!!! Raft state %s", rf.state)
			if rf.state != LEADER {
				log.Printf("raft ID %d, state %s. NOT Leader return ", rf.me, rf.state)
				return
			}
			//loop through every peer
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				log.Printf("enter looooop send heart beats")
				if peerId == rf.me {
					continue
				}
				var entry []LogEntry
				go func(id int) {

					rf.mu.Lock()
					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}
					if rf.nextIndex[id] <= rf.lastIncludedIndex {
						go rf.sendSnapshot(id)
						rf.mu.Unlock()
						return
					}
					//rf.mu.Lock() //need to have lock here otherwise, race condition
					args := AppendEntryArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[id] - 1,
						PrevLogTerm:  -1,
						Entries:      entry,
						LeaderCommit: rf.commitIndex,
					}
					if args.PrevLogIndex >= 0 && args.PrevLogIndex >= rf.lastIncludedIndex {
						if args.PrevLogIndex == rf.lastIncludedIndex {
							args.PrevLogTerm = rf.lastIncludedTerm
						} else {
							args.PrevLogTerm = rf.getEntry(args.PrevLogIndex).Term
						}
					}
					if rf.getSize()-1 >= rf.nextIndex[id] && !rf.killed() {
						//send all the entries from nextIndex
						log.Printf("Leader ID: %d: length of log: %d, Peer ID %d: nextIndex = %d",
							rf.me, len(rf.log), id, rf.nextIndex[id])

						args.Entries = rf.log[(rf.nextIndex[id] - rf.lastIncludedIndex):]
						log.Printf("Leader ID: %d sending log: %v", rf.me, args.Entries)
						//rf.persist()
					}
					rf.mu.Unlock()

					reply := AppendEntryReply{}

					ok := rf.sendAppendEntry(id, &args, &reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.LastApplied+1 <= reply.LastIncludedIndex {
						return
					}

					if !ok || rf.state != LEADER || rf.currentTerm != args.Term {
						return
					}
					//if reply term bigger than current raft, current raft change to follower
					if reply.Term > rf.currentTerm {
						log.Printf("change Raft Id %d to follower", rf.me)
						rf.changeState(FOLLOWER)
						rf.persist()
						rf.currentTerm = reply.Term
						return
					}

					if reply.Term == rf.currentTerm && rf.currentTerm == args.Term {

						if reply.Success {
							//update nextIndex and matchIndex of leader for receiver follower
							rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[id] = args.PrevLogIndex + len(args.Entries) + 1

							log.Printf("Follower Replied YES. matchIndex[%d] = %d. "+
								"nextIndex[%d] = %d", id, rf.matchIndex[id], id, rf.nextIndex[id])
							log.Printf("Totol matchIndex %v, nextIndex %v", rf.matchIndex, rf.nextIndex)

							//check if we need to update commitIndex
							rf.updateCommitIndex()

						} else {
							log.Printf("Raft %d: Fowllower Rejected!", rf.me)
							//follower inconsistent log with leader
							//decrement nextIndex
							rf.nextIndex[id] = args.PrevLogIndex

							//use (XLen: length of follower log),
							//(XTerm: the term of conflicting entry),
							//(XIndex:index of the first entry with XTerm) to get faster backUp
							//1. if the next prevLogIndex is bigger than XLen, just set nextIndex = XLen
							if rf.nextIndex[id]-1 >= reply.XLen {
								rf.nextIndex[id] = reply.XLen
							} else {
								//2. s1 4 5 5 XIndex = 1 XTerm = 5
								//   s2 4 6 6 6 (leader)
								//loop through preLogIndex to XIndex, jump to the index of first pair
								for i := rf.nextIndex[id] - 1; i >= reply.XIndex; i-- {
									if i >= rf.lastIncludedIndex && rf.getEntry(i).Term != reply.XTerm {
										rf.nextIndex[id]--
									} else {
										break
									}
								}
							}

						}
					}

				}(peerId)
			}

		}()
		//send heartbeat no more than 10 times per second, 1s = 1000ms
		//means 100ms send once
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	//commitIndex = 1
	//matchIndex[peer 0] = 2
	//matchIndex[peer 1] = 2
	//matchIndex[peer 2] = 1
	//copy entire matchIndex
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	//sort copy in ascending order
	//1, 2, 2
	sort.Ints(matchIndexCopy)
	//if there exists an N such that N > commitIndex and a majority of matchIndex[i] >= N
	//N is the median of matchIndexCopy
	N := matchIndexCopy[len(matchIndexCopy)/2]
	//log.Printf("N : %d, commitIndex: %d, matchIndex: %d", N, rf.commitIndex, rf.matchIndex)
	if N > rf.commitIndex && rf.getEntry(N).Term == rf.currentTerm {
		rf.commitIndex = N
		//log.Printf("rf.log[N].Term: %v, rf.currentTerm %d", rf.log[N].Term, rf.currentTerm)
		//log.Printf("CommitIndex Update to %d", rf.commitIndex)
		rf.applyChecker = true
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) applyLogMessage() {
	for !rf.killed() {

		rf.mu.Lock()
		//this ensures that after server crashes and reconnected, it first applies snapshot
		if rf.lastApplied+1 > rf.lastIncludedIndex {
			rf.applyCond.Broadcast()
		}
		for !rf.applyChecker || rf.lastApplied+1 <= rf.lastIncludedIndex {
			rf.applyCond.Wait()
		}
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied

		rf.applyChecker = false
		//rf.mu.Unlock()
		Messages := make([]ApplyMsg, 0)
		log.Printf("SENDING APPLY MESSAGE")
		for i := lastApplied + 1; i <= commitIndex; i++ {

			cmd := rf.getEntry(i).Command

			msg := ApplyMsg{
				CommandValid: true, Command: cmd, CommandIndex: i}
			log.Printf("Raft: %d, MESSAGE: log size %v, rf.log size %v, commitIndex %d, lastApplied %d",
				rf.me, rf.getSize(), len(rf.log), rf.commitIndex, rf.lastApplied)
			log.Printf("Raft: [%d]: apply index %d", rf.me, msg.CommandIndex)

			Messages = append(Messages, msg)
			rf.lastApplied = i
		}
		rf.mu.Unlock()
		for _, messages := range Messages {
			rf.applyChan <- messages
		}
		time.Sleep(10 * time.Millisecond)
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

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = append(rf.log, LogEntry{Command: 0, Term: 0})

	rf.applyChan = applyCh

	msg := ApplyMsg{
		CommandValid: true,
		//SnapshotValid: false,
		Command:      rf.log[0].Command,
		CommandIndex: 0,
	}
	rf.applyChan <- msg

	rf.applyChecker = false
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.restart = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	//after crash, if exists snapshot, needs to send snapshot first
	if rf.lastIncludedIndex > 0 {
		//rf.mu.Lock()
		log.Printf("Server %d, after crash, sending snapshot to applyChan", rf.me)
		rf.mu.Lock()
		msg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotIndex: rf.lastIncludedIndex,
			SnapshotTerm:  rf.lastIncludedTerm,
		}
		rf.applyChan <- msg
		rf.mu.Unlock()
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
		rf.restart = true
		//log.Printf("NextIndex[%d] : %d ", rf.me, rf.nextIndex[rf.me])
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogMessage()

	return rf
}
