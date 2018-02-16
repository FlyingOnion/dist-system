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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const (
	StateFollower = iota
	StateCandidate
	StateLeader
	numStates
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logEntries  []interface{}

	state int

	commitIndex int
	lastApplied int

	// election timer: 150~300ms
	// vote timer: 250~400ms
	// heartbeat timer: 50~100ms

	voteC chan *RequestVoteArgs
	appC  chan *RequestAppendEntriesArgs
	exitC chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == StateLeader)
	rf.mu.Unlock()
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// AppendEntries RPC arg structure
// also take as heartbeat
//
type RequestAppendEntriesArgs struct {
	Term         int
	Leader       int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []interface{}
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

func randomDuration(min, max int64) time.Duration {
	// 1e6: millisecond
	return time.Duration((min + rand.Int63n(max-min)) * 1e6)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	vote := false
	if args.Term > rf.currentTerm {
		vote = true
		rf.voteC <- args
	}
	// set reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = vote
	fmt.Println(time.Now(), args.CandidateId, "->", rf.me, "received")
	fmt.Println(time.Now(), args.CandidateId, "->", rf.me, "vote result", vote)
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Println(time.Now(), rf.me, "call", server, "rpc start, term:", args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Println(time.Now(), rf.me, "call", server, "rpc end, term:", args.Term)
	return ok
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	success := false
	if args.Term >= rf.currentTerm {
		success = true
		rf.appC <- args
	}
	reply.Success = success
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	// fmt.Println(args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.state == StateLeader

	// Your code here (2B).
	if isLeader {
		raArgs := RequestAppendEntriesArgs{
			Term:   rf.currentTerm,
			Leader: rf.me,
		}
		var raReply RequestAppendEntriesReply
		for i := 0; i < len(rf.peers); i++ {
			rf.sendRequestAppendEntries(i, &raArgs, &raReply)
			if raReply.Success {

			}
		}
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.exitC)
	//	defer func() {
	//		close(rf.appC)
	//		close(rf.voteC)
	//	}()
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
	fmt.Println(time.Now(), "creating raft", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())
	// Initialize timer

	rf.appC = make(chan *RequestAppendEntriesArgs)
	rf.voteC = make(chan *RequestVoteArgs)
	rf.exitC = make(chan bool)

	rf.becomeFollower(1, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	fmt.Println(time.Now(), me, "created")
	return rf
}

// Attention:
// becomeXXX should be called directly in OTHER functions
// and should be called async (use "go") when they are called by each other

func (rf *Raft) becomeFollower(term, leader int) {
	rf.mu.Lock()
	rf.state = StateFollower

	rf.currentTerm = term
	rf.votedFor = leader
	rf.mu.Unlock()
	go func() {
		select {
		case <-rf.exitC:
			return
		case args := <-rf.appC:
			fmt.Println(time.Now(), rf.me, "got hb request from", args.Leader)
			rf.becomeFollower(args.Term, args.Leader)
		case args := <-rf.voteC:
			fmt.Println(time.Now(), rf.me, "got vote request from", args.CandidateId)
			rf.becomeFollower(args.Term, args.CandidateId)
		case <-time.After(randomDuration(150, 300)):
			fmt.Println(time.Now(), rf.me, "become candidate")
			rf.becomeCandidate()
		}
	}()

}

func (rf *Raft) becomeCandidate() {

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.state = StateCandidate

	rf.currentTerm++
	fmt.Println(time.Now(), rf.me, "begin vote, term:", rf.currentTerm)
	rf.votedFor = rf.me
	rf.mu.Unlock()

	// use only once in each vote process
	winVoteC := make(chan bool)
	voteReceiveC := make(chan bool)
	go func() {
		select {
		case <-rf.exitC:
			return
		case args := <-rf.appC:
			fmt.Println(rf.me, "got hb request from", args.Leader)
			fmt.Println(rf.me, "become follower")
			rf.becomeFollower(args.Term, args.Leader)
		case args := <-rf.voteC:
			fmt.Println(rf.me, "got vote request from", args.CandidateId)
			fmt.Println(rf.me, "become follower")
			rf.becomeFollower(args.Term, args.CandidateId)
		case <-winVoteC:
			fmt.Println(rf.me, "become leader")
			rf.becomeLeader()
		case <-time.After(randomDuration(350, 400)):
			rf.becomeCandidate()
		}
	}()

	rvArgs := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	var rvReply RequestVoteReply
	voteCount := 0

	go func() {
		for voteCount <= len(rf.peers)/2 {
			<-voteReceiveC
			voteCount += 1
		}
		if rf.currentTerm == currentTerm && rf.state == StateCandidate {
			fmt.Println(time.Now(), rf.me, "got enough votes")
		}

		close(winVoteC)
		close(voteReceiveC)
	}()

	for i := 0; i < len(rf.peers); i++ {
		go func(peerId int) {
			if peerId == rf.me {
				voteReceiveC <- true
				return
			}
			fmt.Println(time.Now(), "sending vote", rf.me, "->", peerId)
			rf.sendRequestVote(peerId, &rvArgs, &rvReply)
			if rvReply.VoteGranted {
				select {
				case <-winVoteC:
					return
				default:
					voteReceiveC <- true
				}
			} else if rf.currentTerm < rvReply.Term {
				rf.becomeFollower(rvReply.Term, 0)
				return
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.state = StateLeader
	rf.mu.Unlock()

	raArgs := RequestAppendEntriesArgs{
		Term:   rf.currentTerm,
		Leader: rf.me,
	}
	var raReply RequestAppendEntriesReply
	// leader maintainance
	// send heartbeat periodly
	go func() {
		for rf.state == StateLeader {
			select {
			case <-rf.exitC:
				return
			case args := <-rf.appC:
				rf.becomeFollower(args.Term, args.Leader)
			case args := <-rf.voteC:
				rf.becomeFollower(args.Term, args.CandidateId)
			case <-time.After(randomDuration(50, 80)):
				for i := 0; i < len(rf.peers); i++ {
					go func(peerId int) {
						if peerId != rf.me {
							rf.sendRequestAppendEntries(peerId, &raArgs, &raReply)

							// discover a server with higher term
							// turns into follower
							if rf.currentTerm < raReply.Term {
								rf.currentTerm = raReply.Term

								// already in a go func, so we could call it directly
								// state will change and the loop will terminate
								rf.becomeFollower(rf.currentTerm, 0)
								return
							}
						}
					}(i)

				}
			}
		}

	}()

}
