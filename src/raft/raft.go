package raft

// todo: follower网络被切断term增加，重新加入时出现问题

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
// import "labgob"

const DEBUG = true

const Role_Dead			= 0
const Role_Leader 		= 1
const Role_Follower 	= 2
const Role_Candidate 	= 3

const Const_Init_Term 				= 0
const Const_Voted_Null 				= -1
const Const_Min_Election_Timeout 	= 500
const Const_Max_Election_Timeout 	= 900
const Const_Heartbeat 				= 150

func NullLog() LogEntry {
	logEntry := LogEntry{
		Term: Const_Init_Term,
		LogIndex: 0}
	return logEntry
}

func IntMin(a int, b int ) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func ElectionTimeout() int64 {
	span := Const_Max_Election_Timeout - Const_Min_Election_Timeout
	return Const_Min_Election_Timeout + int64(rand.Float32() * float32(span))
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term 		int
	LogIndex	int
	Command 	interface{}
}

func (le *LogEntry) compareUpToDate(lastLogIndex int, lastLogTerm int) int {
	if le.Term == lastLogTerm {
		return le.LogIndex - lastLogIndex
	} else {
		return le.Term - lastLogTerm
	}
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

	// persistent state on all servers
	currentTerm int
	votedFor	int
	log 		[]LogEntry
	// volatile state on all servers
	commitIndex	int
	lastApplied	int
	// volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int

	role 			int
	timer 			*time.Timer

	roleChan		chan int
	syncTermChan	chan int
	resetTimerChan	chan bool

	applyCh			chan ApplyMsg
	votes			int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.currentTerm
	isLeader = rf.role == Role_Leader
	return term, isLeader
}

func (rf *Raft) GetRole() int {
	return rf.role
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

func (rf *Raft) syncTerm(term int) bool {
	var delay = term > rf.currentTerm
	if delay {
		rf.syncTermChan <- term
	}
	return delay
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := rf.role == Role_Leader

	if isLeader {
		index = len(rf.log)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{term, index, command})
	}
	return index, term, isLeader
}

func (rf *Raft) applyCommand() {
	//fmt.Printf(">>> server %d applyCommand: %d %d\n", rf.me, rf.commitIndex, rf.lastApplied)
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
		rf.applyCh <- msg
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.switchRole(Role_Dead)
}

func (rf *Raft) switchRole(newRole int) {
	if rf.timer != nil {
		rf.timer.Stop()
	}
	rf.roleChan <- newRole
	rf.drainChannels()
}

func (rf *Raft) newTimer(timeout int64) *time.Timer {
	return time.NewTimer(time.Duration(timeout) * time.Millisecond)
}

func (rf *Raft) startTimer(timeout int64) {
	rf.timer = rf.newTimer(timeout)
}

func (rf *Raft) resetTimer(timeout int64) bool {
	return rf.timer.Reset(time.Duration(timeout) * time.Millisecond)
}

func (rf *Raft) drainChannels() {
	for {
		select {
		case <- rf.syncTermChan:
		case <- rf.resetTimerChan:
		default:
			return
		}
	}
}

func (rf *Raft) initChannels() {
	rf.roleChan = make(chan int, 1)
	rf.syncTermChan = make(chan int, 1)
	rf.resetTimerChan = make(chan bool, 1)
}

func (rf *Raft) startFollower() {
	//fmt.Printf(">>> Server %d in role : follower\n", rf.me)
	rf.startTimer(ElectionTimeout())

	for {
		select {
		case <- rf.resetTimerChan:
			rf.resetTimer(ElectionTimeout())
		default:
			select {
			case <- rf.resetTimerChan:
				rf.resetTimer(ElectionTimeout())
			case <- rf.timer.C:
				rf.votedFor = Const_Voted_Null
				rf.switchRole(Role_Candidate)
				return
			case term := <- rf.syncTermChan:
				rf.currentTerm = term
				rf.votedFor = Const_Voted_Null
				fmt.Printf(">>> Server %d switch role to: %d\n", rf.me, Role_Follower)
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.votedFor = rf.me
	rf.votes = 1
	rf.broadcastRequestVotes()
	if DEBUG {
		fmt.Printf(">>>> Server %d start election at term %d\n", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) startCandidate() {
	//fmt.Printf(">>> Server %d in role : candidate\n", rf.me)
	rf.startTimer(ElectionTimeout())
	rf.currentTerm++
	rf.startElection()

	for {
		select {
		case <-rf.resetTimerChan:
			rf.switchRole(Role_Follower)
			return
		case term := <-rf.syncTermChan:
			rf.currentTerm = term
			rf.votedFor = Const_Voted_Null
			rf.switchRole(Role_Follower)
			return
		default:
			select {
			case <-rf.resetTimerChan:
				rf.switchRole(Role_Follower)
				return
			case term := <-rf.syncTermChan:
				rf.currentTerm = term
				rf.votedFor = Const_Voted_Null
				rf.switchRole(Role_Follower)
				return
			case <-rf.timer.C:
				rf.resetTimer(ElectionTimeout())
				rf.startElection()
			}
		}
	}
}

func (rf *Raft) initIndices() {
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
}

func (rf *Raft) startLeader() {
	//fmt.Printf(">>> Server %d in role : leader\n", rf.me)
	rf.initIndices()
	rf.broadcastAppendEntries()
	rf.timer = rf.newTimer(Const_Heartbeat)

	for {
		select {
		case term := <- rf.syncTermChan:
			rf.currentTerm = term
			rf.votedFor = Const_Voted_Null
			rf.switchRole(Role_Follower)
			return
		default:
			select {
			case term := <- rf.syncTermChan:
				rf.currentTerm = term
				rf.votedFor = Const_Voted_Null
				rf.switchRole(Role_Follower)
				return
			case <- rf.timer.C:
				rf.broadcastAppendEntries()
				rf.resetTimer(Const_Heartbeat)
			}
		}
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.initChannels()

	rf.currentTerm = Const_Init_Term
	rf.votedFor = Const_Voted_Null
	rf.log = []LogEntry{NullLog()}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.roleChan <- Role_Follower
	go func() {
		for {
			role := <- rf.roleChan
			if DEBUG {
				fmt.Printf(">>> Server %d switch role to: %d\n", me, role)
			}
			switch rf.role = role; rf.role {
			case Role_Leader:
				go rf.startLeader()
			case Role_Follower:
				go rf.startFollower()
			case Role_Candidate:
				go rf.startCandidate()
			case Role_Dead:
				return
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

