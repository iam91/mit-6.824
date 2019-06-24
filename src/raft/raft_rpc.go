package raft

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries 		[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term 	int
	Success	bool
}

func (rf *Raft) AppendEntries(args * AppendEntriesArgs, reply * AppendEntriesReply) {
	reply.Term = rf.currentTerm

	//fmt.Printf("AppendEntries %d from %d, %d, %d\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if len(rf.log) - 1 < args.PrevLogIndex {
		reply.Success = false
		rf.syncTerm(args.Term)
		return
	} else {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// force follower to be consistent with leader
			rf.log = rf.log[:args.PrevLogIndex]
			reply.Success = false
			rf.syncTerm(args.Term)
			return
		}
	}

	reply.Success = true
	rf.resetTimerChan <- true
	if args.Entries != nil && len(args.Entries) != 0 {
		rf.log = append(rf.log, args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = IntMin(args.LeaderCommit, len(rf.log) - 1)
		rf.applyCommand()
	}
	rf.syncTerm(args.Term)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoteGranted	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm

	//fmt.Printf("RequestVote %d from %d, %d, %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	rf.resetTimerChan <- true
	lastLogEntry := rf.log[len(rf.log) - 1]
	logUpToDate := lastLogEntry.compareUpToDate(args.LastLogIndex, args.LastLogTerm) <= 0

	if (rf.votedFor == Const_Voted_Null || rf.votedFor == args.CandidateId) && logUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	rf.syncTerm(args.Term)
}