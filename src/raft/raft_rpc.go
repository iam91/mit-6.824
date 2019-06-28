package raft

import (
	"time"
)

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
		rf.log = rf.log[:args.PrevLogIndex + 1] // very important
		rf.log = append(rf.log, args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = IntMin(args.LeaderCommit, len(rf.log) - 1)
		rf.applyCommand()
	}
	rf.syncTerm(args.Term)
}

func (rf *Raft) broadcastAppendEntries() {

	args0 := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex}

	commitCheckChan := make(chan int, len(rf.peers) - 1)

	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				notMatched := true

				for notMatched {
					var prevIndex int
					var entries []LogEntry
					if len(rf.log) - 1 >= rf.nextIndex[peer] {
						entries = rf.log[rf.nextIndex[peer]:]
						prevIndex = rf.nextIndex[peer] - 1
					} else {
						prevIndex = len(rf.log) - 1
						entries = make([]LogEntry, 0)
					}

					args := AppendEntriesArgs{
						Term:         	args0.Term,
						LeaderId:     	args0.LeaderId,
						LeaderCommit: 	args0.LeaderCommit,
						PrevLogIndex:	prevIndex,
						PrevLogTerm: 	rf.log[prevIndex].Term,
						Entries: 		entries}

					reply := AppendEntriesReply{}

					ok := false
					for !ok {
						ok = rf.sendAppendEntries(peer, &args, &reply)
					}

					if reply.Term <= args.Term {
						if !reply.Success {
							rf.nextIndex[peer]--
						} else {
							if len(entries) > 0 {
								rf.nextIndex[peer] = prevIndex + len(entries) + 1
								rf.matchIndex[peer] = prevIndex + len(entries)
								commitCheckChan <- peer
							}
							notMatched = false
						}
					} else {
						rf.syncTerm(reply.Term)
						notMatched = false
					}
				}

			}(i)
		}
	}

	go rf.mergeAppendEntries(commitCheckChan)
}

func (rf *Raft) mergeAppendEntries(commitCheckChan chan int) {
	lowerN := rf.commitIndex + 1
	upperN := -1
	cnt := 0

	for {
		select {
		case peer := <- commitCheckChan:
			// majority check whether to commit
			matchIndex := rf.matchIndex[peer]
			if matchIndex >= lowerN {
				if rf.log[matchIndex].Term == rf.currentTerm {
					cnt++
					if upperN < 0 {
						upperN = matchIndex
					} else {
						if matchIndex < upperN {
							upperN = matchIndex
						}
					}
				}
			}
			if cnt >= len(rf.peers) / 2 && upperN >= lowerN{
				rf.commitIndex = upperN
				rf.applyCommand()
			}
		}
	}
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

func (rf *Raft) broadcastRequestVotes() {
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm: rf.log[len(rf.log) - 1].Term,
		CandidateId: rf.me}

	votesChan := make(chan int, len(rf.peers) - 1)

	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				reply := RequestVoteReply{}

				ok := false
				for !ok {
					ok = rf.sendRequestVote(peer, &args, &reply)
				}

				if reply.Term <= args.Term {
					if ok &&reply.VoteGranted {
						rf.mu.Lock()
						rf.votes++
						rf.mu.Unlock()
						votesChan <- 1
					}
				} else {
					rf.syncTerm(reply.Term)
				}
			}(i)
		}
	}

	// 汇总投票结果
	go rf.mergeRequestVotes(votesChan)
}

func (rf *Raft) mergeRequestVotes(votesChan chan int) {
	for  {
		select {
		case <- votesChan:
			if rf.votes >= len(rf.peers) / 2 {
				rf.switchRole(Role_Leader)
				return
			} else {
				rf.timer.Reset(time.Duration(ElectionTimeout()) * time.Millisecond)
			}
		}
	}
}