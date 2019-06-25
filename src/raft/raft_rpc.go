package raft

import (
	"fmt"
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
		rf.log = append(rf.log, args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = IntMin(args.LeaderCommit, len(rf.log) - 1)
		rf.applyCommand()
	}
	rf.syncTerm(args.Term)
}

func (rf *Raft) broadcastAppendEntries() {
	entries := make([]LogEntry, 0)

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex}

	commitCheckChan := make(chan int, 1)

	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				prevIndex := rf.nextIndex[peer] - 1
				if len(rf.log) - 1 >= rf.nextIndex[peer] {
					entries = rf.log[rf.nextIndex[peer]:]
				}

				args.PrevLogIndex = prevIndex
				args.PrevLogTerm = rf.log[prevIndex].Term
				args.Entries = entries
				reply := AppendEntriesReply{}

				ok := false
				for !ok {
					ok = rf.sendAppendEntries(peer, &args, &reply)
				}

				if reply.Term <= args.Term {
					if !reply.Success {
						prevIndex--
					} else {
						if len(entries) > 0 {
							rf.nextIndex[peer] = prevIndex + len(entries) + 1
							rf.matchIndex[peer] = prevIndex + len(entries)
							commitCheckChan <- peer
						}
					}
				} else {
					rf.syncTerm(reply.Term)
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
			//fmt.Printf("[[[[[[[[[ %d: %d, %d\n", peer, matchIndex, lowerN)
			if matchIndex >= lowerN {
				fmt.Printf("------- %d, %d, %d\n", peer, matchIndex, lowerN)
				//fmt.Printf(">>>>>>>>>>> %d, %d, %d\n", peer, rf.log[matchIndex].Term, rf.currentTerm)
				if rf.log[matchIndex].Term == rf.currentTerm {
					cnt++
					if upperN < 0 {
						upperN = rf.matchIndex[peer]
					} else {
						if matchIndex < upperN {
							upperN = matchIndex
						}
					}
				}
			}
			//fmt.Printf("(((((( %d, %d, %d\n", cnt, upperN, lowerN)
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
		CandidateId: rf.me}

	votesChan := make(chan int, 1)

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
						votesChan <- 1
						rf.mu.Unlock()
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