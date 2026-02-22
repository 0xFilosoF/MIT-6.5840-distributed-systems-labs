package raft

// http://nil.csail.mit.edu/6.5840/2026/papers/raft-extended.pdf

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type Entry struct {
	CommandTerm  int
	CommandIndex int
	Command      any
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictEntryTerm  int
	ConflictEntryIndex int
}
