package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const (
	BaseElectionTimeout = 500
	HeartbeatInterval   = 50 * time.Millisecond
	AppliedInterval     = 10 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	lastElectionTime time.Time
	state            RaftState
	applyCh          chan raftapi.ApplyMsg
	logs             []Entry

	currentTerm int
	votedFor    int // -1 equal None
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("Error: failed to read persist state\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.lastElectionTime = time.Now()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	lastLog := rf.logs[len(rf.logs)-1]
	upToDate := args.LastLogTerm > lastLog.CommandTerm ||
		(args.LastLogTerm == lastLog.CommandTerm && args.LastLogIndex >= lastLog.CommandIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastElectionTime = time.Now()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = Follower
	}

	if rf.state == Candidate {
		rf.state = Follower
	}

	reply.Term = rf.currentTerm
	rf.lastElectionTime = time.Now()

	if args.PrevLogIndex >= len(rf.logs) {
		return
	}

	if rf.logs[args.PrevLogIndex].CommandTerm != args.PrevLogTerm {
		conflictTerm := rf.logs[args.PrevLogIndex].CommandTerm
		reply.ConflictEntryTerm = conflictTerm

		var conflictIndex int
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.logs[i-1].CommandTerm != conflictTerm {
				conflictIndex = i
				break
			}
		}
		reply.ConflictEntryIndex = conflictIndex

		return
	}

	reply.Success = true
	for _, entry := range args.Entries {
		if entry.CommandIndex >= len(rf.logs) ||
			rf.logs[entry.CommandIndex].CommandTerm != entry.CommandTerm {
			rf.logs = append([]Entry{}, append(rf.logs[:args.PrevLogIndex+1], args.Entries...)...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit >= len(rf.logs) {
			rf.commitIndex = len(rf.logs) - 1
		}
	}
}

func (rf *Raft) appendEntriesByPeerId(peerId int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	prevIndex := min(rf.nextIndex[peerId]-1, rf.logs[len(rf.logs)-1].CommandIndex)
	prevLog := rf.logs[prevIndex]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLog.CommandIndex,
		PrevLogTerm:  prevLog.CommandTerm,
		Entries:      rf.logs[rf.nextIndex[peerId]:],
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(peerId, &args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if reply.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.state = Follower
		return
	}

	if reply.Success {
		rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
		for index, log := range rf.logs {
			count := 1
			for id := range rf.peers {
				if id != rf.me && rf.matchIndex[id] >= index {
					count++
				}
			}

			if count > len(rf.peers)/2 && index > rf.commitIndex &&
				log.CommandTerm == rf.currentTerm {
				rf.commitIndex = index
			}
		}
	} else {
		rf.nextIndex[peerId] = max(1, reply.ConflictEntryIndex-1)
	}
}

func (rf *Raft) sendHeartbeats() {
	for id := range rf.peers {
		if id == rf.me {
			continue
		}

		go func() {
			for {
				rf.appendEntriesByPeerId(id)
				time.Sleep(HeartbeatInterval)
			}
		}()
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.lastElectionTime = time.Now()
	lastLog := rf.logs[len(rf.logs)-1]

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.CommandIndex,
		LastLogTerm:  lastLog.CommandTerm,
	}
	rf.persist()
	rf.mu.Unlock()

	votesCh := make(chan bool, len(rf.peers)-1)
	votes := 1
	totalVotes := 1

	for id := range rf.peers {
		if id == rf.me {
			continue
		}

		go func() {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(id, &args, &reply); !ok {
				votesCh <- false
				return
			}

			votesCh <- reply.VoteGranted
		}()
	}

	for votes <= len(rf.peers)/2.0 && totalVotes != len(rf.peers) {
		granted := <-votesCh
		if granted {
			votes++
		}
		totalVotes++
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if votes > len(rf.peers)/2 && rf.state == Candidate {
		rf.state = Leader
		rf.lastElectionTime = time.Now()
		lastLogIndex := rf.logs[len(rf.logs)-1].CommandIndex
		for id := range rf.peers {
			rf.nextIndex[id] = lastLogIndex + 1
			rf.matchIndex[id] = 0
		}

		rf.sendHeartbeats()
	}
}

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
func (rf *Raft) Start(command any) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader {
		return len(rf.logs) - 1, rf.currentTerm, false
	}

	newEntry := Entry{
		CommandTerm:  rf.currentTerm,
		CommandIndex: len(rf.logs),
		Command:      command,
	}

	rf.logs = append(rf.logs, newEntry)

	for id := range rf.peers {
		if id != rf.me {
			continue
		}
		go rf.appendEntriesByPeerId(id)
	}

	return len(rf.logs) - 1, rf.currentTerm, true
}

func (rf *Raft) ticker() {
	for {
		// Your code here (3A)
		// Check if a leader election should be started.

		electionTimeout := BaseElectionTimeout + (rand.Int63() % 300)

		rf.mu.Lock()
		isElectionTimeout := time.Now().
			After(rf.lastElectionTime.Add(time.Duration(electionTimeout) * time.Millisecond))
		if isElectionTimeout && rf.state != Leader {
			go rf.election()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) commitEntries() {
	for {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
			entries := make([]Entry, commitIndex-lastApplied)
			copy(entries, rf.logs[lastApplied+1:commitIndex+1])
			rf.mu.Unlock()

			for _, entry := range entries {
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.CommandIndex,
				}
			}

			rf.mu.Lock()
			if rf.lastApplied < commitIndex {
				rf.lastApplied = commitIndex
			}
		}

		rf.mu.Unlock()
		time.Sleep(AppliedInterval)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *tester.Persister,
	applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.logs = make([]Entry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.logs = append(rf.logs, Entry{})
	lastLog := rf.logs[len(rf.logs)-1]

	for id := range peers {
		if id != rf.me {
			rf.nextIndex[id], rf.matchIndex[id] = lastLog.CommandIndex+1, 0
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitEntries()

	return rf
}
