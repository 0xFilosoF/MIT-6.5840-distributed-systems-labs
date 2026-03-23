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

	lastIncludedTerm  int
	lastIncludedIndex int
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
	if rf.persister.ReadSnapshot() != nil {
		rf.persister.Save(rf.encodePersistState(), rf.persister.ReadSnapshot())
	} else {
		rf.persister.Save(rf.encodePersistState(), nil)
	}
}

func (rf *Raft) encodePersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
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
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Error: failed to read persist state\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = rf.logs[0].CommandIndex
		rf.commitIndex = rf.logs[0].CommandIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.logs[0].CommandIndex >= index {
		return
	}

	firstLogIndex := rf.logs[0].CommandIndex

	lastSnapshotEntry := rf.logs[index-firstLogIndex]
	rf.lastIncludedIndex = lastSnapshotEntry.CommandIndex
	rf.lastIncludedTerm = lastSnapshotEntry.CommandTerm

	rf.logs = append([]Entry{}, rf.logs[index-firstLogIndex:]...)
	rf.logs[0].Command = nil
	rf.persister.Save(rf.encodePersistState(), snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = Follower

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.lastElectionTime = time.Now()

	if !args.Done {
		rf.mu.Unlock()
		return
	}

	firstLogIndex := rf.logs[0].CommandIndex
	lastLogIndex := rf.logs[len(rf.logs)-1].CommandIndex
	if args.LastIncludedIndex >= firstLogIndex && args.LastIncludedIndex <= lastLogIndex {
		index := args.LastIncludedIndex - firstLogIndex
		if rf.logs[index].CommandTerm == args.LastIncludedTerm {
			rf.logs = append([]Entry{}, rf.logs[index:]...)
			rf.logs[0].Command = nil
		} else {
			rf.logs = []Entry{{
				CommandIndex: args.LastIncludedIndex,
				CommandTerm:  args.LastIncludedTerm,
				Command:      nil,
			}}
		}
	} else {
		rf.logs = []Entry{{
			CommandIndex: args.LastIncludedIndex,
			CommandTerm:  args.LastIncludedTerm,
			Command:      nil,
		}}
	}

	rf.persister.Save(rf.encodePersistState(), args.Data)

	newMsg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.mu.Unlock()
	rf.applyCh <- newMsg
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
	firstLogIndex := rf.logs[0].CommandIndex
	if args.PrevLogIndex < firstLogIndex {
		reply.ConflictEntryIndex = firstLogIndex
		return
	}

	prevLogIndex := args.PrevLogIndex - firstLogIndex
	if prevLogIndex >= len(rf.logs) {
		reply.ConflictEntryIndex = rf.logs[len(rf.logs)-1].CommandIndex + 1
		return
	}

	if rf.logs[prevLogIndex].CommandTerm != args.PrevLogTerm {
		conflictTerm := rf.logs[prevLogIndex].CommandTerm
		reply.ConflictEntryTerm = conflictTerm

		var conflictIndex int
		for i := prevLogIndex; i > 0; i-- {
			if rf.logs[i-1].CommandTerm != conflictTerm {
				conflictIndex = i
				break
			}
		}
		reply.ConflictEntryIndex = conflictIndex + firstLogIndex

		return
	}

	reply.Success = true
	for idx, entry := range args.Entries {
		logIndex := entry.CommandIndex - firstLogIndex
		if logIndex >= len(rf.logs) || rf.logs[logIndex].CommandTerm != entry.CommandTerm {
			rf.logs = append([]Entry{}, append(rf.logs[:logIndex], args.Entries[idx:]...)...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit-firstLogIndex >= len(rf.logs) {
			rf.commitIndex = rf.logs[len(rf.logs)-1].CommandIndex
		}
	}
}

func (rf *Raft) appendEntriesByPeerId(peerId int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	originalTerm := rf.currentTerm
	firstIndex := rf.logs[0].CommandIndex
	nextIndex := rf.nextIndex[peerId]
	if nextIndex <= firstIndex {
		snapshotArgs := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Offset:            0,
			Data:              rf.persister.ReadSnapshot(),
			Done:              true,
		}
		rf.mu.Unlock()

		snapshotReply := InstallSnapshotReply{}
		if ok := rf.sendInstallSnapshot(peerId, &snapshotArgs, &snapshotReply); !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if snapshotReply.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = snapshotReply.Term
			rf.state = Follower
			return
		}
		if rf.state != Leader || rf.currentTerm != originalTerm {
			return
		}
		rf.nextIndex[peerId] = snapshotArgs.LastIncludedIndex + 1
		rf.matchIndex[peerId] = snapshotArgs.LastIncludedIndex
		return
	}

	prevLogIndex := nextIndex - 1
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.logs[prevLogIndex-firstIndex].CommandIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstIndex].CommandTerm,
		LeaderCommit: rf.commitIndex,
	}
	if nextIndex-firstIndex < len(rf.logs) {
		args.Entries = append([]Entry{}, rf.logs[nextIndex-firstIndex:]...)
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
	if rf.state != Leader || rf.currentTerm != originalTerm {
		return
	}

	if reply.Success {
		rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1

		lastLogIndex := rf.logs[len(rf.logs)-1].CommandIndex
		for i := lastLogIndex; i > rf.commitIndex; i-- {
			if rf.logs[i-rf.logs[0].CommandIndex].CommandTerm != rf.currentTerm {
				continue
			}

			count := 1
			for id := range rf.peers {
				if id != rf.me && rf.matchIndex[id] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
				break
			}
		}
		return
	}

	if reply.ConflictEntryIndex > 0 {
		rf.nextIndex[peerId] = reply.ConflictEntryIndex
	} else if rf.nextIndex[peerId] > rf.lastIncludedIndex+1 {
		rf.nextIndex[peerId]--
	}
}

func (rf *Raft) sendHeartbeats() {
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go rf.appendEntriesByPeerId(id)
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	startedTerm := rf.currentTerm
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.lastElectionTime = time.Now()
	lastLog := rf.logs[len(rf.logs)-1]

	args := RequestVoteArgs{
		Term:         startedTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.CommandIndex,
		LastLogTerm:  lastLog.CommandTerm,
	}
	rf.persist()
	rf.mu.Unlock()

	votesCh := make(chan *RequestVoteReply, len(rf.peers)-1)
	votes := 1
	totalVotes := 1

	for id := range rf.peers {
		if id == rf.me {
			continue
		}

		go func() {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(id, &args, reply); !ok {
				votesCh <- nil
				return
			}

			votesCh <- reply
		}()
	}

	for votes <= len(rf.peers)/2.0 && totalVotes != len(rf.peers) {
		reply := <-votesCh
		totalVotes++
		if reply == nil {
			continue
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.lastElectionTime = time.Now()
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if rf.state != Candidate || rf.currentTerm != startedTerm {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if reply.VoteGranted {
			votes++
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state != Candidate || rf.currentTerm != startedTerm {
		return
	}
	if votes > len(rf.peers)/2.0 {
		rf.state = Leader
		rf.lastElectionTime = time.Now()
		lastLogIndex := rf.logs[len(rf.logs)-1].CommandIndex
		for id := range rf.peers {
			rf.nextIndex[id] = lastLogIndex + 1
			rf.matchIndex[id] = 0
		}
		rf.matchIndex[rf.me] = lastLogIndex
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

	newIndex := rf.logs[len(rf.logs)-1].CommandIndex + 1
	if rf.state != Leader {
		return newIndex, rf.currentTerm, false
	}

	newEntry := Entry{
		CommandTerm:  rf.currentTerm,
		CommandIndex: newIndex,
		Command:      command,
	}

	rf.logs = append(rf.logs, newEntry)
	rf.matchIndex[rf.me] = newIndex
	rf.nextIndex[rf.me] = newIndex + 1
	rf.persist()

	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go rf.appendEntriesByPeerId(id)
	}

	return newIndex, rf.currentTerm, true
}

func (rf *Raft) ticker() {
	for {
		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		state := rf.state
		electionTimeout := time.Duration(BaseElectionTimeout+(rand.Int63()%300)) * time.Millisecond
		electionDeadline := rf.lastElectionTime.Add(electionTimeout)
		rf.mu.Unlock()

		if state == Leader {
			rf.sendHeartbeats()
			time.Sleep(HeartbeatInterval)
			continue
		}

		if time.Now().After(electionDeadline) {
			rf.election()
			continue
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) commitEntries() {
	for {
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(AppliedInterval)
			continue
		}

		nextIndex := rf.lastApplied + 1
		if nextIndex <= rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
			rf.mu.Unlock()
			time.Sleep(AppliedInterval)
			continue
		}

		firstLogIndex := rf.logs[0].CommandIndex
		entry := rf.logs[nextIndex-firstLogIndex]
		rf.lastApplied = nextIndex
		rf.mu.Unlock()

		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			CommandIndex: entry.CommandIndex,
			Command:      entry.Command,
		}
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
		rf.nextIndex[id], rf.matchIndex[id] = lastLog.CommandIndex+1, 0
	}
	rf.matchIndex[rf.me] = lastLog.CommandIndex

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitEntries()

	return rf
}
