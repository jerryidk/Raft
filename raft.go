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
	//	"bytes"

	"bytes"
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	leader    int = 0
	candidate int = 1
	follower  int = 2
)

func min(l int, r int) int {
	if l > r {
		return r
	} else {
		return l
	}
}

func max(l int, r int) int {
	if l < r {
		return r
	} else {
		return l
	}
}

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

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// persistent
	CurrentTerm int
	VotedFor    int
	Log         []Entry
	commitIndex int           // highest log entry index commited
	nextIndex   []int         // higest log entry index will be send out
	matchIndex  []int         // highest log entry index has been replicated
	majority    int           // len(peers)/2
	votes       int           // number of votes received during election
	whoami      int           // 0 leader, 1 candidate, 2 follower
	applyCh     chan ApplyMsg // reply back to client once commit
	lastReceive time.Time
	lastApplied int
}

// return identity of this server, if it's leader, second return val is term
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.whoami == leader
}

// Save CurrentTerm, VotedFor, Log
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []Entry
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Log) != nil {
		fmt.Println("Decode error in readPersist")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// hanlder
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if rf.CurrentTerm > args.Term {
		return
	}

	if args.Term > rf.CurrentTerm {
		DPrintf("RAFT %d: demote to follower from current term %d to new term in RequestVote %d  \n", rf.me, rf.CurrentTerm, args.Term)
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.whoami = follower
	}

	// Raft determines which of two log#s is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {

		if args.LastLogTerm > rf.Log[len(rf.Log)-1].Term {
			goto success
		}

		if args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex+1 >= len(rf.Log) {
			goto success
		}

		DPrintf("RAFT %d: Deny vote because candidate last term %d <= voter last term %d or length of candidate log is short\n",
			rf.me,
			args.LastLogTerm,
			rf.Log[len(rf.Log)-1].Term)

		return
	success:
		DPrintf("RAFT %d: Vote for %d \n", rf.me, args.CandidateId)
		rf.VotedFor = args.CandidateId
		rf.lastReceive = time.Now()
		reply.VoteGranted = true
	}

}

func (rf *Raft) SendRequestVote(server int, args RequestVoteArgs, reply RequestVoteReply) {

	rf.mu.Lock()
	if rf.whoami != candidate {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.CurrentTerm < reply.Term {
			DPrintf("RAFT %d: demote to follower from current term %d to new term %d  \n", rf.me, rf.CurrentTerm, reply.Term)
			rf.CurrentTerm = reply.Term
			rf.whoami = follower
			rf.VotedFor = -1
			rf.persist()
			return
		}

		if rf.whoami == candidate && rf.CurrentTerm == args.Term && reply.VoteGranted {
			rf.votes++
			DPrintf("RAFT %d:  vote count %d\n", rf.me, rf.votes)
		}
	}
}

// AppendEntry ...
type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	FirstConflict int //First entry index that contains ConflictTerm
	LastIndex     int //The Index of the last entry index of server's log
	Success       bool
}

// Find the first entry index that has a conflict given a term, -1 if none found.
// Must be locked around
func (rf *Raft) FindFirstConflict(term int) int {
	for i := 0; i < len(rf.Log); i++ {
		if rf.Log[i].Term == term {
			return i
		}
	}
	return -1
}

// receiver implementation
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.Success = true
	if rf.CurrentTerm > args.Term {
		reply.Success = false
		DPrintf("RAFT %d: fail to update entries b/c term is too low \n", rf.me)
		return
	}

	// Valid leader
	defer rf.persist()

	rf.lastReceive = time.Now()
	rf.whoami = follower
	if args.Term > rf.CurrentTerm {
		DPrintf("RAFT %d: demote to follower from current term %d to new term %d in AppendEntries \n", rf.me, rf.CurrentTerm, args.Term)
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}

	// entries to be appended index are too high
	if args.PrevLogIndex > len(rf.Log)-1 {
		reply.Success = false
		reply.LastIndex = len(rf.Log) - 1
		reply.FirstConflict = 0
		DPrintf("RAFT %d: fail to update entries b/c prelogindex too high\n", rf.me)
		return
	}

	// entries to be appended index doesn't match
	// delete PreLogIndex and up
	if prevLogEntry := rf.Log[args.PrevLogIndex]; prevLogEntry.Term != args.PrevLogTerm {
		reply.Success = false
		if index := rf.FindFirstConflict(prevLogEntry.Term); index > 0 {
			reply.FirstConflict = index
		}
		rf.Log = rf.Log[:args.PrevLogIndex]
		reply.LastIndex = len(rf.Log) - 1
		DPrintf("RAFT %d: fail to update entries bc prevlogterm doens't match\n", rf.me)
		return
	}

	DPrintf("RAFT %d: success to update entries from %d \n", rf.me, args.PrevLogIndex+1)

	// once gets here, it is guranteed that
	// this peer log length > sender log length - 1
	for i := 0; i < len(args.Entries); i++ {
		entry := args.Entries[i]
		//DPrintf("RAFT %d: updating entry index %v in AE\n", rf.me, entry)

		if len(rf.Log) == entry.Index {
			rf.Log = append(rf.Log, entry)
			continue
		}

		// peer has this entry already, check term
		if rf.Log[entry.Index].Term != entry.Term {
			rf.Log[entry.Index] = entry
			rf.Log = rf.Log[:entry.Index+1]
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.Log)-1)
	}

	reply.LastIndex = len(rf.Log) - 1
}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	if rf.whoami != leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.whoami != leader {
			return
		}

		if reply.Success {
			if len(args.Entries) > 0 {
				last_apply_idx := min(args.Entries[len(args.Entries)-1].Index, reply.LastIndex)
				rf.nextIndex[server] = last_apply_idx + 1
				rf.matchIndex[server] = last_apply_idx
				// update commit index
				for N := len(rf.Log) - 1; N > rf.commitIndex; N-- {
					if rf.Log[N].Term != rf.CurrentTerm {
						continue
					}

					acceptNumber := 1
					for i := 0; i < len(rf.matchIndex); i++ {
						if i != rf.me && rf.matchIndex[i] >= N {
							acceptNumber++
						}
					}

					if acceptNumber > rf.majority {
						rf.commitIndex = N
						//rf.cond.Broadcast()
					}
				}
			}
		} else {
			// go back as follower bc term is outdated
			if reply.Term > rf.CurrentTerm {
				DPrintf("RAFT %d: demote to follower from current term %d to new term %d  \n", rf.me, rf.CurrentTerm, reply.Term)
				rf.CurrentTerm = reply.Term
				rf.whoami = follower
				rf.lastReceive = time.Now()
				rf.VotedFor = -1
				rf.persist()
			} else {
				// log consistency failure
				rf.nextIndex[server] = max(rf.nextIndex[server]-1, 1)

				// If PrevLogIndex is too high, we can send at the top of the follower's log
				if reply.LastIndex >= 0 {
					rf.nextIndex[server] = min(reply.LastIndex+1, rf.nextIndex[server])
				}

				// If PrevLogTerm conflics, we can use first entry term in follower to skip whole term.
				if reply.FirstConflict > 0 {
					rf.nextIndex[server] = min(reply.FirstConflict, rf.nextIndex[server])
				}

				DPrintf("RAFT %d: retry send append entry %d \n", rf.me, server)
				rf.CreateAndSendAppendEntries(server)
			}
		}
	}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.Log)
	term := rf.CurrentTerm
	isLeader := rf.whoami == leader
	if isLeader {
		entry := Entry{index, term, command}
		rf.Log = append(rf.Log, entry)
		DPrintf("RAFT %d: entry %d entered into log \n", rf.me, entry)
		rf.SendoutRPCs(leader)
		rf.persist()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return time between min - max ms
func GetTimeout(min int, max int) time.Duration {
	m := big.NewInt(int64(max - min))
	timeout, _ := rand.Int(rand.Reader, m)
	return time.Duration(timeout.Int64()+int64(min)) * time.Millisecond
}

func (rf *Raft) CreateAndSendAppendEntries(server int) {
	sendIndex := rf.nextIndex[server]
	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: sendIndex - 1,
		PrevLogTerm:  rf.Log[sendIndex-1].Term,
	}

	if sendIndex < len(rf.Log) && sendIndex >= 0 {
		args.Entries = make([]Entry, len(rf.Log[sendIndex:]))
		copy(args.Entries, rf.Log[sendIndex:])
	}
	reply := AppendEntriesReply{}
	DPrintf("RAFT %d: sending out Append Entries at term %d \n", rf.me, rf.CurrentTerm)

	go rf.SendAppendEntries(server, args, reply)

}

func (rf *Raft) CreateAndSendRequestVote(server int) {
	last_index := len(rf.Log) - 1
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: last_index,
		LastLogTerm:  rf.Log[last_index].Term,
	}
	reply := RequestVoteReply{}
	DPrintf("RAFT %d: sending out Request Vote at term %d \n", rf.me, rf.CurrentTerm)

	go rf.SendRequestVote(server, args, reply)

}

func (rf *Raft) SendoutRPCs(whoami int) {
	switch whoami {
	case leader:
		// send out AppendEntries to followers
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.CreateAndSendAppendEntries(i)
			}
		}
	case candidate:
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.CreateAndSendRequestVote(i)
			}
		}
	default:
		fmt.Println("error.. unknown server role")
	}
}

func (rf *Raft) LeaderDuty() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.whoami == leader {
			rf.SendoutRPCs(leader)
		}
		rf.mu.Unlock()
		time.Sleep(120 * time.Millisecond)
	}
}

// wait until one of the three senario happens
// 1. this candidate wins the election
// 2. another leader establish leadership (heartbeat)
// 3. a tie (multiple candidates sprung up)
func (rf *Raft) StartElection(term int) {
	requestVoteTimeout := false
	go func() {
		time.Sleep(800 * time.Millisecond)
		rf.mu.Lock()
		requestVoteTimeout = true
		rf.mu.Unlock()
	}()

	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.whoami != candidate || requestVoteTimeout || rf.CurrentTerm > term || rf.votes > rf.majority {
			if rf.whoami == candidate && rf.CurrentTerm == term && rf.votes > rf.majority {
				rf.whoami = leader
				rf.VotedFor = -1
				rf.nextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.nextIndex); i++ {
					rf.nextIndex[i] = len(rf.Log)
				}
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.matchIndex); i++ {
					rf.matchIndex[i] = 0
				}
				DPrintf("RAFT %d, Win Election: New Leader for term %d \n", rf.me, rf.CurrentTerm)
			} else {
				DPrintf("RAFT %d: Lost Election with term %d because no longer candidate ? %v update term ? %v Time out ? %v enough votes ? %v \n",
					rf.me,
					rf.CurrentTerm,
					rf.whoami != candidate,
					rf.CurrentTerm != term,
					requestVoteTimeout,
					rf.votes > rf.majority)
				rf.whoami = follower
				rf.lastReceive = time.Now()
				rf.VotedFor = -1

			}
			rf.persist()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) ElectionTimer() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.whoami != leader {
			timeout := GetTimeout(800, 1800)
			rf.mu.Unlock()
			time.Sleep(timeout)
			rf.mu.Lock()
			if rf.whoami == follower && time.Since(rf.lastReceive) > timeout {
				rf.whoami = candidate
				rf.CurrentTerm++
				rf.VotedFor = rf.me
				rf.votes = 1
				rf.lastReceive = time.Now()
				rf.persist()
				rf.SendoutRPCs(candidate)
				go rf.StartElection(rf.CurrentTerm)
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ApplyMessage() {
	for !rf.killed() {
		rf.mu.Lock()
		//rf.cond.Wait()
		for rf.commitIndex > 0 && rf.lastApplied <= rf.commitIndex {
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.Log[rf.lastApplied].Command,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied++
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.whoami = follower
	rf.VotedFor = -1
	rf.CurrentTerm = 1
	rf.votes = 0
	rf.majority = len(peers) / 2
	rf.commitIndex = 0
	rf.lastApplied = 1
	rf.Log = make([]Entry, 1)
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.readPersist(persister.ReadRaftState())
	DPrintf("RAFT %d: started with term %d \n", rf.me, rf.CurrentTerm)

	go rf.ApplyMessage()
	go rf.ElectionTimer()
	go rf.LeaderDuty()

	return rf
}
