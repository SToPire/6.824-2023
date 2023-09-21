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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

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

type LogEntry struct {
	Term int
	Cmd  interface{}
}

const (
	RAFT_FOLLOWER  = 0
	RAFT_CANDIDATE = 1
	RAFT_LEADER    = 2
)

const AppendEntriesInterval = 50 * time.Millisecond
const ApplyBufferCapacity = 1000

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm      int        // latest term this server has seen
	votedFor         int        // candidateId that received vote in current term
	log              []LogEntry // log entries of this server
	logBase          int        // index of the first log entry
	lastIncludedTerm int        // term of lastIncludedIndex

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex  []int // index of the next log entry to send to each server
	matchIndex []int // index of highest log entry known to be replicated on each server

	// for leader election
	votesFrom []bool // whether this server has voted for this server
	votes     int    // number of votes received

	// for heartbeat
	electionTimeout time.Duration
	lastHeartbeat   time.Time // last time this server received a heartbeat

	applyCh     chan ApplyMsg // channel for sending ApplyMsg to service
	applyBuffer chan ApplyMsg // need a buffer as applyCh's capacity is unknown
	state       int

	commitCV *sync.Cond
	applyCV  *sync.Cond

	snapshot []byte // current snapshot
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == RAFT_LEADER

	return term, isleader
}

func (rf *Raft) HaveLogInCurTerm() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.getLastLogTerm() == rf.currentTerm
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.logBase)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var logBase int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&logBase) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("server %d readPersist error", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.logBase = logBase
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	newLogBase := index + 1

	/* No log entry to be trimmed */
	if newLogBase <= rf.logBase || len(rf.log) == 0 {
		return
	}

	if newLogBase > rf.getLastLogIndex() {
		newLogBase = rf.getLastLogIndex() + 1
		rf.lastIncludedTerm = rf.log[len(rf.log)-1].Term
		rf.log = nil
	} else {
		rf.lastIncludedTerm = rf.log[newLogBase-rf.logBase-1].Term
		rf.log = rf.log[newLogBase-rf.logBase:]
	}
	rf.logBase = newLogBase

	rf.snapshot = snapshot
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current term for candidate to update itself
	VoteGranted bool // whether candidate received vote
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logBase + len(rf.log) - 1
}

/* § 5.4.1 Election restriction
 * only grant vote if candidate's log is at least as up-to-date as receiver's log
 */
func (rf *Raft) candidateIsNewer(args *RequestVoteArgs) bool {
	return args.LastLogTerm > rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	needPersist := false

	if args.Term < rf.currentTerm {
		/* req from outdated candidate */
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		goto out
	} else if args.Term > rf.currentTerm {
		/* req from newer candidate */
		/* votedFor should be cleared as it's a new term */
		rf.votedFor = -1
		rf.currentTerm = args.Term
		needPersist = true

		/* convert to follower */
		if rf.state != RAFT_FOLLOWER {
			rf.state = RAFT_FOLLOWER
			DPrintf("[Server %v] become follower after receiving RequestVote from %v", rf.me, args.CandidateId)
		}
	}

	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.candidateIsNewer(args) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		needPersist = true
		/* reset election timer after voting */
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
	}

out:
	if needPersist {
		rf.persist()
	}
	DPrintf("[RequestVoteReply] %v => %v, granted: %v", rf.me, args.CandidateId, reply.VoteGranted)
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader's id for client to redirect
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term     int  // current term for leader to update itself
	Success  bool // whether follower contained entry matching PrevLogIndex and PrevLogTerm
	Conflict struct {
		XTerm     int  // term of conflicting entry
		XIndex    int  // index of first entry with that term
		XLen      int  // length of log
		XSnapshot bool // please send a snapshot to me
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeartbeat = time.Now()

	needPersist := false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		goto out
	} else if args.Term == rf.currentTerm {
		/* § 5.2: If the leader’s term (included in its RPC) is
		 * at least as large as the candidate’s current term,
		 * then the candidate recognizes the leader as legitimate and returns to follower state.
		 */
		if rf.state == RAFT_CANDIDATE {
			DPrintf("[Server %v] become follower after receiving AppendEntries from %v", rf.me, args.LeaderId)
			rf.state = RAFT_FOLLOWER
			rf.votedFor = -1
			needPersist = true
		}
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		needPersist = true
		/* converts to follower */
		if rf.state != RAFT_FOLLOWER {
			DPrintf("[Server %v] become follower after receiving AppendEntries from %v", rf.me, args.LeaderId)
			rf.state = RAFT_FOLLOWER
		}
	}

	if args.PrevLogIndex <= rf.logBase-1 {
		if !(args.PrevLogIndex == rf.logBase-1 && args.PrevLogTerm == rf.lastIncludedTerm) {
			/* log doesn't contain an entry at PrevLogIndex, ask for a snapshot */
			reply.Term = rf.currentTerm
			reply.Success = false

			reply.Conflict.XSnapshot = true

			goto out
		}
	} else if rf.getLastLogIndex() < args.PrevLogIndex {
		/* log is too short */
		reply.Term = rf.currentTerm
		reply.Success = false

		reply.Conflict.XTerm = -1
		reply.Conflict.XIndex = -1
		reply.Conflict.XLen = rf.logBase + len(rf.log)
		reply.Conflict.XSnapshot = false

		goto out
	} else if rf.log[args.PrevLogIndex-rf.logBase].Term != args.PrevLogTerm {
		/* log doesn't contain an entry at PrevLogIndex whose term matches PrevLogTerm */
		reply.Term = rf.currentTerm
		reply.Success = false

		reply.Conflict.XTerm = rf.log[args.PrevLogIndex-rf.logBase].Term
		reply.Conflict.XIndex = args.PrevLogIndex
		reply.Conflict.XLen = rf.logBase + len(rf.log)
		reply.Conflict.XSnapshot = false

		/* find the first entry with term XTerm */
		for i := args.PrevLogIndex - 1; i >= rf.logBase; i-- {
			if rf.log[i-rf.logBase].Term != reply.Conflict.XTerm {
				reply.Conflict.XIndex = i + 1
				break
			}
			/* we have reached the beginning of log */
			if i == rf.logBase {
				reply.Conflict.XIndex = rf.logBase
			}
		}

		goto out
	}

	/* append entries */
	if args.Entries != nil {
		for i := 0; i < len(args.Entries); i++ {
			index := args.PrevLogIndex - rf.logBase + i + 1
			needPersist = true
			if index < len(rf.log) {
				if rf.log[index].Term != args.Entries[i].Term {
					/* remove any conlicting entries */
					rf.log = rf.log[:index]
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}
			} else {
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
	}

	/* update commitIndex */
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		newCommitIndex := lastNewEntryIndex
		if args.LeaderCommit < lastNewEntryIndex {
			newCommitIndex = args.LeaderCommit
		}

		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCV.Signal()
			DPrintf("[Server %v] update commitIndex to %v", rf.me, rf.commitIndex)
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true

out:
	if needPersist {
		rf.persist()
	}
	DPrintf("[AppendEntriesReply] %v => %v, success: %v", rf.me, args.LeaderId, reply.Success)
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // leader's id for client to redirect
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // current term for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var msg ApplyMsg
	needPersist := false

	/* reset election timeout as AppendEntries */
	rf.lastHeartbeat = time.Now()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		needPersist = true
		/* converts to follower */
		if rf.state != RAFT_FOLLOWER {
			DPrintf("[Server %v] become follower after receiving InstallSnapshot from %v", rf.me, args.LeaderId)
			rf.state = RAFT_FOLLOWER
		}
	}

	/* The snapshot received is a prefix of current log, omit it. */
	if args.LastIncludedIndex < rf.logBase {
		goto out
	}

	rf.snapshot = args.Data

	if args.LastIncludedIndex < rf.getLastLogIndex() &&
		rf.log[args.LastIncludedIndex-rf.logBase].Term == args.LastIncludedTerm {
		/* log contains an entry at LastIncludedIndex whose term matches LastIncludedTerm */
		rf.log = rf.log[args.LastIncludedIndex+1-rf.logBase:]
	} else {
		/* log is too short */
		rf.log = nil
	}
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.logBase = args.LastIncludedIndex + 1

	/* tell upper service to install snapshot */
	if rf.lastApplied < args.LastIncludedIndex {
		msg = ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.applyBuffer <- msg
	}

	needPersist = true

out:
	reply.Term = rf.currentTerm
	if needPersist {
		rf.persist()
	}
	DPrintf("[InstallSnapshotReply] %v => %v", rf.me, args.LeaderId)
}

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

/* send InstallSnapshot RPC to server,
 * caller must take rf.mu
 */
func (rf *Raft) doInstallSnapshot(server int) {
	req := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.logBase - 1,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()

	DPrintf("[InstallSnapshot] %v => %v, args=%v", rf.me, server, req)
	ok := rf.sendInstallSnapshot(server, req, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			DPrintf("[Server %v] become follower after receiving InstallSnapshotReply from %v", rf.me, server)
			rf.currentTerm = reply.Term
			rf.state = RAFT_FOLLOWER
			rf.votedFor = -1
			rf.persist()
			return
		}

		rf.nextIndex[server] = rf.logBase
		rf.matchIndex[server] = rf.logBase - 1

		rf.commitCV.Signal()
	}
}

/* send AppendEntries RPC to server,
 * caller must take rf.mu
 */
func (rf *Raft) doAppendEntries(server int, prevLogIndex int, lastLogIndex int) {
	req := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		Entries:      make([]LogEntry, lastLogIndex-prevLogIndex),
		LeaderCommit: rf.commitIndex,
	}
	if prevLogIndex == rf.logBase-1 {
		req.PrevLogTerm = rf.lastIncludedTerm
	} else {
		/* prevLogIndex > rf.logBase - 1 */
		req.PrevLogTerm = rf.log[prevLogIndex-rf.logBase].Term
	}
	copy(req.Entries, rf.log[prevLogIndex-rf.logBase+1:])
	reply := &AppendEntriesReply{}

	rf.mu.Unlock()

	DPrintf("[AppendEntries] %v => %v, args=%v", rf.me, server, req)
	ok := rf.sendAppendEntries(server, req, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			DPrintf("[Server %v] become follower after receiving AppendEntriesReply from %v", rf.me, server)
			rf.currentTerm = reply.Term
			rf.state = RAFT_FOLLOWER
			rf.votedFor = -1
			rf.persist()
			return
		}

		if reply.Success {
			/* If successful: update nextIndex and matchIndex for follower (§5.3) */
			rf.nextIndex[server] = lastLogIndex + 1
			rf.matchIndex[server] = lastLogIndex

			rf.commitCV.Signal()
		} else {
			/* decrement nextIndex and retry (§5.3) */
			var newNextIndex int
			if reply.Conflict.XSnapshot {
				/* follower asks for a snapshot */
				newNextIndex = rf.logBase
			} else if reply.Conflict.XTerm == -1 {
				/* follower's log is too short */
				newNextIndex = reply.Conflict.XLen
			} else {
				/* binsearch leader's log for XTerm */
				i := 0
				j := len(rf.log) - 1
				for i <= j {
					m := (i + j) / 2
					if rf.log[m].Term > reply.Conflict.XTerm {
						i = m + 1
					} else if rf.log[m].Term < reply.Conflict.XTerm {
						j = m - 1
					} else {
						i = m
						break
					}
				}
				if i >= len(rf.log) || rf.log[i].Term != reply.Conflict.XTerm {
					/* leader doesn't have XTerm */
					newNextIndex = reply.Conflict.XIndex
				} else {
					/* leader has XTerm,
					 * nextIndex = leader's last entry for XTerm
					 */
					newNextIndex = i
					for newNextIndex < len(rf.log) && rf.log[newNextIndex].Term == reply.Conflict.XTerm {
						newNextIndex++
					}
					newNextIndex--
					newNextIndex += rf.logBase
				}
			}

			rf.nextIndex[server] = newNextIndex
		}
	}
}

/* leader send its log entries to followers */
func (rf *Raft) sendNewEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			if rf.killed() {
				return
			}

			rf.mu.Lock()

			/* TOCTTOU check: early return if not a leader any more */
			if rf.state != RAFT_LEADER {
				rf.mu.Unlock()
				return
			}

			/* send AppendEntries RPC with log entries starting at nextIndex */
			prevLogIndex := rf.nextIndex[server] - 1
			lastLogIndex := rf.getLastLogIndex()

			if prevLogIndex < rf.logBase-1 || prevLogIndex > lastLogIndex {
				/* prevLogIndex is out of leader's log range, send snapshot */
				rf.doInstallSnapshot(server)
			} else {
				rf.doAppendEntries(server, prevLogIndex, lastLogIndex)
			}
		}(i)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RAFT_LEADER {
		isLeader = false
	} else {
		term = rf.currentTerm
		index = rf.logBase + len(rf.log)
		rf.log = append(rf.log, LogEntry{term, command})
		rf.persist()

		go rf.sendNewEntries()
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if time.Since(rf.lastHeartbeat) > rf.electionTimeout {
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) becomeLeader() {
	DPrintf("[Server %v] become leader", rf.me)
	rf.state = RAFT_LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.logBase + len(rf.log)
		rf.matchIndex[i] = 0
		// make sure first AppendEntries will be sent
	}
	rf.commitCV.Signal()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	/* reset election timeout */
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(450+rand.Int63()%100) * time.Millisecond

	/* become a candidate */
	rf.state = RAFT_CANDIDATE
	rf.currentTerm++

	/* vote for myself */
	rf.votedFor = rf.me
	for i := range rf.peers {
		rf.votesFrom[i] = false
	}
	rf.votesFrom[rf.me] = true
	rf.votes = 1

	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	rf.persist()
	DPrintf("[Server %v] start election, term=%v", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			if rf.killed() {
				return
			}

			reply := &RequestVoteReply{}

			DPrintf("[RequestVote] %v => %v, args=%v", rf.me, i, req)
			ok := rf.sendRequestVote(i, req, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if reply.VoteGranted {
				/* support from server i */
				if rf.state == RAFT_CANDIDATE {
					if !rf.votesFrom[i] {
						rf.votesFrom[i] = true
						rf.votes++
					}
					if rf.votes > len(rf.peers)/2 {
						rf.becomeLeader()
					}
				}
			} else if reply.Term > rf.currentTerm {
				/* server i is more up-to-date than me */
				DPrintf("[Server %v] become follower after receiving term %v RequestVoteReply from Server %v",
					rf.me, reply.Term, i)
				rf.state = RAFT_FOLLOWER
				rf.currentTerm = reply.Term

				rf.votedFor = -1
				rf.persist()
			}
			rf.mu.Unlock()
		}(i)
	}
}

/* leader's broadcasting routine */
func (rf *Raft) broadcast() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state == RAFT_LEADER {
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()
			rf.sendNewEntries()
		} else {
			rf.mu.Unlock()
		}
		/* No more than 10 times per second */
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {
		if rf.state == RAFT_LEADER {
			/* If there exists an N such that N > commitIndex,
			 * a majority of matchIndex[i] ≥ N and log[N].term == currentTerm:
			 * set commitIndex = N (§5.3, §5.4).
			 */
			for i := rf.commitIndex + 1; i < rf.logBase+len(rf.log); i++ {
				/* skip log entries not in current log as they have been snapshoted */
				if i < rf.logBase {
					rf.commitIndex = i
					rf.applyCV.Signal()
					continue
				}

				if rf.log[i-rf.logBase].Term == rf.currentTerm {
					count := 1
					for j := range rf.peers {
						if j == rf.me {
							continue
						}
						if rf.matchIndex[j] >= i {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						DPrintf("[Server %v] update commitIndex to %v", rf.me, i)
						rf.commitIndex = i
						rf.applyCV.Signal()
					} else {
						break
					}
				}
			}
		}

		rf.commitCV.Wait()
	}
}

func (rf *Raft) applyToBuf() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			/* skip log entries not in current log as they have been snapshoted */
			if i < rf.logBase {
				rf.lastApplied = i
				continue
			}

			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.logBase].Cmd,
				CommandIndex: i,
			}
			rf.applyBuffer <- msg
			rf.lastApplied = i

			/* applyBuffer is full, go sleep */
			if cap(rf.applyBuffer) == len(rf.applyBuffer) {
				break
			}
		}
		rf.applyCV.Wait()
	}
}

/* consume the applyBuffer */
func (rf *Raft) applyToService() {
	for e := range rf.applyBuffer {
		if rf.killed() {
			return
		}
		rf.applyCh <- e
		/* notice applyToBuf() to continue */
		rf.applyCV.Signal()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.logBase = 0
	rf.lastIncludedTerm = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.votesFrom = make([]bool, len(peers))
	for i := range rf.votesFrom {
		rf.votesFrom[i] = false
	}
	rf.votes = 0

	rf.electionTimeout = time.Duration(450+rand.Int63()%100) * time.Millisecond
	rf.lastHeartbeat = time.Now()

	rf.state = RAFT_FOLLOWER
	rf.applyCh = applyCh
	rf.applyBuffer = make(chan ApplyMsg, ApplyBufferCapacity)

	rf.commitCV = sync.NewCond(&rf.mu)
	rf.applyCV = sync.NewCond(&rf.mu)

	rf.snapshot = persister.ReadSnapshot()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.broadcast()
	go rf.commit()
	go rf.applyToBuf()
	go rf.applyToService()

	DPrintf("[Server %v] created\n", me)
	return rf
}
