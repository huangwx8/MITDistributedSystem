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
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	RAFT_STATE_FOLLOWER  = "FOLLOWER"
	RAFT_STATE_CANDIDATE = "CANDIDATE"
	RAFT_STATE_LEADER    = "LEADER"
)

const HEARTBEAT_INTERVAL_MS = 100
const COMMIT_INTERVAL_MS = 10
const HEARTBEAT_TIMEOUT_MS = 1000
const COUNT_VOTES_INTERVAL_MS = 5
const ELECTION_TIMEOUT_MS = 500

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	// Non-volatile
	currentTerm   int
	votedFor      int
	log           []LogEntry
	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int

	// Volatile
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	visitMarks  []bool // used to restrict frequency of heartbeats

	state                            string
	lastReceivedHeartbeatTimestampMs int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == RAFT_STATE_LEADER

	return term, isleader
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		DPrintf("readPersist err")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex

		DPrintf("Node[%d][%s]: readPersist snapshotIndex=[%d] snapshotTerm=[%d] commitIndex=[%d] lastApplied=[%d]",
			rf.me, rf.state, rf.snapshotIndex, rf.snapshotTerm, rf.commitIndex, rf.lastApplied)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

	index = index - 1

	pIndex := rf.l2p(index)

	lastIncludedLogEntry := rf.log[pIndex]

	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = lastIncludedLogEntry.Term

	rf.log = rf.log[pIndex+1:]

	DPrintf("Node[%d][%s]: Snapshot snapshotIndex=[%d] snapshotTerm=[%d]", rf.me, rf.state, rf.snapshotIndex, rf.snapshotTerm)

	rf.persist()
}

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	if args.Term < rf.currentTerm {
		DPrintf("Node[%d][%s]: RequestVote FAILURE candidate=[%d] Term=[%d] LowerCandidateTerm: (currentTerm=[%d])",
			rf.me, rf.state, args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		rf.receiveTerm(args.Term, false)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastIndex, lastTerm := rf.getLastLogIndexAndTerm()
		upToDate := false
		if args.LastLogTerm == lastTerm {
			upToDate = args.LastLogIndex >= lastIndex
		} else {
			upToDate = args.LastLogTerm > lastTerm
		}
		if upToDate {
			rf.lastReceivedHeartbeatTimestampMs = time.Now().UnixMilli() // candidate is alive
			DPrintf("Node[%d][%s]: RequestVote SUCCESS candidate=[%d] Term=[%d]", rf.me, rf.state, args.CandidateId, args.Term)
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
			reply.Term = args.Term
			return
		} else {
			DPrintf("Node[%d][%s]: RequestVote FAILURE candidate=[%d] Term=[%d] NotUpToDate: (lastTerm[%d] candidateLastTerm[%d] lastIndex[%d] candidateLastIndex[%d])",
				rf.me, rf.state, args.CandidateId, args.Term, lastTerm, args.LastLogTerm, lastIndex, args.LastLogIndex)
			reply.VoteGranted = false
			reply.Term = args.Term
			return
		}
	} else {
		DPrintf("Node[%d][%s]: RequestVote FAILURE candidate=[%d] Term=[%d] AlreadyVoted: (votedFor=[%d])",
			rf.me, rf.state, args.CandidateId, args.Term, rf.votedFor)
		reply.VoteGranted = false
		reply.Term = args.Term
		return
	}
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                   int
	Success                bool
	PrevLogIndexSuggestion int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	if rf.state != RAFT_STATE_FOLLOWER {
		return
	}

	if args.Term < rf.currentTerm {
		DPrintf("Node[%d][%s]: AppendEntries FAILURE LeaderId=[%d] LowerLeaderTerm: (Term=[%d] < currentTerm=[%d])", rf.me, rf.state, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else {
		rf.receiveTerm(args.Term, true)
	}

	// Leader is alive
	rf.lastReceivedHeartbeatTimestampMs = time.Now().UnixMilli()

	logLength := rf.p2l(len(rf.log))
	pPrevLogIndex := rf.l2p(args.PrevLogIndex)

	// compare my log with Prev
	if pPrevLogIndex >= 0 {
		if args.PrevLogIndex >= logLength {
			DPrintf("Node[%d][%s]: AppendEntries FAILURE LeaderId=[%d] Term=[%d] OutOfBound: (PrevLogIndex=[%d] >= #log=[%d])",
				rf.me, rf.state, args.LeaderId, args.Term, args.PrevLogIndex, logLength)
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.PrevLogIndexSuggestion = logLength - 1
			return
		} else if args.PrevLogTerm != rf.log[pPrevLogIndex].Term {
			DPrintf("Node[%d][%s]: AppendEntries FAILURE LeaderId=[%d] Term=[%d] TermMismatch: (PrevLogTerm=[%d] != log[PrevLogIndex].Term=[%d])",
				rf.me, rf.state, args.LeaderId, args.Term, args.PrevLogTerm, rf.log[pPrevLogIndex].Term)
			reply.Success = false
			reply.Term = rf.currentTerm
			termToSkip := rf.log[pPrevLogIndex].Term
			index := pPrevLogIndex - 1
			for ; index >= 0; index-- {
				if rf.log[index].Term == termToSkip {
					continue
				} else {
					break
				}
			}
			reply.PrevLogIndexSuggestion = rf.p2l(index)
			return
		}
	} else {
		// Prev is behind snapshot, skip validation
	}

	indexEntries := 0

	for ; indexEntries < len(args.Entries); indexEntries++ {
		indexLog := pPrevLogIndex + 1 + indexEntries
		if indexLog < 0 {
			continue
		} else if indexLog >= len(rf.log) {
			break
		} else if rf.log[indexLog].Term != args.Entries[indexEntries].Term {
			// truncate
			rf.log = rf.log[:indexLog]
			break
		}
	}

	if indexEntries < len(args.Entries) {
		// overwrite
		for ; indexEntries < len(args.Entries); indexEntries++ {
			indexLog := pPrevLogIndex + 1 + indexEntries
			if indexLog >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[indexEntries])
			} else {
				rf.log[indexLog] = args.Entries[indexEntries]
			}
		}
	}

	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		n, _ := rf.getLastLogIndexAndTerm()
		if args.LeaderCommit < n {
			rf.doCommit(args.LeaderCommit)
		} else {
			rf.doCommit(n)
		}
	}

	lastIndex, lastTerm := rf.getLastLogIndexAndTerm()

	DPrintf("Node[%d][%s]: AppendEntries SUCCESS LeaderId=[%d] Term=[%d] lastIndex=[%d] lastTerm=[%d] commitIndex=[%d]",
		rf.me, rf.state, args.LeaderId, args.Term, lastIndex, lastTerm, rf.commitIndex)
	reply.Success = true
	reply.Term = args.Term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	if rf.state != RAFT_STATE_FOLLOWER {
		return
	}

	if args.Term < rf.currentTerm {
		DPrintf("Node[%d][%s]: InstallSnapshot FAILURE LeaderId=[%d] LowerLeaderTerm: (Term=[%d] < currentTerm=[%d])", rf.me, rf.state, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	} else {
		rf.receiveTerm(args.Term, true)
	}

	// Leader is alive
	rf.lastReceivedHeartbeatTimestampMs = time.Now().UnixMilli()

	if args.LastIncludedIndex <= rf.snapshotIndex {
		DPrintf("Node[%d][%s]: InstallSnapshot FAILURE LeaderId=[%d] LowerSnapshotIndex: (LastIncludedIndex=[%d] <= snapshotIndex=[%d])", rf.me, rf.state, args.LeaderId, args.LastIncludedIndex, rf.snapshotIndex)
		reply.Term = rf.currentTerm
		return
	}

	lastIndex, lastTerm := rf.getLastLogIndexAndTerm()

	if args.LastIncludedIndex > lastIndex {
		// discard entire log
		rf.log = make([]LogEntry, 0)
	} else {
		pLastIncludedIndex := rf.l2p(args.LastIncludedIndex)
		if rf.log[pLastIncludedIndex].Term == args.LastIncludedTerm {
			// retain following entries
			rf.log = rf.log[pLastIncludedIndex+1:]
		} else {
			// discard entire log
			rf.log = make([]LogEntry, 0)
		}
	}

	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm

	msg := ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotIndex: rf.snapshotIndex + 1, SnapshotTerm: rf.snapshotTerm}
	rf.applyCh <- msg

	if rf.commitIndex < rf.snapshotIndex {
		rf.commitIndex = rf.snapshotIndex
	}

	if rf.lastApplied < rf.snapshotIndex {
		rf.lastApplied = rf.snapshotIndex
	}

	rf.persist()

	lastIndex, lastTerm = rf.getLastLogIndexAndTerm()

	DPrintf("Node[%d][%s]: InstallSnapshot SUCCESS LeaderId=[%d] Term=[%d] snapshotIndex=[%d] snapshotTerm=[%d] lastIndex=[%d] lastTerm=[%d]",
		rf.me, rf.state, args.LeaderId, args.Term, rf.snapshotIndex, rf.snapshotTerm, lastIndex, lastTerm)
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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

	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.state != RAFT_STATE_LEADER {
		isLeader = false
	} else {
		_, lastLogTerm := rf.getLastLogIndexAndTerm()

		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		index = rf.p2l(len(rf.log) - 1)
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		isLeader = true

		rf.persist()

		DPrintf("Node[%d][%s]: Start term=[%d] index=[%d]", rf.me, rf.state, term, index)

		if term < lastLogTerm {
			panic("Decreasing log term")
		}

		// trigger a broadcast immediately
		rf.broadcastAppendEntries(false)
	}

	return index + 1, term, isLeader
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

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		if (rf.state == RAFT_STATE_FOLLOWER) && ((time.Now().UnixMilli() - rf.lastReceivedHeartbeatTimestampMs) > HEARTBEAT_TIMEOUT_MS) {

			rf.state = RAFT_STATE_CANDIDATE
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()

			if rf.state == RAFT_STATE_CANDIDATE {
				DPrintf("Node[%d][%s]: Start election for term[%d]", rf.me, rf.state, rf.currentTerm)

				n := len(rf.peers)                               // number of nodes
				targetVotes := n/2 + 1                           // votes required
				numVotes := 1                                    // votes collected
				numReplies := 1                                  // replies collected
				wonElection := false                             // has I won this election
				startElectionTimestamp := time.Now().UnixMilli() // used to detect election timeout

				args := RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex, args.LastLogTerm = rf.getLastLogIndexAndTerm()

				for i, _ := range rf.peers {
					if i != rf.me {
						go func(dest int) {
							reply := RequestVoteReply{}
							if rf.sendRequestVote(dest, &args, &reply) {
								if rf.killed() {
									return
								}
								rf.mu.Lock()
								DPrintf("Node[%d][%s]: Received RequestVoteReply for term[%d] VoteGranted=[%v]", rf.me, rf.state, reply.Term, reply.VoteGranted)
								if (time.Now().UnixMilli()-startElectionTimestamp) < ELECTION_TIMEOUT_MS &&
									rf.state == RAFT_STATE_CANDIDATE &&
									rf.currentTerm == reply.Term &&
									reply.VoteGranted {
									numVotes++
									numReplies++
								} else {
									numReplies++
								}
								rf.mu.Unlock()
							} else {
								if rf.killed() {
									return
								}
								rf.mu.Lock()
								numReplies++
								rf.mu.Unlock()
							}
						}(i)
					}
				}

				// Every node replied, or timeout
				for (numReplies < n) && ((time.Now().UnixMilli() - startElectionTimestamp) < ELECTION_TIMEOUT_MS) {
					rf.mu.Unlock()
					time.Sleep(COUNT_VOTES_INTERVAL_MS * time.Millisecond)

					if rf.killed() {
						return
					}

					rf.mu.Lock()

					// state could have changed while sleeping
					if rf.state != RAFT_STATE_CANDIDATE {
						break
					}

					// successful
					if numVotes >= targetVotes {
						wonElection = true
						break
					}
				}

				if wonElection {
					DPrintf("Node[%d][%s]: Won election for term [%d]", rf.me, rf.state, rf.currentTerm)
					rf.toLeader()
				} else {
					DPrintf("Node[%d][%s]: Lost election for term [%d] numReplies=[%d] numVotes=[%d]",
						rf.me, rf.state, rf.currentTerm, numReplies, numVotes)
					rf.toFollower()
				}
			}
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) tickBroadcastHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.broadcastAppendEntries(true)
		rf.mu.Unlock()
		time.Sleep(HEARTBEAT_INTERVAL_MS * time.Millisecond)
	}
}

func (rf *Raft) tickCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.commitMatchedEntries()
		rf.mu.Unlock()
		time.Sleep(COMMIT_INTERVAL_MS * time.Millisecond)
	}
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	if rf.state == RAFT_STATE_LEADER {
		for i, _ := range rf.peers {
			if i != rf.me {
				if isHeartbeat {
					rf.visitMarks[i] = true
				} else {
					if rf.visitMarks[i] {
						rf.visitMarks[i] = false // visited recently, skip for this time
						return
					}
				}

				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				pPrevLogIndex := rf.l2p(args.PrevLogIndex)
				if pPrevLogIndex >= 0 {
					args.PrevLogTerm = rf.log[pPrevLogIndex].Term
				} else if pPrevLogIndex == -1 {
					args.PrevLogTerm = -1
				} else {
					// lagged behind snapshot, should installSnapshot
					installArgs := InstallSnapshotArgs{}
					installArgs.LeaderId = rf.me
					installArgs.Term = rf.currentTerm
					installArgs.Data = rf.snapshot
					installArgs.LastIncludedIndex = rf.snapshotIndex
					installArgs.LastIncludedTerm = rf.snapshotTerm
					go func(dest int) {
						installReply := InstallSnapshotReply{}
						if rf.sendInstallSnapshot(dest, &installArgs, &installReply) {
							rf.receiveInstallSnapshotReply(dest, &installArgs, &installReply)
						}
					}(i)
					continue
				}
				n := len(rf.log) - pPrevLogIndex - 1
				args.Entries = make([]LogEntry, n)
				for j := 0; j < n; j++ {
					args.Entries[j] = rf.log[pPrevLogIndex+j+1]
				}
				args.LeaderCommit = rf.commitIndex

				go func(dest int) {
					reply := AppendEntriesReply{}
					if rf.sendAppendEntries(dest, &args, &reply) {
						rf.receiveAppendEntriesReply(dest, &args, &reply)
					}
				}(i)
			}
		}
	}
}

func (rf *Raft) commitMatchedEntries() {
	if rf.state == RAFT_STATE_LEADER {
		commited := false
		lastIndex, _ := rf.getLastLogIndexAndTerm()

		for i := rf.commitIndex + 1; i <= lastIndex; i++ {
			pIndex := rf.l2p(i)
			if rf.log[pIndex].Term == rf.currentTerm {
				matchCount := 0
				n := len(rf.peers)
				for j := 0; j < n; j++ {
					if rf.matchIndex[j] >= i {
						matchCount++
					}
				}
				if matchCount >= (n/2 + 1) {
					rf.doCommit(i)
					commited = true
				}
			}
		}

		if commited {
			rf.broadcastAppendEntries(false)
		}
	}
}

func (rf *Raft) doCommit(logIndex int) {
	if logIndex < rf.commitIndex {
		panic("Decreasing commitIndex")
	}

	DPrintf("Node[%d][%s]: doCommit logIndex=[%d]", rf.me, rf.state, logIndex)

	rf.commitIndex = logIndex

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.l2p(i)].Command, CommandIndex: i + 1}
		rf.applyCh <- msg
	}

	rf.lastApplied = rf.commitIndex
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

	// Your initialization code here (3A, 3B, 3C).

	rf.applyCh = applyCh

	n := len(rf.peers)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	rf.visitMarks = make([]bool, n)

	for i := 0; i < n; i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = -1
		rf.visitMarks[i] = false
	}

	rf.snapshot = nil
	rf.snapshotIndex = -1
	rf.snapshotTerm = 0

	rf.toFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// leader's routines
	go rf.tickBroadcastHeartbeat()
	go rf.tickCommit()

	return rf
}

func (rf *Raft) toLeader() {
	DPrintf("Node[%d][%s]: toLeader currentTerm=[%d]", rf.me, rf.state, rf.currentTerm)

	rf.state = RAFT_STATE_LEADER
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	for i := 0; i < n; i++ {
		lastIndex, _ := rf.getLastLogIndexAndTerm()
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = -1
		rf.visitMarks[i] = false
	}
}

func (rf *Raft) toFollower() {
	DPrintf("Node[%d][%s]: toFollower currentTerm=[%d]", rf.me, rf.state, rf.currentTerm)

	rf.state = RAFT_STATE_FOLLOWER
	rf.lastReceivedHeartbeatTimestampMs = time.Now().UnixMilli()
}

func (rf *Raft) receiveTerm(term int, isLeader bool) {
	if rf.state == RAFT_STATE_LEADER && term == rf.currentTerm && isLeader {
		panic("Non-unique leader")
	}

	foundHigherTerm := term > rf.currentTerm
	foundNewLeader := term >= rf.currentTerm && isLeader

	if foundHigherTerm {
		DPrintf("Node[%d][%s]: Update term[%d]", rf.me, rf.state, term)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}

	if (rf.state == RAFT_STATE_LEADER || rf.state == RAFT_STATE_CANDIDATE) && (foundHigherTerm || foundNewLeader) {
		rf.toFollower()
	}
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	n := len(rf.log)

	if n > 0 {
		return rf.p2l(n - 1), rf.log[n-1].Term
	}

	return rf.snapshotIndex, rf.snapshotTerm
}

func (rf *Raft) receiveAppendEntriesReply(followerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RAFT_STATE_LEADER {
		return
	}

	if reply.Term < rf.currentTerm {
		return // No need to process reply from a previous term
	} else if reply.Term > rf.currentTerm {
		rf.receiveTerm(reply.Term, false)
	} else {
		if reply.Success {
			rf.matchIndex[followerId] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[followerId] = rf.matchIndex[followerId] + 1
		} else {
			if reply.PrevLogIndexSuggestion >= 0 {
				rf.nextIndex[followerId] = reply.PrevLogIndexSuggestion
			} else {
				rf.nextIndex[followerId] = 0
			}
			rf.broadcastAppendEntries(false) // resend immediately
		}
	}
}

func (rf *Raft) receiveInstallSnapshotReply(followerId int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RAFT_STATE_LEADER {
		return
	}

	if reply.Term < rf.currentTerm {
		return // No need to process reply from a previous term
	} else if reply.Term > rf.currentTerm {
		rf.receiveTerm(reply.Term, false)
	} else {
		rf.matchIndex[followerId] = args.LastIncludedIndex
		rf.nextIndex[followerId] = args.LastIncludedIndex + 1
	}
}

// LogicalIndex to PhysicalIndex
func (rf *Raft) l2p(logicalIndex int) int {
	return logicalIndex - rf.snapshotIndex - 1
}

// PhysicalIndex to LogicalIndex
func (rf *Raft) p2l(physicalIndex int) int {
	return rf.snapshotIndex + 1 + physicalIndex
}
