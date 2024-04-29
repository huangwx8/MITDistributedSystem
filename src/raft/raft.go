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
	Term  int
	Op    string
	Param string
}

const (
	RAFT_STATE_FOLLOWER  = 1
	RAFT_STATE_CANDIDATE = 2
	RAFT_STATE_LEADER    = 3
)

const HEARTBEAT_INTERVAL_MS = 10
const HEARTBEAT_TIMEOUT_MS = 1000
const COUNT_VOTES_INTERVAL_MS = 5
const ELECTION_TIMEOUT_MS = 100

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

	// Non-volatile
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile
	commitIndex int64
	lastApplied int64
	nextIndex   []int64
	matchIndex  []int64

	state                    int
	lastHeartbeatTimestampMs int64
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	Term         int
	CandidateId  int
	LastLogIndex int64
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

	DPrintf("Node[%d]: RequestVote candidate=[%d] Term=[%d]", rf.me, args.CandidateId, args.Term)

	if args.Term < rf.currentTerm {
		DPrintf("Node[%d]: Deny RequestVote candidate=[%d] Term=[%d] currentTerm=[%d]", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.receiveHigherTerm(args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if true /*election restriction*/ {
			// Candidate is alive
			rf.lastHeartbeatTimestampMs = time.Now().UnixMilli()

			DPrintf("Node[%d]: Grant RequestVote candidate=[%d] Term=[%d]", rf.me, args.CandidateId, args.Term)
			reply.VoteGranted = true
			reply.Term = args.Term
			return
		}
	}

	DPrintf("Node[%d]: Deny RequestVote candidate=[%d] Term=[%d] votedFor=[%d]", rf.me, args.CandidateId, args.Term, rf.votedFor)
	reply.VoteGranted = false
	reply.Term = args.Term
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
	DPrintf("Node[%d]: sendRequestVote to [%d]", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

	DPrintf("Node[%d]: AppendEntries LeaderId=[%d] Term=[%d]", rf.me, args.LeaderId, args.Term)

	if args.Term < rf.currentTerm {
		DPrintf("Node[%d]: Deny AppendEntries LeaderId=[%d] Term=[%d] currentTerm=[%d]", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.receiveHigherTerm(args.Term)
		if rf.state != RAFT_STATE_FOLLOWER {
			rf.toFollower()
		}
	}

	// Leader is alive
	rf.lastHeartbeatTimestampMs = time.Now().UnixMilli()

	DPrintf("Node[%d]: AppendEntries succeed LeaderId=[%d] Term=[%d]", rf.me, args.LeaderId, args.Term)
	reply.Success = true
	reply.Term = args.Term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Node[%d]: sendAppendEntries to [%d]", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		if (rf.state == RAFT_STATE_FOLLOWER) && ((time.Now().UnixMilli() - rf.lastHeartbeatTimestampMs) > HEARTBEAT_TIMEOUT_MS) {

			rf.state = RAFT_STATE_CANDIDATE
			rf.currentTerm++
			rf.votedFor = rf.me

			if rf.state == RAFT_STATE_CANDIDATE {
				DPrintf("Node[%d]: StartElection", rf.me)

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
								rf.mu.Lock()
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
					DPrintf("Node[%d]: become leader of term [%d]", rf.me, rf.currentTerm)
					rf.state = RAFT_STATE_LEADER
				} else {
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

func (rf *Raft) tickLeader() {
	for !rf.killed() {
		rf.mu.Lock()

		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = -1
		args.PrevLogTerm = -1
		args.Entries = make([]LogEntry, 0)
		args.LeaderCommit = rf.commitIndex

		if rf.state == RAFT_STATE_LEADER {
			for i, _ := range rf.peers {
				if i != rf.me {
					go func(dest int) {
						reply := AppendEntriesReply{}
						if rf.sendAppendEntries(dest, &args, &reply) {
							rf.receiveAppendEntriesReply(&reply)
						}
					}(i)
				}
			}
		}

		rf.mu.Unlock()

		time.Sleep(HEARTBEAT_INTERVAL_MS * time.Millisecond)
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

	// Your initialization code here (3A, 3B, 3C).

	n := len(rf.peers)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int64, n)
	rf.matchIndex = make([]int64, n)

	rf.toFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// leader's routine
	go rf.tickLeader()

	return rf
}

func (rf *Raft) toFollower() {
	DPrintf("Node[%d]: toFollower", rf.me)

	rf.state = RAFT_STATE_FOLLOWER

	// Reset timer
	rf.lastHeartbeatTimestampMs = time.Now().UnixMilli()
}

func (rf *Raft) getLastLogIndexAndTerm() (int64, int) {
	n := len(rf.log)

	if n > 0 {
		return int64(n - 1), rf.log[n-1].Term
	}

	return -1, -1
}

func (rf *Raft) receiveAppendEntriesReply(reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RAFT_STATE_LEADER {
		return
	}

	if rf.currentTerm < reply.Term {
		// A new leader has been elected, we should step down
		rf.toFollower()
		return
	} else if rf.currentTerm > reply.Term {
		// During reply, this node has won another election
		return
	}

	if !reply.Success {
		// resend
	}
}

func (rf *Raft) receiveHigherTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
}
