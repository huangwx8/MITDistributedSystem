package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OP_TYPE_GET    = 1
	OP_TYPE_PUT    = 2
	OP_TYPE_APPEND = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType         int
	Key            string
	Value          string
	ClerkId        int64
	SequenceNumber int
}

type OpResult struct {
	valid bool
	err   Err
	value string
	term  int
}

type ClerkWriteHistory struct {
	recentSequenceNumber int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	db                   map[string]string
	opResults            map[int]OpResult
	clerkWriteHistoryMap map[int64]*ClerkWriteHistory
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if kv.killed() {
		DPrintf("KV[%d] [%d]Get[%s] killed", kv.me, args.ClerkId, args.Key)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	op := Op{OpType: OP_TYPE_GET, Key: args.Key, ClerkId: args.ClerkId}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("KV[%d] [%d]Get[%s] NOT Leader", kv.me, args.ClerkId, args.Key)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		DPrintf("KV[%d] [%d]Get[%s] index=[%d] Requested raft", kv.me, args.ClerkId, args.Key, index)
	}

	kv.opResults[index] = OpResult{valid: false, err: "", value: ""}

	kv.mu.Unlock()

	for {
		time.Sleep(time.Millisecond * 10)

		kv.mu.Lock()

		if kv.killed() {
			reply.Err = ErrWrongLeader
			DPrintf("KV[%d] [%d]Get[%s] index=[%d] Killed", kv.me, args.ClerkId, args.Key, index)
			delete(kv.opResults, index)
			kv.mu.Unlock()
			return
		}

		if kv.opResults[index].valid {
			reply.Value = kv.opResults[index].value
			reply.Err = kv.opResults[index].err
			DPrintf("KV[%d] [%d]Get[%s] index=[%d] %s Value=[%s]", kv.me, args.ClerkId, args.Key, index, reply.Err, reply.Value)
			delete(kv.opResults, index)
			kv.mu.Unlock()
			return
		}

		_, isLeader = kv.rf.GetState()

		if !isLeader {
			reply.Err = ErrWrongLeader
			DPrintf("KV[%d] [%d]Get[%s] index=[%d] Lost leadership", kv.me, args.ClerkId, args.Key, index)
			delete(kv.opResults, index)
			kv.mu.Unlock()
			return
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if kv.killed() {
		DPrintf("KV[%d] [%d]Put[%s] killed", kv.me, args.ClerkId, args.Key)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	op := Op{OpType: OP_TYPE_PUT, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, SequenceNumber: args.SequenceNumber}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("KV[%d] [%d]Put[%s] NOT Leader", kv.me, args.ClerkId, args.Key)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		DPrintf("KV[%d] [%d]Put[%s] index=[%d] Value=[%s] Requested raft", kv.me, args.ClerkId, args.Key, index, args.Value)
	}

	kv.opResults[index] = OpResult{valid: false, err: "", value: ""}

	kv.mu.Unlock()

	for {
		time.Sleep(time.Millisecond * 10)

		kv.mu.Lock()

		if kv.killed() {
			reply.Err = ErrWrongLeader
			DPrintf("KV[%d] [%d]Put[%s] index=[%d] Killed", kv.me, args.ClerkId, args.Key, index)
			delete(kv.opResults, index)
			kv.mu.Unlock()
			return
		}

		if kv.opResults[index].valid {
			reply.Err = kv.opResults[index].err
			DPrintf("KV[%d] [%d]Put[%s] index=[%d] %s", kv.me, args.ClerkId, args.Key, index, reply.Err)
			delete(kv.opResults, index)
			kv.mu.Unlock()
			return
		}

		_, isLeader = kv.rf.GetState()

		if !isLeader {
			reply.Err = ErrWrongLeader
			DPrintf("KV[%d] [%d]Put[%s] index=[%d] Lost leadership", kv.me, args.ClerkId, args.Key, index)
			delete(kv.opResults, index)
			kv.mu.Unlock()
			return
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if kv.killed() {
		DPrintf("KV[%d] [%d]Append[%s] killed", kv.me, args.ClerkId, args.Key)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	op := Op{OpType: OP_TYPE_APPEND, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, SequenceNumber: args.SequenceNumber}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("KV[%d] [%d]Append[%s] NOT Leader", kv.me, args.ClerkId, args.Key)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		DPrintf("KV[%d] [%d]Append[%s] index=[%d] Value=[%s] Requested raft", kv.me, args.ClerkId, args.Key, index, args.Value)
	}

	kv.opResults[index] = OpResult{valid: false, err: "", value: ""}

	kv.mu.Unlock()

	for {
		time.Sleep(time.Millisecond * 10)

		kv.mu.Lock()

		if kv.killed() {
			reply.Err = ErrWrongLeader
			DPrintf("KV[%d] [%d]Append[%s] index=[%d] Killed", kv.me, args.ClerkId, args.Key, index)
			delete(kv.opResults, index)
			kv.mu.Unlock()
			return
		}

		if kv.opResults[index].valid {
			reply.Err = kv.opResults[index].err
			DPrintf("KV[%d] [%d]Append[%s] index=[%d] %s", kv.me, args.ClerkId, args.Key, index, reply.Err)
			delete(kv.opResults, index)
			kv.mu.Unlock()
			return
		}

		_, isLeader = kv.rf.GetState()

		if !isLeader {
			reply.Err = ErrWrongLeader
			DPrintf("KV[%d] [%d]Append[%s] index=[%d] Lost leadership", kv.me, args.ClerkId, args.Key, index)
			delete(kv.opResults, index)
			kv.mu.Unlock()
			return
		}

		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	// close(kv.applyCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.db = make(map[string]string)
	kv.opResults = make(map[int]OpResult)
	kv.clerkWriteHistoryMap = make(map[int64]*ClerkWriteHistory)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.applyChReader()

	return kv
}

func (kv *KVServer) applyChReader() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(Op)
			_, needResult := kv.opResults[msg.CommandIndex]
			_, isLeader := kv.rf.GetState()

			if op.OpType == OP_TYPE_GET {
				DPrintf("KV[%d] Get[%s] index=[%d] Apply", kv.me, op.Key, msg.CommandIndex)
				if needResult {
					value, foundKey := kv.db[op.Key]
					if !isLeader {
						res := OpResult{}
						res.valid = true
						res.err = ErrWrongLeader
						kv.opResults[msg.CommandIndex] = res
					} else if foundKey {
						res := OpResult{}
						res.valid = true
						res.err = OK
						res.value = value
						kv.opResults[msg.CommandIndex] = res
					} else {
						res := OpResult{}
						res.valid = true
						res.err = ErrNoKey
						kv.opResults[msg.CommandIndex] = res
					}
				}
			} else if op.OpType == OP_TYPE_PUT {
				DPrintf("KV[%d] Put[%s] index=[%d] Value=[%s] Apply", kv.me, op.Key, msg.CommandIndex, op.Value)
				history := kv.FindOrAddClerkWriteHistory(op.ClerkId)
				if op.SequenceNumber > history.recentSequenceNumber {
					if op.SequenceNumber != history.recentSequenceNumber+1 {
						panic(fmt.Sprintf("nonconsecutive seq[%d], recent=[%d]", op.SequenceNumber, history.recentSequenceNumber))
					}
					kv.db[op.Key] = op.Value
					history.recentSequenceNumber = op.SequenceNumber
				}
				if needResult {
					if !isLeader {
						res := OpResult{}
						res.valid = true
						res.err = ErrWrongLeader
						kv.opResults[msg.CommandIndex] = res
					} else {
						res := OpResult{}
						res.valid = true
						res.err = OK
						kv.opResults[msg.CommandIndex] = res
					}
				}
			} else if op.OpType == OP_TYPE_APPEND {
				DPrintf("KV[%d] Append[%s] index=[%d] Value=[%s] Apply", kv.me, op.Key, msg.CommandIndex, op.Value)
				history := kv.FindOrAddClerkWriteHistory(op.ClerkId)
				if op.SequenceNumber > history.recentSequenceNumber {
					if op.SequenceNumber != history.recentSequenceNumber+1 {
						panic(fmt.Sprintf("nonconsecutive seq[%d], recent=[%d]", op.SequenceNumber, history.recentSequenceNumber))
					}
					_, foundKey := kv.db[op.Key]
					if foundKey {
						kv.db[op.Key] = kv.db[op.Key] + op.Value
					} else {
						kv.db[op.Key] = op.Value
					}
					history.recentSequenceNumber = op.SequenceNumber
				}
				if needResult {
					if !isLeader {
						res := OpResult{}
						res.valid = true
						res.err = ErrWrongLeader
						kv.opResults[msg.CommandIndex] = res
					} else {
						res := OpResult{}
						res.valid = true
						res.err = OK
						kv.opResults[msg.CommandIndex] = res
					}
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) FindOrAddClerkWriteHistory(clerkId int64) *ClerkWriteHistory {
	clerkWriteHistory, ok := kv.clerkWriteHistoryMap[clerkId]

	if !ok {
		clerkWriteHistory = &ClerkWriteHistory{recentSequenceNumber: 0}
		kv.clerkWriteHistoryMap[clerkId] = clerkWriteHistory
	}

	return clerkWriteHistory
}
