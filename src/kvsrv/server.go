package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClerkWriteHistory struct {
	recentSequenceNumber int
	recentVersion        int
}

type ValueLog struct {
	latestValue        string
	latestVersion      int
	valueCheckpointMap map[int]string
}

func (vlog *ValueLog) Get() string {
	return vlog.latestValue
}

func (vlog *ValueLog) GetByVersion(version int) string {
	value, ok := vlog.valueCheckpointMap[version]
	if ok {
		return value
	} else {
		return ""
	}
}

func (vlog *ValueLog) Put(value string) (string, int) {
	outValue := vlog.latestValue
	outVersion := vlog.latestVersion
	vlog.latestValue = value
	vlog.latestVersion = vlog.latestVersion + 1
	vlog.valueCheckpointMap[vlog.latestVersion] = vlog.latestValue
	return outValue, outVersion
}

func (vlog *ValueLog) Append(value string) (string, int) {
	outValue := vlog.latestValue
	outVersion := vlog.latestVersion
	vlog.latestValue = vlog.latestValue + value
	vlog.latestVersion = vlog.latestVersion + 1
	vlog.valueCheckpointMap[vlog.latestVersion] = vlog.latestValue
	return outValue, outVersion
}

func (vlog *ValueLog) FreeVersion(version int) {
	_, ok := vlog.valueCheckpointMap[version]
	if ok {
		delete(vlog.valueCheckpointMap, version)
	}
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.

	db map[string]*ValueLog

	clerkWriteHistoryMap map[int64]*ClerkWriteHistory
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.mu.Lock()

	// We don't need to worry duplicate Get requests, always returning the latest is fine

	clerkWriteHistory := kv.FindClerkWriteHistory(args.ClerkId)
	vlog := kv.FindValueLog(args.Key)

	if vlog != nil {
		if clerkWriteHistory != nil {
			vlog.FreeVersion(clerkWriteHistory.recentVersion)
		}
		reply.Value = vlog.Get()
	} else {
		reply.Value = ""
	}

	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()

	clerkWriteHistory := kv.FindOrAddClerkWriteHistory(args.ClerkId)
	vlog := kv.FindOrAddValueLog(args.Key)

	if args.SequenceNumber == clerkWriteHistory.recentSequenceNumber+1 {
		// New request, let's process it as normal
		vlog.FreeVersion(clerkWriteHistory.recentVersion)
		value, version := vlog.Put(args.Value)
		clerkWriteHistory.recentSequenceNumber = args.SequenceNumber
		clerkWriteHistory.recentVersion = version
		reply.Value = value
	} else if args.SequenceNumber == clerkWriteHistory.recentSequenceNumber {
		// Duplicate request, return cached reply content
		reply.Value = vlog.GetByVersion(clerkWriteHistory.recentVersion)
	} else if args.SequenceNumber > clerkWriteHistory.recentSequenceNumber+1 {
		// Discontinuous sequence number, which should not happen, clients must retry lost request until success
		reply.Value = "DISCONTINUOUS-SEQUENCE-NUMBER"
	} else if args.SequenceNumber < clerkWriteHistory.recentSequenceNumber {
		// Expired request due to unbounded flying time, in this case, reply content doesn't matter, client will not see it
		reply.Value = "EXPIRED-SEQUENCE-NUMBER"
	}

	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()

	clerkWriteHistory := kv.FindOrAddClerkWriteHistory(args.ClerkId)
	vlog := kv.FindOrAddValueLog(args.Key)

	if args.SequenceNumber == clerkWriteHistory.recentSequenceNumber+1 {
		// New request, let's process it as normal
		vlog.FreeVersion(clerkWriteHistory.recentVersion)
		value, version := vlog.Append(args.Value)
		clerkWriteHistory.recentSequenceNumber = args.SequenceNumber
		clerkWriteHistory.recentVersion = version
		reply.Value = value
	} else if args.SequenceNumber == clerkWriteHistory.recentSequenceNumber {
		// Duplicate request, return cached reply content
		reply.Value = vlog.GetByVersion(clerkWriteHistory.recentVersion)
	} else if args.SequenceNumber > clerkWriteHistory.recentSequenceNumber+1 {
		// Discontinuous sequence number, which should not happen, clients must retry lost request until success
		reply.Value = "DISCONTINUOUS-SEQUENCE-NUMBER"
	} else if args.SequenceNumber < clerkWriteHistory.recentSequenceNumber {
		// Expired request due to unbounded flying time, in this case, reply content doesn't matter, client will not see it
		reply.Value = "EXPIRED-SEQUENCE-NUMBER"
	}

	kv.mu.Unlock()
}

func (kv *KVServer) AcknowledgePutAppend(args *AcknowledgePutAppendArgs, reply *AcknowledgePutAppendReply) {
	kv.mu.Lock()

	clerkWriteHistory := kv.FindOrAddClerkWriteHistory(args.ClerkId)
	vlog := kv.FindOrAddValueLog(args.Key)
	vlog.FreeVersion(clerkWriteHistory.recentVersion)

	kv.mu.Unlock()
}

func (kv *KVServer) FindClerkWriteHistory(clerkId int64) *ClerkWriteHistory {
	clerkWriteHistory, ok := kv.clerkWriteHistoryMap[clerkId]

	if ok {
		return clerkWriteHistory
	}

	return nil
}

func (kv *KVServer) FindOrAddClerkWriteHistory(clerkId int64) *ClerkWriteHistory {
	clerkWriteHistory, ok := kv.clerkWriteHistoryMap[clerkId]

	if !ok {
		clerkWriteHistory = &ClerkWriteHistory{recentSequenceNumber: 0, recentVersion: 0}
		kv.clerkWriteHistoryMap[clerkId] = clerkWriteHistory
	}

	return clerkWriteHistory
}

func (kv *KVServer) FindValueLog(key string) *ValueLog {
	valueLog, ok := kv.db[key]

	if ok {
		return valueLog
	}

	return nil
}

func (kv *KVServer) FindOrAddValueLog(key string) *ValueLog {
	valueLog, ok := kv.db[key]

	if !ok {
		valueLog = &ValueLog{latestValue: "", latestVersion: 0, valueCheckpointMap: make(map[int]string)}
		kv.db[key] = valueLog
	}

	return valueLog
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.db = map[string]*ValueLog{}
	kv.clerkWriteHistoryMap = make(map[int64]*ClerkWriteHistory)

	return kv
}
