package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	assumedLeaderId int
	clerkId         int64
	sequenceNumber  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.InitClerk()

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs{}
	args.Key = key
	args.ClerkId = ck.clerkId

	reply := GetReply{}

	for {
		DPrintf("Clerk[%d] Call KV[%d] Get[%s]", ck.clerkId, ck.assumedLeaderId, args.Key)
		ok := ck.servers[ck.assumedLeaderId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
		}

		ck.rr()
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.ClerkId = ck.clerkId
	ck.sequenceNumber++
	args.SequenceNumber = ck.sequenceNumber

	reply := PutAppendReply{}

	for {
		DPrintf("Clerk[%d] Call KV[%d] %s[%s] Value=[%s]", ck.clerkId, ck.assumedLeaderId, op, args.Key, args.Value)
		ok := ck.servers[ck.assumedLeaderId].Call("KVServer."+op, &args, &reply)

		if ok {
			if reply.Err == OK {
				return
			}
		}

		ck.rr()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) InitClerk() {
	// You will have to modify this function.

	ck.clerkId = INVALID_CLERK_ID

	for ck.clerkId == INVALID_CLERK_ID {
		ck.clerkId = nrand()
	}

	ck.sequenceNumber = 0
	ck.assumedLeaderId = 0
}

// round-robin
func (ck *Clerk) rr() {
	ck.assumedLeaderId = (ck.assumedLeaderId + 1) % len(ck.servers)
}
