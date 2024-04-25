package kvsrv

const INVALID_CLERK_ID int64 = 0

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClerkId        int64
	SequenceNumber int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClerkId int64
}

type GetReply struct {
	Value string
}

type AcknowledgePutAppendArgs struct {
	Key     string
	ClerkId int64
}

type AcknowledgePutAppendReply struct {
}
