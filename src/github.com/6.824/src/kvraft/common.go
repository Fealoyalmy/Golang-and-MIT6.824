package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int
	RequestID uint64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int
	RequestID uint64
}

type GetReply struct {
	Err   Err
	Value string
}

// 根据客户端ID与请求ID生成唯一的请求码
func UniqueRequestId(clientId int, requestId uint64) uint64 {
	return uint64(clientId<<32) + requestId&0xffffffff
}
