package kvraft

import (
	"6.824/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"
import "sync"

var clientGerarator int32

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu              sync.Mutex
	lastRPCServerID int    // 上次成功通信的RPCserver
	clientID        int    // 客户端ID
	nextRequestID   uint64 // 下一次请求ID
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
	ck.mu = sync.Mutex{}
	ck.lastRPCServerID = 0                                  // 初始化通信RPCserver
	ck.clientID = int(atomic.AddInt32(&clientGerarator, 1)) // 每次构建新的Clerk时客户端ID自增
	ck.nextRequestID = 0                                    // 初始化请求ID
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &GetArgs{ // 初始化Get请求参数
		ClientID:  ck.clientID,
		RequestID: atomic.AddUint64(&ck.nextRequestID, 1),
		Key:       key,
	}
	rpcServerID := ck.lastRPCServerID // 从上一次通信的RPCserver开始建立通信

	for {
		reply := &GetReply{}
		ok := ck.servers[rpcServerID].Call("KVServer.Get", args, reply)
		if !ok { // 说明请求的不是leader，依次尝试每个server
			rpcServerID++
			rpcServerID %= len(ck.servers)
		} else if reply.Err == OK { // 请求成功，获取返回值
			ck.lastRPCServerID = rpcServerID // 更新上次通信成功的RPCserverID
			ck.lastRPCServerID %= len(ck.servers)
			return reply.Value
		} else { // 可能请求码出现不幂等的情况，依次尝试每个server
			rpcServerID++
			rpcServerID %= len(ck.servers)
		}
		time.Sleep(time.Millisecond * 1)
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &PutAppendArgs{ // 初始化Put/Append请求参数
		ClientID:  ck.clientID,
		RequestID: atomic.AddUint64(&ck.nextRequestID, 1),
		Key:       key,
		Value:     value,
		Op:        op,
	}
	rpcServerID := ck.lastRPCServerID // 从上一次通信的RPCserver开始建立通信

	for {
		reply := &PutAppendReply{}
		ok := ck.servers[rpcServerID].Call("KVServer.PutAppend", args, reply)
		if !ok {
			rpcServerID++
			rpcServerID %= len(ck.servers)
		} else if reply.Err == OK {
			ck.lastRPCServerID = rpcServerID
			ck.lastRPCServerID %= len(ck.servers)
			return
		} else {
			rpcServerID++
			rpcServerID %= len(ck.servers)
		}
		time.Sleep(time.Millisecond * 1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
