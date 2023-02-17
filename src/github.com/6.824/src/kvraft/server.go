package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID       int
	RequestID      uint64
	OpType         string // “Get“/"Put"/"Append"
	Key            string
	Value          string
	StartTimestamp int64 // 开始时间戳（暂不用）
}

type OpContext struct {
	ClientID        int
	RequestID       uint64
	UniqueRequestID uint64      // 生成的唯一请求码
	Op              *Op         // op参数组
	Term            int         // 从raft获取到的当前term
	WaitCh          chan string // 每个op对应一个信道，保证每条op都能得到独立响应
}

// 根据op参数添加信息生成新的op文本
func NewOpContext(op *Op, term int) *OpContext {
	return &OpContext{
		ClientID:        op.ClientID,
		RequestID:       op.RequestID,
		UniqueRequestID: UniqueRequestId(op.ClientID, op.RequestID),
		Op:              op,
		Term:            term,
		WaitCh:          make(chan string, 1),
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate      int // snapshot if log grows this big
	persister         *raft.Persister
	lastIncludedIndex int

	// Your definitions here.
	kvStore          map[string]string     //k-v对
	opContextMap     map[uint64]*OpContext //用于每个请求的上下文
	lastRequestIdMap map[int]uint64        //clientID-->lastRequestID，维持幂等性，需要客户端能够保证串行
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := &Op{ // 配置op参数
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		OpType:    "Get",
		Key:       args.Key,
		//StartTimestamp: time.Now().UnixMilli(),
	}

	term := 0
	isLeader := false
	if term, isLeader = kv.rf.GetState(); !isLeader { // 向对应raft请求状态查看是否为leader
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	opContext := NewOpContext(op, term)                    // 生成新op文本（添加ClientID与RequestID结合生成的唯一请求码，添加term与WaitCh信道）
	kv.opContextMap[opContext.UniqueRequestID] = opContext // 将op文本加入字典
	kv.mu.Unlock()
	_, _, ok := kv.rf.Start(*op) // 将op发送到对应raft
	defer func() {               // 返回时删除字典中的该op文本（保证按照线性执行一个指令清除一个排队）
		kv.mu.Lock()
		delete(kv.opContextMap, opContext.UniqueRequestID)
		kv.mu.Unlock()
	}()
	if !ok { // 发送cmd时可能leader改变导致错误
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case c := <-opContext.WaitCh: // op信道获取信息则成功
		reply.Err = OK
		reply.Value = c // 写入查询到的val
	case <-time.After(time.Millisecond * 1000): // 设置超时
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := &Op{
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		//StartTimestamp: time.Now().UnixMilli(),
	}
	term := 0
	isLeader := false
	reply.Err = ErrWrongLeader
	if term, isLeader = kv.rf.GetState(); !isLeader {
		return
	}

	kv.mu.Lock()
	if lastRequestID, ok := kv.lastRequestIdMap[op.ClientID]; ok && lastRequestID >= op.RequestID { // 查询并比较本次请求ID与同一client的上次请求ID（不是更新的则无效）
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	opContext := NewOpContext(op, term)
	kv.opContextMap[UniqueRequestId(op.ClientID, op.RequestID)] = opContext // 在op文本字典中添加本次op文本
	kv.mu.Unlock()
	_, _, ok := kv.rf.Start(*op) // 向对应的raft发送op
	defer func() {
		//DPrintf("server PutAppend, args: %v, reply: %v", mr.Any2String(args), mr.Any2String(reply))
		kv.mu.Lock()
		delete(kv.opContextMap, UniqueRequestId(op.ClientID, op.RequestID))
		kv.mu.Unlock()
	}()
	if !ok {
		return
	}
	select {
	case <-opContext.WaitCh:
		reply.Err = OK
	case <-time.After(time.Millisecond * 1000):
		reply.Err = ErrTimeout
	}
}

// 串行写状态机
func (kv *KVServer) applyStateMachineLoop() {
	for !kv.killed() { // 在KVServer运行期间持续循环执行
		select {
		case applyMsg := <-kv.applyCh: // 持续接收对应raft的apply信息，否则阻塞等待
			if applyMsg.CommandValid { // 收到指令提交
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					op := applyMsg.Command.(Op)                           // 取出提交的op命令
					if op.RequestID <= kv.lastRequestIdMap[op.ClientID] { //保证幂等性
						return
					}
					if applyMsg.CommandIndex <= kv.lastIncludedIndex && op.OpType != "Get" { // 如果提交的cmd index不是更新的且不是Get操作
						if c, ok := kv.opContextMap[UniqueRequestId(op.ClientID, op.RequestID)]; ok { // 找到唯一请求码对应的client反馈0
							c.WaitCh <- "0"
						}
						return
					}
					switch op.OpType { // 根据操作指令区分
					case "Put":
						kv.kvStore[op.Key] = op.Value                   // 创建新的键值对
						kv.lastRequestIdMap[op.ClientID] = op.RequestID // 更新对应client的最后请求ID
						kv.maybeSnapshot(applyMsg.CommandIndex)         // 判断是否需要生成快照
					case "Append":
						kv.kvStore[op.Key] += op.Value // 在指定key后附加值
						kv.lastRequestIdMap[op.ClientID] = op.RequestID
						kv.maybeSnapshot(applyMsg.CommandIndex)
					case "Get":
						//Get请求不需要更新lastRequestId
					}
					val := kv.kvStore[op.Key] // 操作完成后反馈最新的键值
					//使得写入的client能够响应
					if c, ok := kv.opContextMap[UniqueRequestId(op.ClientID, op.RequestID)]; ok { // 找到唯一请求码对应的op进行响应
						c.WaitCh <- val // 响应反馈最新值
					}
				}()
			} else if applyMsg.SnapshotValid { // 收到快照提交
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					if kv.decodeSnapshot(applyMsg.Snapshot) { // 如果无快照信息，则需要对kv进行快照装载
						kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot)
					}
				}()
			}
		}
	}
}

func (kv *KVServer) maybeSnapshot(index int) {
	if kv.maxraftstate == -1 { // 如果raft未拥有持久化状态记录则无需生成快照
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate { // 如果raft状态大小大于最大状态限制时则开启快照生成 TODO ？不确定RaftStateSize()到底返回的是什么信息
		kv.rf.Snapshot(index, kv.encodeSnapshot(index)) // 将KVServer状态打包进raft的本次快照中
	}
}

// 将当前KVServer状态进行快照编码（上层已加锁）
func (kv *KVServer) encodeSnapshot(lastIncludedIndex int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(lastIncludedIndex)
	e.Encode(kv.lastRequestIdMap) //持久化每个client的最大已执行过的写请求
	return w.Bytes()
}

// 解码快照信息（上层已加锁）
func (kv *KVServer) decodeSnapshot(snapshot []byte) bool {
	if len(snapshot) == 0 {
		return true
	} // 如果该kv提交数据不包含快照 TODO ？不理解为什么leader传来的snapshot为空要装载
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	// TODO ？不理解为什么leader传来的snapshot解码为空时要装载
	if err := d.Decode(&kv.kvStore); err != nil {
		return false
	}
	if err := d.Decode(&kv.lastIncludedIndex); err != nil {
		return false
	}
	//持久化每个client的最大已执行过的写请求
	if err := d.Decode(&kv.lastRequestIdMap); err != nil {
		return false
	}
	return true
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
	kv.me = me                     // 赋予自身ID
	kv.maxraftstate = maxraftstate // 赋予raft最大状态，超过则需要打快照
	kv.persister = persister       // 获取外部持久化器

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 10)             // 开辟10容量的apply通道
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // 根据配置创建对应raft（同ID）
	kv.kvStore = make(map[string]string)                  // 创建kv库
	kv.opContextMap = make(map[uint64]*OpContext)         // 创建op文本字典
	kv.lastRequestIdMap = make(map[int]uint64)            // 创建客户端最后请求ID字典
	kv.decodeSnapshot(persister.ReadSnapshot())           // 执行一次硬装载操作
	go kv.applyStateMachineLoop()                         // 开启主循环
	return kv
}
