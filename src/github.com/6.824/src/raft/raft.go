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
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

// 定义日志条目
type Log struct {
	index   int    // 索引
	command string // 日志命令
	term    int    // 当前日志条目所在term
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's role
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted role
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// role a Raft server must maintain.
	role int // 是2:leader/1:candidate/0:follower?

	currentTerm int   // 当前服务器看到的最新term
	votedFor    int   // 在当前term收到投票的候选人ID
	log         []Log // log条目

	commitIndex int // 已知被提交的最高log条目
	lastApplied int // 应用到状态机的上一条log条目

	nextIndex  int // 要送到服务器的下一个log index (each)
	matchIndex int // 已知被备份到服务器的最新log index (each)
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
	if rf.role == 2 {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent role to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() { //2C
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted role.
func (rf *Raft) readPersist(data []byte) { //2C
	if data == nil || len(data) < 1 { // bootstrap without any role?
		return
	}
	// Your code here (2C).
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool { //2D
	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) { //2D
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的term
	CandidateID  int // 候选人请求投票
	LastLogIndex int // 候选人的最后一个log条目号
	LastLogTerm  int // 候选人的最后一个log条目对应的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	mu          sync.Mutex
	Term        int  // 系统当前term，用于候选人更新自己的term
	VoteGranted bool // true表示候选人获得投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || rf.votedFor != 0 && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		rf.currentTerm = args.Term
		rf.role = 0
	} else if (rf.votedFor == 0 || rf.votedFor == args.CandidateID) &&
		rf.log[len(rf.log)-1].term <= args.LastLogTerm && len(rf.log)-1 <= args.LastLogIndex {
		reply.VoteGranted = true
	}
	return
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

// AppendEntries RPC 结构定义（实现心跳）
type AppendEntriesArgs struct {
	Term         int   // leader的term
	LeaderID     int   // follower重定向client
	PreLogIndex  int   // 新条目之前的日志条目index
	PreLogTerm   int   // 新条目之前的日志条目term
	Entries      []Log // 需要存储的log条目
	LeaderCommit int   // leader的提交index
}

type AppendEntriesReply struct {
	mu      sync.Mutex
	Term    int  // 当前term（用于leader更新自己）
	Success bool // 如果条目匹配先前logindex和termindex则返回true
}

// 服务器接收来自leader的AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { // 如果leader.term < follower.term
		reply.Success = false
		return
	}
	if args.PreLogTerm != rf.log[args.PreLogIndex].term { // 如果leader的新log之前的条目term不是最新的
		reply.Success = false
		return
	}
	rf.currentTerm = args.Term
	rf.role = 0
	if rf.log[len(rf.log)-1].term == args.PreLogTerm && len(rf.log)-1 == args.PreLogIndex {
		reply.Success = true
	} else {
		// 如果follower中的log与leader的不一致，则覆盖其不一致部分并append没有的部分
		for i := 0; i < len(args.Entries); i++ {
			if len(rf.log) <= len(args.Entries) && rf.log[i] != args.Entries[i] {
				rf.log[i] = args.Entries[i]
			} else {
				rf.log = append(rf.log, args.Entries[i])
			}
		}
	}
	// 设置follower的最后提交logIndex = min(leader的提交logIndex,args.Entries[-1].Index)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit >= len(args.Entries)-1 {
			rf.commitIndex = len(args.Entries) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
func (rf *Raft) Start(command interface{}) (int, int, bool) { //2B
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		switch rf.role {
		case 0: // 如果当前rf为follower，则等待RPC请求，否则因超时进入选举
			//time.Sleep(100 * time.Millisecond) // 进入100ms计时器，等待candidate/leader的RPC
			// TODO 如果收到RPC应该改变某些状态 (?是否可以用channel?WaitGroup???)
		case 1: // 如果当前rf为candidate，则准备请求投票
			rvArgs := RequestVoteArgs{ // 初始化rpc参数
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: rf.commitIndex,
				LastLogTerm:  rf.log[1].term,
			}
			rvReply := RequestVoteReply{}
			for i := 0; i < len(rf.peers); i++ { // 向每个服务器发送请求投票
				if i != rf.me {
					rf.sendRequestVote(i, &rvArgs, &rvReply)
				}
			}
			// TODO (?需要考虑选举超时的问题，如重置计时器，方法考虑如上???)
			time.Sleep(1000 * time.Millisecond) // 1s
		case 2: // 如果当前rf为leader，则定期发送心跳给所有服务器
			aeArgs := AppendEntriesArgs{
				Term:         rf.currentTerm,             // leader的term
				LeaderID:     rf.me,                      // follower重定向client
				PreLogIndex:  len(rf.log) - 1,            // 新条目之前的日志条目index
				PreLogTerm:   rf.log[len(rf.log)-1].term, // 新条目之前的日志条目term
				Entries:      []Log{},                    // 需要存储的log条目
				LeaderCommit: rf.commitIndex,             // leader的提交index
			}
			aeReply := AppendEntriesReply{}
			for i := 0; i < len(rf.peers); i++ { // 向每个服务器发送心跳
				if i != rf.me {
					rf.sendAppendEntries(i, &aeArgs, &aeReply)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond) // 100ms
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order.
// persister is a place for this server to save its persistent role,
// and also initially holds the most recent saved role, if any.
// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = 0 // 初始化为follower
	rf.currentTerm = 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = rf.commitIndex + 1
	rf.matchIndex = rf.commitIndex

	// initialize from role persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
