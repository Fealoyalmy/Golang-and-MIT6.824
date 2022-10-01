package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RPCArgs struct {
	ID      int        // worker自身ID
	Command string     // 请求命令
	Data    []KeyValue // 回传给coordinator的数据数组
}

type RPCReply struct {
	Name    string     // 被分配到要处理的文件名
	Content string     // 要处理的文件内容
	IRkv    []KeyValue // 要处理的键值对
	OutNum  int        // 被分配到的reduce()任务号
	Status  bool       // 当前任务状态
	Data    int        // 用来获取全局ID更新worker自身初始ID
}

// Cook up a unique-ish UNIX-domain socket name in /var/tmp, for the coordinator.
// Can't use the current directory since Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
