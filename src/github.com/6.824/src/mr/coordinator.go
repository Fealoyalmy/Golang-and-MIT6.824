package mr

import (
	"log"
	"sort"
)
import "net"
import "os"
import "fmt"
import "sync"
import "net/rpc"
import "net/http"

var wg sync.WaitGroup
var rw sync.RWMutex

type Coordinator struct {
	// Your definitions here.
	WorkerID        int          // 当前最新的worker ID（标记用于为新worker分配ID）
	FileName        []string     // 待处理txt文件名
	FileStat        []int        // txt文件处理状态
	FileContent     []string     // txt内容
	Intermediate    []KeyValue   // 中间键值对
	MapCompleted    bool         // map任务完成状态
	NumReduce       int          // reduce任务数
	ReduceCompleted []int        // reduce任务完成状态
	ReduceTask      [][]KeyValue // reduce任务列表
}

type ByKey []KeyValue // 用于按键排序的接口
// 实现ByKey接口方法
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPChandler(args *RPCArgs, reply *RPCReply) error {
	// 为worker赋予递增的ID
	rw.Lock()
	if args.ID == 0 {
		c.WorkerID++
		reply.Data = c.WorkerID
		args.ID = reply.Data

	}
	rw.Unlock()
	// 判断worker的请求指令
	if args.Command == "MapRequest" { // worker请求map任务
		rw.RLock()
		if c.MapCompleted == false { // 如果map()任务未全部完成
			rw.RUnlock()
			rw.Lock()                         // c.FileStat读锁
			for i, stat := range c.FileStat { // 遍历文件状态，找出未被处理的txt文件
				if stat != 1 {
					reply.Status = true        // 告诉worker当前有task
					reply.Name = c.FileName[i] // 传递文件名给worker
					//rw.Lock()
					c.FileStat[i] = 1 // 标记已被分配的文件
					fmt.Println(c.FileStat)
					rw.Unlock()
					fmt.Printf("分配%v 给worker%v!\n", reply.Name, args.ID)
					//rw.RUnlock() // c.FileStat读锁//
					return nil
				}
			}
			rw.Unlock()           // c.FileStat读锁//
			rw.Lock()             // c.MapCompleted写锁
			c.MapCompleted = true // 置map()的所有任务状态为已完成
			rw.Unlock()           // c.MapCompleted写锁//
			fmt.Printf("所有map()完成！\n")
			return nil
		} else if c.MapCompleted == true {
			rw.RUnlock()
			rw.Lock()
			sort.Sort(ByKey(c.Intermediate)) // 若全部txt文件均已被处理则对中间键值对做排序，以便接下来分配reduce()
			rw.Unlock()
			rw.Lock()
			for i := 0; i < c.NumReduce; i++ { // 将中间键值对分成块reduce
				c.ReduceTask = append(c.ReduceTask, c.Intermediate[i*len(c.Intermediate)/c.NumReduce:(i+1)*len(c.Intermediate)/c.NumReduce])
				fmt.Printf("len[%d]=%d\n", i, len(c.ReduceTask[i]))
			}
			rw.Unlock()
			fmt.Printf("中间键值对排序完成，reduce任务分块完成！\n")
			reply.Status = false // 当前无task，告知worker
			return nil
		}
	} else if args.Command == "ResultBack" { // worker回传map()处理结果到中间键值对
		rw.Lock()
		c.Intermediate = append(c.Intermediate, args.Data...) // 将worker缓存中的处理结果追加到中间键值对中
		fmt.Printf("worker%v 完成map() %v len(ikv)=%d\n", args.ID, reply.Name, len(c.Intermediate))
		rw.Unlock()
	} else if args.Command == "ReduceRequest" { // worker请求reduce任务
		rw.RLock()
		if c.MapCompleted == false { // 如果map任务尚未全部完成，则不予分配reduce任务
			rw.RUnlock()
			reply.Status = false
			return nil
		}
		rw.RUnlock()
		rw.Lock()
		for i, r := range c.ReduceCompleted { // 遍历reduce任务列表，分配尚未被处理的reduce任务
			if r != 1 {
				reply.Status = true // 告诉worker当前有task
				reply.OutNum = i
				//rw.RLock()
				reply.IRkv = c.ReduceTask[i] // 分配reduce任务给worker
				//rw.RUnlock()
				//rw.Lock()
				//fmt.Println("ReduceCompleted获取写锁")
				c.ReduceCompleted[i] = 1 // 标记已分配的reduce任务
				rw.Unlock()
				//fmt.Println("ReduceCompleted释放写锁")
				fmt.Printf("分配reduce()%v 给worker%v!\n", i, args.ID)
				return nil
			}
		}
		rw.Unlock()
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)  // 注册对象
	rpc.HandleHTTP() // 把rpc监听对应到http处理器（即指定http请求addr+port时，调用的方法）
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname) // 获取监听地址
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // 开启一个go线程持续监听数据
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	for i := 0; i < c.NumReduce; i++ {
		rw.RLock()
		if c.ReduceCompleted[i] == 0 {
			rw.RUnlock()
			return ret
		}
		rw.RUnlock()
	}
	fmt.Println(c.ReduceCompleted)
	fmt.Println("所有reduce任务完成！")
	ret = true
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NumReduce = nReduce
	for i := 0; i < nReduce; i++ { // 将中间键值对分成块reduce
		c.ReduceCompleted = append(c.ReduceCompleted, 0) // reduce任务状态初始化
	}
	for _, filename := range files { // 遍历每个txt文件文件名存数组并初始化文件状态
		c.FileName = append(c.FileName, filename)
		c.FileStat = append(c.FileStat, 0)
	}
	fmt.Printf("txt文件读取完成!\n")
	fmt.Println(c.FileStat)
	c.server()
	return &c
}
