package mr

import (
	"io/ioutil"
	"log"
	"sort"
)
import "net"
import "os"
import "fmt"
import "net/rpc"
import "net/http"

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
	fmt.Printf("worker进入ID=%v\n", args.ID)
	if args.ID == 0 {
		fmt.Println("workerID++")
		c.WorkerID++
		args.ID = c.WorkerID
	}
	fmt.Printf("c.WorkerID=%v\n", c.WorkerID)
	// 判断worker的请求指令
	if args.Command == "MapRequest" { // worker请求map任务
		if c.MapCompleted == false { // 如果map()任务未全部完成
			for i, stat := range c.FileStat { // 遍历文件状态，找出未被处理的txt文件
				if stat != 1 {
					reply.Status = true // 告诉worker当前有task
					//reply.Index = i
					reply.Name = c.FileName[i]       // 传递文件名给worker
					reply.Content = c.FileContent[i] // 传递文件文本给worker
					c.FileStat[i] = 1                // 标记已被分配的文件
					fmt.Println(c.FileStat)
					fmt.Printf("分配%v 给worker%v!\n", reply.Name, args.ID)
					return nil
				}
			}
			c.MapCompleted = true // 置map()的所有任务状态为已完成
			fmt.Printf("所有map()完成！\n")
			sort.Sort(ByKey(c.Intermediate))   // 若全部txt文件均已被处理则对中间键值对做排序，以便接下来分配reduce()
			for i := 0; i < c.NumReduce; i++ { // 将中间键值对分成块reduce
				c.ReduceTask = append(c.ReduceTask, c.Intermediate[i*len(c.Intermediate)/c.NumReduce:(i+1)*len(c.Intermediate)/c.NumReduce])
			}
			fmt.Printf("中间键值对排序完成，reduce任务分块完成！\n")
		}
		reply.Status = false // 当前无task，告知worker
		fmt.Printf("status=%v\n", reply.Status)
	} else if args.Command == "ResultBack" { // worker回传map()处理结果到中间键值对
		c.Intermediate = append(c.Intermediate, args.Data...) // 将worker缓存中的处理结果追加到中间键值对中
		fmt.Printf("worker%v 完成%vmap() len(ikv)=%d: \n", args.ID, reply.Name, len(c.Intermediate))
	} else if args.Command == "ReduceRequest" { // worker请求reduce任务
		if c.MapCompleted == false { // 如果map任务尚未全部完成，则不予分配reduce任务
			reply.Status = false
			return nil
		}
		for i, r := range c.ReduceCompleted { // 遍历reduce任务列表，分配尚未被处理的reduce任务
			if r != 1 {
				//reply.IRkb = c.Intermediate[i*len(c.Intermediate)/10 : (i+1)*len(c.Intermediate)/10]
				reply.OutNum = i
				reply.IRkb = c.ReduceTask[i] // 分配reduce任务给worker
				c.ReduceCompleted[i] = 1     // 标记已分配的reduce任务
				fmt.Printf("分配reduce() %v 给worker%v!\n", i, args.ID)
			}
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	for _, i := range c.ReduceCompleted {
		if i == 0 {
			return ret
		}
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
	for _, filename := range files { // 遍历文件名读取每个txt文件
		file, err := os.Open(filename) // 打开txt文件
		if err != nil {                // 判断是否打开文件失败
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file) // 读txt文件内容
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		// 遍历读取txt存入数组
		c.FileName = append(c.FileName, filename)
		c.FileStat = append(c.FileStat, 0)
		c.FileContent = append(c.FileContent, string(content))
		file.Close()
	}
	fmt.Printf("txt文件读取完成!\n")
	fmt.Println(c.FileStat)
	c.server()
	return &c
}
