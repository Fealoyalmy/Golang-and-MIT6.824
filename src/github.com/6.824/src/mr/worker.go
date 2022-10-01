package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var WorkerID = 1

// use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := RPCArgs{}
	reply := RPCReply{} // var reply RPCReply

	for true { // 反复请求任务
		cmdCall("MapRequest", &args, &reply)
		if args.ID == 0 { // 如果worker的ID仍未初始值0则赋值
			args.ID = reply.Data
		}
		if reply.Status == true { // 收到task通知
			workerMap(&args, reply.Name, mapf)
			cmdCall("ResultBack", &args, &reply)
			fmt.Printf("worker%v map()完成 %v！ len(Data)= %d\n", args.ID, reply.Name, len(args.Data))
		} else {
			cmdCall("ReduceRequest", &args, &reply)
			if reply.Status == true { // 收到task通知
				workerReduce(reply, reducef) // 调用写文件函数，执行reduce()处理完毕后写入文件
				fmt.Printf("worker%v reduce() 生成mr-out-%v!\n", args.ID, strconv.Itoa(reply.OutNum))
			}
			fmt.Printf("worker%v 未请求到task!\n", args.ID)
		}
	}
}

// 根据文件名读取文件内容，经map()处理后写入参数Data
func workerMap(args *RPCArgs, name string, mapf func(string, string) []KeyValue) {
	file, err := os.Open(name) // 打开txt文件
	if err != nil {            // 判断是否打开文件失败
		log.Fatalf("cannot open %v", name)
	}
	content, err := ioutil.ReadAll(file) // 读txt文件内容
	if err != nil {
		log.Fatalf("cannot read %v", name)
	}
	file.Close()
	args.Data = mapf(name, string(content)) // 调用map()处理txt文件
}

// 将接收到的reduce任务数据写入对应任务号的mr-out-x文件
func workerReduce(reply RPCReply, reducef func(string, []string) string) {
	oname := "mr-out-" + strconv.Itoa(reply.OutNum)
	ofile, _ := os.Create(oname) // 创建名为"mr-out-x"的文件
	for i := 0; i < len(reply.IRkv); {
		j := i + 1
		for j < len(reply.IRkv) && reply.IRkv[j].Key == reply.IRkv[i].Key { // 找到同一key的区间[i, j]
			j++
		}
		values := []string{}     // 设一个空数组
		for k := i; k < j; k++ { // 将同一key对应的Value值全部追加入values数组中
			values = append(values, reply.IRkv[k].Value)
		}
		output := reducef(reply.IRkv[i].Key, values)             // 调用Reduce返回string类型的values长度值
		fmt.Fprintf(ofile, "%v %v\n", reply.IRkv[i].Key, output) // 将合并结果的键值对写入ofile文件
		i = j
	}
	ofile.Close()
}

// 根据指令码请求RPC
func cmdCall(command string, args *RPCArgs, reply *RPCReply) {
	args.Command = command
	*reply = RPCReply{} // 重置reply
	call("Coordinator.RPChandler", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()            // 获取当前进程的uid
	c, err := rpc.DialHTTP("unix", sockname) // 连接rpc的http服务端
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close() // 在函数返回后清除变量

	err = c.Call(rpcname, args, reply) // 调用rpc注册的方法
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
