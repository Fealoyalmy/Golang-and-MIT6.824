package mr

import (
	"fmt"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := RPCArgs{}
	reply := RPCReply{}

	fmt.Println(args)
	fmt.Println(reply)
	for true {
		args.Command = "MapRequest"
		call("Coordinator.RPChandler", &args, &reply)
		fmt.Printf("status=%v\n", reply.Status)
		if reply.Status == true { // 收到task通知
			args.Data = mapf(reply.Name, reply.Content) // 调用map()处理txt文件
			args.Command = "ResultBack"
			call("Coordinator.RPChandler", &args, &reply) // 回传处理结果到coordinator
			//time.Sleep(10 * time.Second)
			fmt.Printf("worker%v map() 完成%v len(Data)= %d!\n", args.ID, reply.Name, len(args.Data))
		} else {
			args.Command = "ReduceRequest"
			call("Coordinator.RPChandler", &args, &reply)
			if reply.Status == true { // 收到task通知
				for i := 0; i < len(reply.IRkb); {
					j := i + 1
					for j < len(reply.IRkb) && reply.IRkb[j].Key == reply.IRkb[i].Key { // 找到同一key的区间[i, j]
						j++
					}
					values := []string{}     // 设一个空数组
					for k := i; k < j; k++ { // 将同一key对应的Value值全部追加入values数组中
						values = append(values, reply.IRkb[k].Value)
					}
					output := reducef(reply.IRkb[i].Key, values) // 调用Reduce返回string类型的values长度值

					oname := "mr-out-" + string(reply.OutNum)
					ofile, _ := os.Create(oname)                             // 创建名为"mr-out-0"的文件
					fmt.Fprintf(ofile, "%v %v\n", reply.IRkb[i].Key, output) // 将合并结果的键值对写入ofile文件
					ofile.Close()
					i = j
				}
				fmt.Printf("worker%v reduce() 完成%v!\n", args.ID, reply.Name)
			}
			fmt.Printf("worker%v 当前无task!\n", args.ID)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
