package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.824/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n") // 检测Shell指令格式是否有误
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1]) // 调用wc.go编写完成的wc.so插件获取函数方法

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] { // 通过Shell指令的第三个参数及其后给出的文件名找到txt文件
		file, err := os.Open(filename) // 打开txt文件
		if err != nil {                // 判断是否打开文件失败
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file) // 读txt文件内容
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))      // 对读取到的内容进行Map()
		intermediate = append(intermediate, kva...) // 将所有的键值对追加入中间键值对数组
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate)) // 中间键值对按key排序

	oname := "mr-out-0"
	ofile, _ := os.Create(oname) // 创建名为"mr-out-0"的文件

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key { // 找到同一key的区间[i, j]
			j++
		}
		values := []string{}     // 设一个空数组
		for k := i; k < j; k++ { // 将同一key对应的Value值全部追加入values数组中
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values) // 调用Reduce返回string类型的values长度值

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output) // 将合并结果的键值对写入ofile文件

		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
