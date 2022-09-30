package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10) // 调用coordinator.go的MakeCoordinator方法
	for m.Done() == false {                  // m为上述方法的返回值，显示任务是否全部完成
		time.Sleep(time.Second) // 如未完成则等待一秒再次检测
	}

	time.Sleep(time.Second)
}
