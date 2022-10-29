package main // 每个go项目必须有main包

import (
	"fmt"
	"strconv"
) // 导入fmt包（实现了格式化IO的函数）

var A int       // 全局变量允许仅声明不使用
const B int = 1 // 常量定义

func test() {
	// 定义的变量必须要被使用，否则编译不通过
	/* 普通变量 */
	var a int           // 整型
	var a1, a2 float32  // 浮点型32/64
	var b1 string = "1" // type可省略
	var b2 = 1          // 直接按赋值确定type
	c := 1              // 简略初始化变量（只能一次）

	a = 1
	a1, a2, b1 = 1.1, 2.2, "2" // 可连续赋值

	fmt.Print(a)                      // 打印任意type
	fmt.Println(a1, a2)               // 打印任意type加\n
	fmt.Printf(b1, string(c+b2)+"\n") // 只能打印string

	/* 数组 */
	//var arr1 []int                   // 只声明数组
	var arr2 = [5]int{1, 2, 3, 4, 5} // 定义确定长度数组
	arr3 := []float32{1.1, 2.2, 3.3} // 定义数组长度根据给定元素个数确定
	arr4 := [5]int{1: 1, 3: 2}       // 指定下标赋值

	s1 := make([]int, 3, 6)            // 创建长度为3，容量为6的切片 *空切片为nil
	s1 = append(s1, 2)                 // 往s1切片中添加一个新元素2，len(s1)=4
	s1 = append(s1, []int{7, 8, 9}...) // 往s1中追加一个新的切片
	s2 := arr2[1:4]                    // 切片s为arr2下标1-4的引用，类似python
	fmt.Printf("len(s1)=%d, cap(s1)=%d, s1=%v\n", len(s1), cap(s1), s1)
	copy(s1, s2) // 拷贝s2的内容到s1

	//fmt.Println("len(s1)=", len(s1), "cap(s1)=", cap(s1))

	for i := 0; i < 3; i++ {
		fmt.Printf("arr3[%d] = %f\n", i, arr3[i]) // 类似C语言格式打印
	}
	fmt.Println(arr2, arr3, arr4, s1, s2)

	/* map */
	var m map[string]int     // 声明map
	m = make(map[string]int) // 必须用make初始化map，否则nil map不能用来存放键值对
	m["name1"] = 1
	m["name2"] = 2
	m["name3"] = 3

	fmt.Println(m["name1"])
	for mVal := range m { // 遍历取mVal为key
		fmt.Println(mVal, " = ", m[mVal])
	}
	delete(m, "name3") // 删除map中的元素
	for mVal := range m {
		fmt.Println(mVal, " = ", m[mVal])
	}
	num, ok := m["name1"] // 查看元素在map中是否存在
	if ok {
		fmt.Println("name1的值为", num)
	} else {
		fmt.Println("name1的值不存在！")
	}

	/* 指针 */
	fmt.Printf("变量a的地址: %x\n", &a)
	var ip *int
	var fp *float32
	ip = &a
	fmt.Printf("&a=%x, ip=%x, *ip=%d, fp=%x\n", &a, ip, *ip, fp) // fp为空指针0

	/* 结构体 */
	type Books struct {
		title   string
		author  string
		book_id int
	}
	var book Books = Books{"Go", "someone", 123456}
	fmt.Println(Books{title: "Go", book_id: 123456}) // 忽略字段为0或空
	fmt.Println(book.title)
	var bp *Books = &book // 定义结构体的指针
	fmt.Println(bp)

	//var f func(string) int

	strconv.Itoa(a)  // int转string
	strconv.Atoi(b1) // string转int
}

// 自定义函数
func f1(num1, num2 int) int {
	var res int
	if num1 > num2 {
		res = num1 + num2
	} else { // else必须挨在}后面
		res = num1 - num2
	}
	return res
}

type AA struct {
	a int
	b bool
}

type BB struct {
	a int
	b bool
}

func (a *AA) af(b *BB) {
	a.a = 1
	a.b = true
	b.a = a.a
	b.b = a.b
}

func main() { // 必须有main函数
	//aa := AA{}
	//bb := BB{}
	//fmt.Println(aa, bb)
	//aa.af(&bb)
	//fmt.Println(aa.a, aa.b)
	//fmt.Println(bb.a, bb.b)

	//var r = f1(1, 2)
	//fmt.Print(r)
	//test()
	//sl := []int{}
	//fmt.Println(sl)
	//sl = append(sl, 1)
	//fmt.Println(sl)
	//s := []int{1, 2, 3}
	//sl = append(sl, s...)
	//fmt.Println(sl)

	//s := "I love you"
	//f := func(r rune) bool { return !unicode.IsLetter(r) }
	//words := strings.FieldsFunc(s, f)
	//fmt.Println(words)
	//for _, w := range words {
	//	fmt.Println(w)
	//}

	//arr := [3]int{1, 2, 3}
	//for i, a := range arr {
	//	fmt.Println(i, a)
	//}

	//OutNum := 1
	//
	//oname := "mr-out-" + strconv.Itoa(OutNum)
	//fmt.Println(strconv.Itoa(OutNum))
	//ofile, _ := os.Create(oname)
	//fmt.Println(oname)
	////fmt.Fprintf(ofile, "abc")
	//ofile.Close()

	//a := 7 / 2
	//b := 13 / 2
	//c := 4.6 / 2
	//println(a, b, c)

	//wg := sync.WaitGroup{}
	//a := 1
	//wg.Add(1)
	//go func() {
	//	a = 2
	//	wg.Done()
	//}()
	//wg.Wait()
	//println(a)

	//ch := make(chan int)
	//a := 1
	//go func() {
	//	select {
	//	case ch <- 2:
	//	case <-time.After(200 * time.Millisecond):
	//	}
	//	println("11")
	//}()
	//
	////time.Sleep(300 * time.Millisecond)
	//go func() {
	//	select {
	//	case a = <-ch:
	//	case <-time.After(200 * time.Millisecond):
	//	}
	//	println("22")
	//}()
	//time.Sleep(500 * time.Millisecond)
	//println(a)

	//var a []int
	//a = []int{}
	//
	//a = append(a, 1)
	//a = append(a, 2)
	//println(a[0], a[1])

	a := 0
	for i := 0; i < 3; i++ {
		go f2(a)
	}
	fmt.Println(a)
}

func f2(n int) {
	n++
	println("1")
	f3(n)
}

func f3(n int) {
	n++
	println("1")
}
