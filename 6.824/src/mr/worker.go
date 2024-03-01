package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// SortedKey 用来存储排序后的键值对
type SortedKey []KeyValue

// Len 重写len,swap,less才能排序
func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }

// ihash use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// 根据ihash函数将key分配到对应的Reduce任务中
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
// 由mian/mrworker.go调用执行
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	flag := true
	for flag {
		// send an RPC to the coordinator asking for a task
		task := getTask()  //向master轮询任务
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task) //调用系统提供的mapf函数来对任务执行mapreduce的map任务
				callDone(&task)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				callDone(&task)
			}
		case WaittingTask:
			{
				fmt.Println("[INFO] All tasks are in progress, please wait")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Println("[INFO] exit")
				flag = false
			}
		}

	}

}

// getTask 获取任务
func getTask() Task {
	reqArgs := TaskArgs{}
	task := Task{}

	ok := call("Master.PollTask", &reqArgs, &task)
	if !ok {
		fmt.Println("worker getTask call failed")
	}
	return task
}

// DoMapTask 将输入文件切分成一个一个的kv结构
// <hello,1> <world,1> <hello ,1> <MapReduce ,1>
//  map节点再把这些缓存数据分成R块（对应 R 个 reduce 节点），并写入本地磁盘中。
func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	var kvs []KeyValue
	filepath := task.Files[0]   // map任务一个task只分配了一个file
	fmt.Println("MapTask filepath： ", filepath)
	file, err := os.Open(filepath) // Open返回的是*File类型
	if err != nil {
		fmt.Println("DoMapTask cannot open ", filepath)
	}
	// 通过io工具包获取content,作为mapf的参数
	content, err := ioutil.ReadAll(file) // ReadAll返回Byte类型
	if err != nil {
		fmt.Println("DoMapTask cannot read ", filepath)
	}
	file.Close()

	// map返回一组KV结构体数组
	kvs = mapf(filepath, string(content))
	// 将mapf输出的kv键值对存储到二维切片HashedKV中，即,将这些kv交付给对应的reducer
	reducerNum := task.ReducerNum
	HashedKV := make([][]KeyValue, reducerNum) //用来存储对应reducer所需要处理的具体kv
	for _, kv := range kvs {
		HashedKV[ihash(kv.Key)%reducerNum] = append(HashedKV[ihash(kv.Key)%reducerNum], kv)
	}
	
	// 将map阶段生成的kvs存储至mr-tmp-*这些文件中
	for i := 0; i < reducerNum; i++ {
		// 将map阶段生成的kv键值对暂时存放在mr-tmp-*这些文件中。   
		// 这里的i就是具体的reducer的编号了

		// 这里这样命名是为了方便后面使用 strings.HasPrefix()和strings.HasSuffix()这两个判断前后缀的函数
		outputFileName := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		outputFile, _ := os.Create(outputFileName)
		encodeFile := json.NewEncoder(outputFile)
		for _, kv := range HashedKV[i] {
			err = encodeFile.Encode(kv)
			if err != nil {
				return
			}
		}
		outputFile.Close()
	}
}

// 传入文件名数组,返回排序好的 kv 数组
// 将key相同的键值对排在一起，并转换成<key values[]>
// map阶段后传出 <hello,1> <world,1> <hello ,1> <MapReduce ,1>
// shuffle为<hello,1> <hello ,1> <MapReduce ,1> <world,1>
func shuffle(files []string) []KeyValue {
	var kvs []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		decodeFile := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decodeFile.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKey(kvs))

	return kvs
}

// DoReduceTask 将shuffle排好的键值对，最后进行记数操作
// shuffle后传出 <hello, [1,1]> <MapReduce, [1]> <world,[1]>
// reduce后为 <hello ,2> <MapReduce ,1> <world,1>
func DoReduceTask(reducef func(string, []string) string, task *Task) {
	reduceFileNum := task.TaskId
	kvs := shuffle(task.Files)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("[ERROR] Failed to create temp file", err)
	}

	// <hello, [1,1]> <MapReduce, [1]> <world,[1]>
	// <hello ,2> <MapReduce ,1> <world,1>
	i := 0
	for i < len(kvs) {
		// <hello,1> <hello ,1> <MapReduce ,1> <world,1>
		// <hello, [1,1]> <MapReduce, [1]> <world,[1]>	
		// 本来应该交给shuffle做的，但是这样结构体的定义就冗余了，不如放在这里
		var values []string
		var j int
		values = append(values, kvs[i].Value)
		for j = i +1; j < len(kvs) && kvs[j].Key == kvs[i].Key; j++{
			values = append(values, kvs[j].Value)
		}
		
		// var reducef func(string, []string) string;
		output := reducef(kvs[i].Key, values)
		fmt.Println(kvs[i].Key, " ", output)
		fmt.Fprintf(tempFile, "%v %v\n", kvs[i].Key, output)
		i = j
	}

	tempFile.Close()

	// 在完全写入后进行重命名
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// callDone Call RPC to mark the task as completed
// 告知master任务已经完成
func callDone(task *Task) Task {
	reply := Task{}
	ok := call("Master.SetTaskDone", task, &reply)
	if ok {
		fmt.Println("worker close task: ", task)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}
