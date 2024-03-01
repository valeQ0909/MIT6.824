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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	flag := true
	for flag {
		// send an RPC to the coordinator asking for a task
		task := getTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
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

	ok := call("master.PollTask", &reqArgs, &task)
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
	filepath := task.Files[0]
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
	//初始化并循环遍历 []KeyValue
	rn := task.ReducerNum
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)
	for _, kv := range kvs {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}

	for i := 0; i < rn; i++ {
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

// DoReduceTask 将shuffle排好的键值对最后进行记数操作
// shuffle后传出 <hello [1,1]> <MapReduce ,[1]> <world,[1]>
// <hello ,2> <MapReduce ,1> <world,1>
func DoReduceTask(reducef func(string, []string) string, task *Task) {
	reduceFileNum := task.TaskId
	kvs := shuffle(task.Files)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("[ERROR] Failed to create temp file", err)
	}
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
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
func callDone(finishedTask *Task) Task {

	reply := Task{}
	ok := call("Coordinator.SetTaskDone", finishedTask, &reply)

	if ok {
		fmt.Println("[INFO] close task: ", finishedTask)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}
