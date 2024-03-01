package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"net"
	"os"
	"net/rpc"
	"net/http"
)

var mu sync.Mutex

type Master struct {
	// Your definitions here.
	TaskId            int           // 自增的id,每一个任务都是通过Master初始化的,这个属性主要是为了给Task分配唯一id
	ReducerNum        int           // reducer 数量
	files             []string      // 传入的文件数组
	TaskChannelReduce chan *Task    // Reduce 任务
	TaskChannelMap    chan *Task    // Map 任务
	TaskMap           map[int]*Task // 任务 map
	Phase             Phase         // 程序所处阶段(map/reduce/done)
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:             files,
		ReducerNum:        nReduce,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		TaskMap:           make(map[int]*Task, len(files)+nReduce),
		Phase:             MapPhase,
	}

	// Your code here.
	m.makeMapTasks(files)  //发布mapreduce中的map任务，等待worker来取
	m.server()

	go m.CrashDetector()

	return &m
}


// Your code here -- RPC handlers for the worker to call.

// PollTask rpc调用，worker轮询任务
func (m *Master) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	// 判断当前程序处于哪个阶段
	switch m.Phase {
	// 处于 map 阶段
	case MapPhase:
		{
			if len(m.TaskChannelMap) > 0 { // 如果当前有map任务，发一个任务给这个worker
				*reply = *<-m.TaskChannelMap
				if !m.judgeState(reply.TaskId) { // task 不处于 Waiting 状态
					fmt.Printf("Map-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else { // master中没有mapreduce中的map任务了
				reply.TaskType = WaittingTask
				if m.checkTaskDone() { //当master检测到所有map任务都完成后，系统进入reduce阶段
					m.toNextPhase()
				}
				return nil
			}

		}
	case ReducePhase:
		{
			if len(m.TaskChannelReduce) > 0 {
				*reply = *<-m.TaskChannelReduce
				if !m.judgeState(reply.TaskId) {
					fmt.Println("Reduce-task is running", reply)
				}
			} else { // master中没有reduce任务了
				reply.TaskType = WaittingTask 
				if m.checkTaskDone() { //当master检测到所有reduce任务都完成后，系统进入AllDown阶段
					m.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		{
			panic("the phase undefined")
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// server
// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
// main/mrmaster.go 周期性调用Done()来查找整个job是否已经完成
func (m *Master) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if m.Phase == AllDone {
		fmt.Println("[INFO] All tasks are finished,the master will be exit.")
		return true
	} else {
		return false
	}
}

// makeMapTasks 初始化map任务
func (m *Master) makeMapTasks(files []string) {
	for _, v := range files {
		// 生成唯一 id
		id := m.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: m.ReducerNum,
			Files:      []string{v},
			State:      Waiting,  // 等待被worker执行
		}
		m.TaskMap[id] = &task //向TaskMap中分配任务
		m.TaskChannelMap <- &task
	}
}

// 对reduce任务进行处理,初始化reduce任务
func (m *Master) makeReduceTasks() {
	// 生成m.ReducerNum个reduce任务
	for i := 0; i < m.ReducerNum; i++ {
		id := m.generateTaskId()
		task := Task{
			TaskId:     id,
			TaskType:   ReduceTask,
			Files:      selectReduceName(i),
			State:      Waiting,
			ReducerNum: m.ReducerNum,
		}
		m.TaskMap[id] = &task
		m.TaskChannelReduce <- &task
	}
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (m *Master) generateTaskId() int {
	res := m.TaskId
	m.TaskId ++
	return res
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (m *Master) judgeState(taskId int) bool {
	task, ok := m.TaskMap[taskId]   // 如果键不存在，ok的值为false
	if !ok || task.State != Waiting {
		return false
	}
	task.StartTime = time.Now()
	task.State = Working
	return true
}

// 判断是否需要切换到下一阶段 map->reduce ...
func (m *Master) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, v := range m.TaskMap {
		if v.TaskType == MapTask {
			if v.State == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskType == ReduceTask {
			if v.State == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		// map阶段的全部完成,reduce阶段的全部都还没开始
		return true
	} else if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		// reduce阶段的全部完成
		return true
	}
	return false
}

// 切换到下一阶段
func (m *Master) toNextPhase() {
	if m.Phase == MapPhase { // 当前在 map 阶段 map -> reduce
		// 初始化reduce任务
		m.makeReduceTasks()
		m.Phase = ReducePhase
	} else if m.Phase == ReducePhase { // 当前在 reduce 阶段 reduce -> AllDone
		m.Phase = AllDone
	}
}

// 提供一个RPC调用,标记当前任务已经完成
func (m *Master) SetTaskDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		task, ok := m.TaskMap[args.TaskId]
		if ok && task.State == Working {
			task.State = Done
			// fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("[INFO] Map task Id[%d] is already finished.\n", args.TaskId)
		}
	case ReduceTask:
		task, ok := m.TaskMap[args.TaskId]
		if ok && task.State == Working {
			task.State = Done
			// fmt.Printf("[INFO] ReduceTask task Id[%d] is finished,\n", args.TaskId)
		} else {
			fmt.Printf("Reduce task Id[%d] is already finished.\n", args.TaskId)
		}
	default:
		panic("[REEOR] The task type undefined")
	}
	return nil
}

// 从当前工作目录中读取文件列表，并选择map生成的temp文件
func selectReduceName(reduceNum int) []string {
	var s []string
	// 获取当前工作目录
	path, _ := os.Getwd()
	//fmt.Println("当前工作目录： ", path)
	// 获取当前工作目录下的所有文件
	files, _ := ioutil.ReadDir(path)
	for _, file := range files {
		// 以 "mr-tmp" 开头并且以 reduceNum 结尾
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			//fmt.Println("filename： ", file.Name())
			s = append(s, file.Name())
		}
	}
	return s
}

// 健壮性检测函数
func (m *Master) CrashDetector() {
	for {
		// 每次执行任务会先休眠 2s
		time.Sleep(time.Second * 2)
		mu.Lock()
		// 如果任务已经处于完成阶段
		if m.Phase == AllDone {
			mu.Unlock()
			break
		}
		for _, task := range m.TaskMap {
			// 如果任务在工作中且距离开始时间已经过去 10s -> 认为任务已经崩溃
			if task.State == Working && time.Since(task.StartTime) > 10*time.Second {
				fmt.Println("[INFO] the task", task.TaskId, " is crash,take ", time.Since(task.StartTime).Seconds(), "s")
				// 根据任务类型,将任务发送到不同的管道,将任务的状态设置为 Waiting,等待重新执行
				switch task.TaskType {
				case MapTask:
					task.State = Waiting
					m.TaskChannelMap <- task
				case ReduceTask:
					task.State = Waiting
					m.TaskChannelReduce <- task
				}
			}
		}
		mu.Unlock()
	}
}
