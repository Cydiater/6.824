package mr

import "log"
import "fmt"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strings"
import "sync"
import "time"
import "github.com/google/uuid"

type Task struct {
	id string
	inames []string
	onames []string
	taskType string
	status string
}

type Slave struct {
	updateTime time.Time
	id string
	task *Task
}

type Master struct {
	inames []string
	workers map[string]*Slave
	tasks map[string]*Task
	nReduce int
	status string // map -> reduce -> done
	mu sync.Mutex
}

func (m *Master) AllocateTask(args *AllocateTaskRequest, reply *AllocateTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Printf("Recive request from %v", args.Id)
	var worker *Slave
	var ok bool
	if worker, ok = m.workers[args.Id]; !ok { // track to a new worker
		log.Printf("New worker for %v", args.Id)
		worker = &Slave {
			id: args.Id,
			task: nil,
		}
		m.workers[args.Id] = worker
	}
	if worker.task != nil { // checkout last task
		worker.task.status = "done"
		delete(m.tasks, worker.task.id)
		worker.task = nil
		log.Printf("Remaining %v tasks", len(m.tasks))
	}
	worker.updateTime = time.Now()
	for _, task := range m.tasks { // allocate a new task
		if task.status == "running" {
			continue
		}
		worker.task = task
		worker.task.status = "running"
		break
	}
	if worker.task != nil { // allocate a task successfully
		reply.Inames = worker.task.inames
		reply.Onames = worker.task.onames
		reply.TaskType = worker.task.taskType
		reply.FiredTime = worker.updateTime
		return nil
	}
	for id, worker := range m.workers { // handle crashed workers
		if worker.task == nil {
			continue
		}
		if time.Since(worker.updateTime).Seconds() < 10.0 {
			continue
		}
		log.Printf("worker %v is crashed", worker.id)
		m.tasks[worker.task.id] = worker.task
		worker.task.status = "ready"
		delete(m.workers, id)
	}
	if len(m.tasks) == 0 { // switch master status
		log.Printf("switch master status from %v", m.status)
		if m.status == "map" {
			m.status = "reduce"
		} else if m.status == "reduce" {
			m.status = "done"
		}
	} else {
		reply.TaskType = "wait"
		return nil
	}
	if m.status == "map" {
		reply.TaskType = "wait"
		return nil
	}
	if m.status == "reduce" {
		log.Printf("generate reduce tasks")
		for i := 0; i < m.nReduce; i++ {
			task := Task {
				id: uuid.NewString(),
				onames: []string{fmt.Sprintf("mr-out-%v", i)},
				taskType: "reduce",
				status: "ready",
			}
			for j := 0; j < len(m.inames); j++ {
				components := strings.Split(m.inames[j], "/")
				task.inames = append(task.inames, fmt.Sprintf("mr-%v-%v", components[len(components) - 1], i))
			}
			m.tasks[task.id] = &task
		}
		reply.TaskType = "wait"
		return nil
	}
	if m.status == "done" {
		reply.TaskType = "done"
		return nil
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	if m.status == "done" {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		inames: files,
		status: "map",
		tasks: make(map[string]*Task),
		workers: make(map[string]*Slave),
	}

	m.nReduce = nReduce
	for _, filename := range files {
		onames := []string{}
		for j := 0; j < nReduce; j++ {
			components := strings.Split(filename, "/")
			onames = append(onames, fmt.Sprintf("mr-%v-%v", components[len(components) - 1], j))
		}
		task := Task {
			id: uuid.NewString(),
			inames: []string{filename},
			onames: onames,
			taskType: "map",
			status: "ready",
		}
		m.tasks[task.id] = &task
	}

	m.server()
	return &m
}
