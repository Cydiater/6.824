package mr

import "log"
import "fmt"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strings"
import "strconv"

type Master struct {
	filenames []string
	nReduce int
	nMap int
	ReduceTasks [][]string
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) AllocateMapTask(args *AllocateMapTaskRequest, reply *AllocateMapTaskReply) error {
	reply.Filename = ""
	reply.NReduce = m.nReduce
	if len(m.filenames) == 0 {
		return nil
	}
	reply.Filename = m.filenames[0]
	m.filenames = m.filenames[1:]
	return nil
}

func (m *Master) AllocateReduceTask(args *AllocateReduceTaskRequest, reply *AllocateReduceTaskReply) error {
	if len(m.ReduceTasks) == 0 {
		return nil
	}
	if len(m.ReduceTasks[m.nReduce - 1]) != m.nMap {
		return nil
	}
	reply.Ifilenames = m.ReduceTasks[m.nReduce - 1]
	reply.Ofilename = fmt.Sprintf("mr-out-%v", m.nReduce - 1)
	m.ReduceTasks = m.ReduceTasks[:(m.nReduce - 1)]
	m.nReduce -= 1
	return nil
}

func (m *Master) ReportReduceInput(args *ReportReduceInputRequest, reply *ReportReduceInputReply) error {
	onames := args.Filenames
	for i := 0; i < len(onames); i++ {
		components := strings.Split(onames[i], "-")
		index, err := strconv.Atoi(components[len(components) - 1])
		if err != nil {
			log.Printf("Failed to parse index from %v", onames[i])
			continue
		}
		m.ReduceTasks[index] = append(m.ReduceTasks[index], onames[i])
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

	if m.nReduce == 0 {
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
	m := Master{}

	m.filenames = files
	m.nReduce = nReduce
	m.nMap = len(files)
	m.ReduceTasks = make([][]string, nReduce)

	m.server()
	return &m
}
