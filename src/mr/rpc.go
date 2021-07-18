package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type AllocateMapTaskRequest struct {}
type AllocateMapTaskReply struct {
	Filename string
	NReduce int
}

type AllocateReduceTaskRequest struct {}
type AllocateReduceTaskReply struct {
	Ifilenames []string
	Ofilename string
}

type ReportReduceInputRequest struct {
	Filenames []string
}
type ReportReduceInputReply struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
