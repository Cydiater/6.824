package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "encoding/json"
import "io/ioutil"
import "bufio"
import "sort"
import "strings"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getMapFilename() (string, int) {
	request := AllocateMapTaskRequest{}
	reply := AllocateMapTaskReply{}
	success := call("Master.AllocateMapTask", &request, &reply)
	if success != true {
		reason := "Failed to exec RPC"
		log.Fatal(reason)
	}
	return reply.Filename, reply.NReduce
}

func notifyReduceInput(onames []string) {
	request := ReportReduceInputRequest{
		Filenames: onames,
	}
	reply := ReportReduceInputReply{}
	success := call("Master.ReportReduceInput", &request, &reply)
	if success != true {
		reason := "Failed to exec RPC"
		log.Fatal(reason)
	}
}

func getReduceTask() ([]string, string) {
	request := AllocateReduceTaskRequest{}
	reply := AllocateReduceTaskReply{}
	success := call("Master.AllocateReduceTask", &request, &reply)
	if success != true {
		reason := "Failed to exec RPC"
		log.Fatal(reason)
	}
	return reply.Ifilenames, reply.Ofilename
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func DoReduce(reducef func(string, []string) string) bool {
	Inames, oname := getReduceTask()
	log.Printf("Got reduce task with count to Inames = %v and oname = %v", len(Inames), oname)
	if len(oname) == 0 {
		return false
	}
	kva := make([]KeyValue, 0)
	for i := 0; i < len(Inames); i++ {
		iname := Inames[i]
		file, err := os.Open(iname)
		defer file.Close()
		if err != nil {
			reason := fmt.Sprintf("Failed to open %v", iname)
			log.Println(reason)
			continue
		}
		scanner := bufio.NewScanner(file)
		kv := KeyValue{}
		for scanner.Scan() {
			text := scanner.Text()
			err := json.Unmarshal([]byte(text), &kv)
			if err != nil {
				reason := fmt.Sprintf("Failed to unmarshal %v", text)
				log.Println(reason)
				continue
			}
			kva = append(kva, kv)
		}
	}
	for i := 0; i < len(Inames); i++ {
		err := os.Remove(Inames[i])
		if err != nil {
			reason := fmt.Sprintf("Failed to remove %v", Inames[i])
			log.Fatal(reason)
		}
	}

	sort.Sort(ByKey(kva))

	log.Printf("Count to kva = %v", len(kva))

	ofile, err := os.Create(oname)
	defer ofile.Close()
	if err != nil {
		reason := fmt.Sprintf("Failed to create %v", oname)
		log.Fatal(reason)
	}

	log.Printf("Write to %v", oname)
	for i, j := 0, 0; i < len(kva); i = j + 1 {
		values := make([]string, 0)
		j = i
		for j + 1 < len(kva) && kva[j + 1].Key == kva[i].Key {
			j += 1
		}
		for k := i; k <= j; k++ {
			values = append(values, kva[k].Value)
		}
		reduced := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, reduced)
	}

	return true
}

func DoMap(filename string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		reason := fmt.Sprintf("Failed to open %v", filename)
		log.Fatal(reason)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		reason := fmt.Sprintf("Failed to read content from %v", filename)
		log.Fatal(reason)
	}

	kva := mapf(filename, string(content))
	buckets := make([][]KeyValue, nReduce)
	for i := 0; i < len(kva); i++ {
		hashValue := ihash(kva[i].Key)
		bucketIndex := hashValue % nReduce
		buckets[bucketIndex] = append(buckets[bucketIndex], kva[i])
	}

	onames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		components := strings.Split(filename, "/")
		oname := fmt.Sprintf("mr-%v-%v", components[len(components) - 1], i)
		onames[i] = oname
		ofile, err := os.OpenFile(oname, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
		if err != nil {
			reason := fmt.Sprintf("Failed to open %v", oname)
			log.Fatal(reason)
		}
		defer ofile.Close()
		for j := 0; j < len(buckets[i]); j++ {
			text, err := json.Marshal(buckets[i][j])
			if err != nil {
				reason := "Failed to marshal"
				log.Fatal(reason)
			}
			if _, err := ofile.WriteString(string(text) + "\n"); err != nil {
				reason := "Failed to write"
				log.Fatal(reason)
			}
		}
	}

	notifyReduceInput(onames)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	more := true
	for more {
		filename, nReduce := getMapFilename()
		log.Printf("got map task with filename = %v nReduce = %v", filename, nReduce)

		if len(filename) == 0 {
			log.Printf("no more map tasks, continue with reduce")
			more = DoReduce(reducef)
			continue
		}

		DoMap(filename, nReduce, mapf)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
