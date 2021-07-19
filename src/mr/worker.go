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
import "github.com/google/uuid"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var id string

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getTask(id string) ([]string, []string, string, time.Time) {
	request := AllocateTaskRequest{
		Id: id,
	}
	reply := AllocateTaskReply{}
	success := call("Master.AllocateTask", &request, &reply)
	if success != true {
		reason := "Failed to exec RPC"
		log.Fatal(reason)
	}
	if reply.TaskType == "wait" {
		time.Sleep(time.Second)
		return getTask(id)
	}
	return reply.Inames, reply.Onames, reply.TaskType, reply.FiredTime
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func DoReduce(inames []string, oname string, firedTime time.Time, reducef func(string, []string) string) {
	kva := make([]KeyValue, 0)
	for i := 0; i < len(inames); i++ {
		iname := inames[i]
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

	sort.Sort(ByKey(kva))

	log.Printf("Count to kva = %v", len(kva))

	ofile, err := ioutil.TempFile("", oname)
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
	if time.Since(firedTime).Seconds() >= 10.0 {
		log.Printf("Processing time too long")
		id = uuid.NewString()
		return
	}

	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		reason := fmt.Sprintf("Failed to rename: %v", err)
		log.Fatal(reason)
	}

	for i := 0; i < len(inames); i++ {
		err := os.Remove(inames[i])
		if err != nil {
			reason := fmt.Sprintf("Failed to remove %v", inames[i])
			log.Fatal(reason)
		}
	}

	return
}

func DoMap(iname string, onames []string, firedTime time.Time, mapf func(string, string) []KeyValue) {
	file, err := os.Open(iname)
	defer file.Close()
	if err != nil {
		reason := fmt.Sprintf("Failed to open %v", iname)
		log.Fatal(reason)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		reason := fmt.Sprintf("Failed to read content from %v", iname)
		log.Fatal(reason)
	}

	kva := mapf(iname, string(content))
	buckets := make([][]KeyValue, len(onames))
	for i := 0; i < len(kva); i++ {
		hashValue := ihash(kva[i].Key)
		bucketIndex := hashValue % len(onames)
		buckets[bucketIndex] = append(buckets[bucketIndex], kva[i])
	}

	if time.Since(firedTime).Seconds() >= 10.0 {
		log.Printf("Processing time too long")
		id = uuid.NewString()
		return
	}

	for i := 0; i < len(onames); i++ {
		oname := onames[i]
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
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	id := uuid.NewString()
	for true {
		inames, onames, taskType, firedTime := getTask(id)
		log.Printf("got %v task with len(inames) = %v len(onames) = %v", taskType, len(inames), len(onames))

		if (taskType == "done") {
			return
		}

		if (taskType == "map") {
			DoMap(inames[0], onames, firedTime, mapf)
		}

		if (taskType == "reduce") {
			DoReduce(inames, onames[0], firedTime, reducef)
		}
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
