package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var taskWg sync.WaitGroup
	taskChan := make(chan DoTaskArgs, 5)

	taskWg.Add(ntasks)
	go dispatchTask(taskChan, registerChan, &taskWg)

	switch phase {
	case mapPhase:
		for i, file := range mapFiles {

			taskChan <- DoTaskArgs{
				JobName:       jobName,
				File:          file,
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: n_other,
			}

		}
	case reducePhase:

		for i := 0; i < ntasks; i++ {

			taskChan <- DoTaskArgs{
				JobName:       jobName,
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: n_other,
			}

		}
	}

	// wait for done
	taskWg.Wait()
	// shutdown dispatcher
	close(taskChan)

	fmt.Printf("Schedule: %v done\n", phase)
}

func dispatchTask(tasks chan DoTaskArgs, registerChan chan string, taskWg *sync.WaitGroup) {

	log.Println("dispatch start")
	for task := range tasks {

		go func(t DoTaskArgs) {

			addr := <-registerChan

			ok := call(addr, "Worker.DoTask", t, nil)

			if !ok {
				// retry
				tasks <- t
			} else {
				// done
				taskWg.Done()
			}

			// free worker
			registerChan <- addr

		}(task)

	}
	log.Println("dispatch exit")

}
