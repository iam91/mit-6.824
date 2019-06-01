package mapreduce

import (
	"fmt"
	"sync"
)

//type WorkerList struct {
//	mu sync.Mutex
//	workers map[string]bool
//}
//
//func (wl *WorkerList) offer(w string) {
//	wl.mu.Lock()
//	wl.workers[w] = true
//	wl.mu.Unlock()
//}
//
//func (wl *WorkerList) poll() string {
//	for {
//		for k, v := range wl.workers {
//			if v {
//				wl.workers[k] = false
//				return k
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//	}
//}


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
	var nTasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		nTasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		nTasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", nTasks, phase, nOther)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	wg := sync.WaitGroup{}
	wg.Add(nTasks)

	tl := make(chan int)

	go func() {
		for t := 0; t < nTasks; t++ {
			tl <- t
		}
	}()

	go func() {
		// put the following loop in a goroutine for the reason that
		// `t := <- tl` may block when no more task in `tl`, thus
		// it should be killed by someone (here it will be killed
		// when the main goroutine exits)
		for {
			t := <- tl

			var file string
			switch phase {
			case mapPhase: file = mapFiles[t]
			case reducePhase: file = ""
			}

			doTaskArgs := DoTaskArgs{
				JobName: jobName,
				File: file,
				Phase: phase,
				TaskNumber: t,
				NumOtherPhase: nOther}

			go func() {
				w := <- registerChan
				ok := call(w, "Worker.DoTask", &doTaskArgs, nil)
				if !ok {
					fmt.Printf("Worker: RPC %s DoTask error\n", w)
					tl <- t
				} else {
					wg.Done()
				}
				registerChan <- w
			}()
		}
	}()

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}

