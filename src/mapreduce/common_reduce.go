package mapreduce

import (
	"encoding/json"
	"io"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	kvs := make([]KeyValue, 0, 1024)

	for m := 0; m < nMap; m++ {
		tmpName := reduceName(jobName, m, reduceTask)
		tmpFile, err := os.Open(tmpName)
		errorCheck(err)

		enc := json.NewDecoder(tmpFile)
		for {
			var kv KeyValue
			err := enc.Decode(&kv)
			if err == io.EOF {
				break
			}
			errorCheck(err)

			kvs = append(kvs, kv)
		}

		tmpFile.Close()
	}

	out, err := os.Create(outFile)
	errorCheck(err)
	enc := json.NewEncoder(out)

	// ----- by hash -----
	if false {
		t := map[string][]string{}
		for _, kv := range kvs {
			_, ok := t[kv.Key]
			if !ok {
				t[kv.Key] = make([]string, 0)
			}
			t[kv.Key] = append(t[kv.Key], kv.Value)
		}
		for k, v := range t {
			vv := reduceF(k, v)
			err := enc.Encode(&KeyValue{k, vv})
			errorCheck(err)
		}
	}
	// ----- by sort -----
	if true {

		sort.Slice(kvs, func (i, j int) bool {
			return kvs[i].Key < kvs[j].Key
		})

		preKey := kvs[0].Key
		preIdx := 0
		n := len(kvs)

		for i := 1; i < n; i++ {
			if kvs[i].Key != preKey {
				newKey := preKey

				values := sliceValue(preIdx, i, kvs)

				enc.Encode(&KeyValue{newKey, reduceF(newKey, values)})
				preKey = kvs[i].Key
				preIdx = i
			}

			if i == n - 1 {
				newKey := kvs[i].Key

				var startIdx int
				if kvs[i].Key != preKey {
					startIdx = preIdx + 1
				} else {
					startIdx = preIdx
				}

				values := sliceValue(startIdx, n, kvs)

				enc.Encode(&KeyValue{newKey, reduceF(newKey, values)})
			}
		}

	}

	out.Close()
}

func sliceValue(startIdx, endIdx int, kvs []KeyValue) []string {
	values := make([]string, 0)
	for _, kv := range kvs[startIdx: endIdx] {
		values = append(values, kv.Value)
	}
	return values
}