package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
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

	bucket := make(map[string][]string) // [key]values

	outF, err := os.OpenFile(outFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("doReduce: %s", err)
	}
	defer outF.Close()

	outEnc := json.NewEncoder(outF)

	for m := 0; m < nMap; m++ {

		kv := new(KeyValue)
		fpath := filepath.Join(IntermediateDir, reduceName(jobName, m, reduceTask))
		err := readIntermediaFile(fpath, func(line []byte) error {

			json.Unmarshal(line, kv)
			bucket[kv.Key] = append(bucket[kv.Key], kv.Value)

			return nil
		})

		if err != nil {
			log.Fatalf("doReduce: %s", err)
		}

	}

	// sort
	sortedKey := make([]string, 0, len(bucket))
	for k := range bucket {
		sortedKey = append(sortedKey, k)
	}
	sort.Strings(sortedKey)

	// output
	for _, k := range sortedKey {
		v := reduceF(k, bucket[k])
		err := outEnc.Encode(&KeyValue{k, v})
		if err != nil {
			log.Fatalf("doReduce: %s", err)
		}
	}

}

func readIntermediaFile(fpath string, onLine func(line []byte) error) error {

	inF, err := os.Open(fpath)
	if err != nil {
		return fmt.Errorf("readIntermediaFile: %s", err)
	}
	defer inF.Close()

	bufIn := bufio.NewScanner(inF)
	for bufIn.Scan() {

		line := bufIn.Bytes()

		err := onLine(line)
		if err != nil {
			return err
		}

	}

	return bufIn.Err()

}
