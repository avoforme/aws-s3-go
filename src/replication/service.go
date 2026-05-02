package replication

import (
	"sync"
	"time"
	"math/rand"
)

/*
service.go serves as an emulator for the Amazon Web Services Simple Storage Service (S3). Implement the below functions
to create a complete miniature implementation of MiniS3

TODO: implement all methods in this file
*/

/*
InitS3 initializes the node computers and unlocks the file table

TODO: you are encouraged to edit this method, if there is something you want to do at the start of each test.
*/
func InitS3(node int) {
	ResetNodes()
	InitializeNodes(node)
}

/*
RequestWriteFile puts the specified file to the specified bucket.

This function must consider multiple clients using S3 at the same time. If two clients want to write to the same file at
the same time, then the client that requested to write first gets to write first. Perhaps implement a kind of scheduler?
*/

var mu sync.Mutex
func RequestWriteFile(bucketName string, fileName string, fileContents []byte) {
	// TODO: implement this method
	mu.Lock()
    defer mu.Unlock()

	// numberNodes := getNumberNodes()
	writeNodeSize := getWriteQuorum()
	var wg sync.WaitGroup
	timeNow := time.Now()

	writeNodeList := rand.Perm(getNumberNodes())[:writeNodeSize]
	for _, i := range writeNodeList {
		wg.Add(1)
		go func(i int, timeNow time.Time) {
			defer wg.Done()
			WriteNodeFile(i, bucketName, fileName, fileContents, timeNow)
		} (i, timeNow)

	}

	wg.Wait()
}

/*
RequestReadFile gets the contents of a file from the specified bucket

RequestReadFile must retrieve the local file from each node and reach a quorum before it returns the correct file

Additionally, this function must consider multiple clients using S3 at the same time. If one client wants to write to a
file while another client wants to read the same file at the same time, then the client that requested first gets to do
its action first. Perhaps implement a kind of scheduler?
*/
func RequestReadFile(bucketName string, fileName string) []byte {
	// TODO: implement this method
	mu.Lock()
    defer mu.Unlock()

    readNodeSize := getReadQuorum()
	readNodeList := rand.Perm(getNumberNodes())[:readNodeSize]

    type Result struct {
        data      []byte
        timestamp time.Time
    }

    results := []Result{}
    var wg sync.WaitGroup
    var resultMu sync.Mutex

    for _, node := range readNodeList {
        wg.Add(1)

        go func(n int) {
            defer wg.Done()

            data, metadata := ReadNodeFile(n, bucketName, fileName)


			if len(data) == 0 {
				return
			}

            resultMu.Lock()
            results = append(results, Result{
                data: data,
                timestamp: metadata,
            })
            resultMu.Unlock()
        }(node)
    }

    wg.Wait()

	if len(results) == 0 {
		return nil
	}

    newest := results[0]

    for _, r := range results {
        if r.timestamp.After(newest.timestamp) {
            newest = r
        }
    }

    return newest.data
}
