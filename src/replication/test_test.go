package replication

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

/*
TestCreateBucket tests your implementation of the CreateBucket function. Tests creating buckets with several numbers of
nodes
*/
func TestCreateBucket(t *testing.T) {

	// 1 node
	InitS3(1)
	CreateBucket("create_bucket_1")
	if !BucketExists("create_bucket_1") {
		t.Fatal("Failed to create bucket with one node.")
	}

	// 2 nodes
	InitS3(2)
	CreateBucket("create_bucket_2")
	if !BucketExists("create_bucket_2") {
		t.Fatal("Failed to create bucket with two nodes.")
	}

	// 3 nodes
	InitS3(3)
	CreateBucket("create_bucket_3")
	if !BucketExists("create_bucket_3") {
		t.Fatal("Failed to create bucket with three nodes.")
	}
}

/*
TestReadWriteOneClientOneNode Tests writing and then reading to S3 with one node. Quorum does not need to be
implemented to pass this test. This test may never finish if your RequestReadFile implementation does not return.
*/
func TestReadWriteOneClientOneNode(t *testing.T) {
	InitS3(1)
	setWriteQuorum(1) // just in case you're using this. not necessary to pass the test
	setReadQuorum(1)

	bucketName := "test1_bucket"
	fileName := "test1.txt"
	contents, err := os.ReadFile("../main/test_simple.txt")
	checkError(err)

	CreateBucket(bucketName)
	resultChannel := make(chan []byte, 1) // output channel for client

	// client 1
	go func() {
		RequestWriteFile(bucketName, fileName, contents)
		resultChannel <- RequestReadFile(bucketName, fileName)
	}()
	result := <-resultChannel

	// test writing file
	if !filesEqual("../main/test_simple.txt", "nodes/0/test1_bucket/test1.txt") {
		t.Fatal("main/test_simple.txt does not equal nodes/0/test1_bucket/test1.txt. RequestWriteFile failed.")
	}

	// test reading file
	if !compareFileAndContents("../main/test_simple.txt", result) {
		fmt.Printf("Contents: %s, filename: %s\n", string(result), fileName)
		t.Fatal("main/test_simple.txt does not match result from write and read. RequestReadFileFailed.")
	}
}

/*
TestReadWriteTwoClientOneNode Tests writing and then reading to S3 with one node and two clients. Quorum does not need to be
implemented to pass this test. This test may never finish if your RequestReadFile implementation does not return.
*/
func TestReadWriteTwoClientOneNode(t *testing.T) {
	InitS3(1)
	setWriteQuorum(1) // just in case you're using this. not necessary to pass the test
	setReadQuorum(1)

	bucketName := "test2_bucket"
	fileName := "test2.txt"
	contents, err := os.ReadFile("../main/test_simple.txt")
	checkError(err)

	CreateBucket(bucketName)
	resultChannel := make(chan []byte, 1) // output channel for client

	// client 1
	go func() {
		RequestWriteFile(bucketName, fileName, contents)
	}()

	// client 2
	go func() {
		time.Sleep(time.Second)
		RequestReadFile(bucketName, fileName)
		resultChannel <- RequestReadFile(bucketName, fileName)
	}()
	result := <-resultChannel

	// test writing file
	if !filesEqual("../main/test_simple.txt", "nodes/0/test2_bucket/test2.txt") {
		t.Fatal("main/test_simple.txt does not equal nodes/0/test2_bucket/test2.txt. RequestWriteFile failed.")
	}

	// test reading file
	if !compareFileAndContents("../main/test_simple.txt", result) {
		t.Fatal("main/test_simple.txt does not match result from write and read. RequestReadFileFailed.")
	}
}

/*
TestReadWriteOneClientTwoNodes tests writing and then reading to S3 with one node. Quorum does not need to be
implemented to pass this test. This test may never finish if your RequestReadFile implementation does not return.
*/
func TestReadWriteOneClientTwoNodes(t *testing.T) {
	InitS3(2)
	setWriteQuorum(2) // just in case you're using this. not necessary to pass the test
	setReadQuorum(2)

	bucketName := "test3_bucket"
	fileName := "test3.txt"
	contents, err := os.ReadFile("../main/test_simple.txt")
	checkError(err)

	CreateBucket(bucketName)
	resultChannel := make(chan []byte, 1) // output channel for client

	// client 1
	go func() {
		RequestWriteFile(bucketName, fileName, contents)
		resultChannel <- RequestReadFile(bucketName, fileName)
	}()
	result := <-resultChannel

	// test writing file
	if !filesEqual("../main/test_simple.txt", "nodes/0/test3_bucket/test3.txt") {
		t.Fatal("main/test_simple.txt does not equal nodes/0/test3_bucket/test1.txt. RequestWriteFile failed.")
	}

	// test reading file
	if !compareFileAndContents("../main/test_simple.txt", result) {
		t.Fatal("main/test_simple.txt does not match result from write and read. RequestReadFileFailed.")
	}
}

/**
TestQuorumOneClientFiveNodes tests writing to a file twice, and ensuring that the correct information is gathered.
*/
func TestQuorumOneClientFiveNodes(t *testing.T) {
	InitS3(5)
	setWriteQuorum(3) // must use this number
	setReadQuorum(3)

	bucketName := "test4_bucket"
	fileName := "test4.txt"
	contents1, err := os.ReadFile("../main/test_simple.txt")
	checkError(err)

	CreateBucket(bucketName)
	resultChannel := make(chan []byte, 1) // output channel for client

	// client 1
	go func() {
		RequestWriteFile(bucketName, fileName, contents1)
		resultChannel <- RequestReadFile(bucketName, fileName)
	}()
	result1 := <-resultChannel

	// Check first file
	if !compareFileAndContents("../main/test_simple.txt", result1) {
		t.Fatal("main/test_simple.txt does not match result from write and read. Quorum policy is broken.")
	}

	contents2, err := os.ReadFile("../main/test_simple2.txt")
	checkError(err)
	go func() {
		RequestWriteFile(bucketName, fileName, contents2)
		resultChannel <- RequestReadFile(bucketName, fileName)
	}()
	result2 := <-resultChannel

	// Check first file
	if !compareFileAndContents("../main/test_simple2.txt", result2) {
		t.Fatal("main/test_simple.txt does not match result from write and read. Quorum policy is broken.")
	}

	// finally, make sure that 3 nodes have current versions of the file
	numberNewest := countNewestFileOccurrences(fileName, bucketName)
	if numberNewest != getWriteQuorum() {
		t.Fatalf("%d files written to instead of %d. Write quorum not being used properly.", numberNewest, getWriteQuorum())
	}
}

/*
TestFaultyNode tests to see if your code survives if one of the nodes fails after you write a file to it. Write and Read
quorum policies must be implemented
*/
func TestFaultyNode(t *testing.T) {
	// set up folders, parameters
	InitS3(5)
	setWriteQuorum(5) // must use this number
	setReadQuorum(4)

	bucketName := "test5_bucket"
	fileName := "test5.txt"
	contents, err := os.ReadFile("../main/test_simple.txt")
	checkError(err)

	CreateBucket(bucketName)
	resultChannel := make(chan []byte, 1) // output channel for client

	// client 1
	go func() {
		RequestWriteFile(bucketName, fileName, contents)
		failedNodePath := fmt.Sprintf("nodes/%d/test5_bucket/test5.txt", rand.Intn(5))
		if _, err := os.Stat(failedNodePath); !errors.Is(err, os.ErrNotExist) {
			err := os.Remove(failedNodePath)
			checkError(err)
		}
		resultChannel <- RequestReadFile(bucketName, fileName)
	}()
	result := <-resultChannel

	// Check first file
	if !compareFileAndContents("../main/test_simple.txt", result) {
		t.Fatal("main/test_simple.txt does not match result from write and read. Quorum policy is broken.")
	}

	// finally, make sure that 4 nodes have current versions of the file
	numberNewest := countNewestFileOccurrences(fileName, bucketName)
	if numberNewest != getWriteQuorum()-1 { // one faulty node, remember?
		t.Fatalf("%d files written to instead of %d. Write quorum not being used properly.", numberNewest, getWriteQuorum())
	}
}

/*
TestQuorumThreeClientsFiveNodes is the test to end all tests. Three clients, three different files, 5 nodes, quorum = 3
*/
func TestQuorumThreeClientsFiveNodes(t *testing.T) {
	InitS3(5)
	setWriteQuorum(3)
	setReadQuorum(3)

	bucketName := "test6_bucket"
	fileName := "test6.txt"
	contents1, err := os.ReadFile("../main/test_simple.txt")
	checkError(err)
	contents2, err := os.ReadFile("../main/test_simple2.txt")
	checkError(err)
	contents3, err := os.ReadFile("../main/test_simple3.txt")
	checkError(err)

	CreateBucket(bucketName)
	resultChannel := make(chan []byte, 1) // output channel for client
	var waitGroup sync.WaitGroup
	waitGroup.Add(3)

	/*
		write, read, check and write, repeat. across 3 clients
	*/

	// client 1
	go func() {
		// write file 1
		RequestWriteFile(bucketName, fileName, contents1) // t = 0

		time.Sleep(3 * time.Second) // t = 3
		// read file 2
		resultChannel <- RequestReadFile(bucketName, fileName)

		time.Sleep(3 * time.Second) // t = 6
		// check file 3, client 1 is now done
		if !compareFileAndContents("../main/test_simple3.txt", <-resultChannel) {
			t.Error("main/test_simple3.txt does not match result from write and read. Quorum policy is broken.")
			waitGroup.Done()
			return
		}
		waitGroup.Done()
	}()

	// client 2
	go func() {
		time.Sleep(time.Second)
		// read file 1 to channel
		resultChannel <- RequestReadFile(bucketName, fileName) // t = 1

		time.Sleep(3 * time.Second) // t = 4
		// check file 2
		if !compareFileAndContents("../main/test_simple2.txt", <-resultChannel) {
			t.Error("main/test_simple2.txt does not match result from write and read. Quorum policy is broken.")
			waitGroup.Done()
			return
		}
		// write file 3, client 2 is now done.
		RequestWriteFile(bucketName, fileName, contents3)
		waitGroup.Done()
	}()

	// client 3
	go func() {
		time.Sleep(2 * time.Second)
		// check file 1 is correct
		if !compareFileAndContents("../main/test_simple.txt", <-resultChannel) { // t = 2
			t.Error("main/test_simple.txt does not match result from write and read. Quorum policy is broken.")
			waitGroup.Done()
			return
		}
		// write file 2
		RequestWriteFile(bucketName, fileName, contents2)

		time.Sleep(3 * time.Second) // t = 5
		// read file 3, client 3 is now done
		resultChannel <- RequestReadFile(bucketName, fileName) // client 1 will check to make sure this is correct
		waitGroup.Done()
	}()

	fmt.Println("Info: if this test hangs, your code is likely broken. Beware any messages below.")
	waitGroup.Wait()

	numberNewest := countNewestFileOccurrences(fileName, bucketName)
	if numberNewest != getWriteQuorum() {
		t.Fatalf("%d files written to instead of %d. Write quorum not being used properly.", numberNewest, getWriteQuorum())
	}

	ResetNodes()
}

/*
TestConcurrentDifferentFiles tests that two clients writing to different files concurrently do not interfere with each
other. This catches implementations that use a single global lock instead of per-file locking.
*/
func TestConcurrentDifferentFiles(t *testing.T) {
	InitS3(3)
	setWriteQuorum(2)
	setReadQuorum(2)

	bucketName := "test7_bucket"
	fileNameA := "test7a.txt"
	fileNameB := "test7b.txt"
	contentsA, err := os.ReadFile("../main/test_simple.txt")
	checkError(err)
	contentsB, err := os.ReadFile("../main/test_simple2.txt")
	checkError(err)

	CreateBucket(bucketName)
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	// client 1: writes and reads file A
	resultA := make(chan []byte, 1)
	go func() {
		RequestWriteFile(bucketName, fileNameA, contentsA)
		resultA <- RequestReadFile(bucketName, fileNameA)
		waitGroup.Done()
	}()

	// client 2: writes and reads file B concurrently
	resultB := make(chan []byte, 1)
	go func() {
		RequestWriteFile(bucketName, fileNameB, contentsB)
		resultB <- RequestReadFile(bucketName, fileNameB)
		waitGroup.Done()
	}()

	waitGroup.Wait()

	// check file A
	rA := <-resultA
	if !compareFileAndContents("../main/test_simple.txt", rA) {
		t.Fatal("File A does not match after concurrent write. Per-file locking may be broken.")
	}

	// check file B
	rB := <-resultB
	if !compareFileAndContents("../main/test_simple2.txt", rB) {
		t.Fatal("File B does not match after concurrent write. Per-file locking may be broken.")
	}
}

/*
compareFileAndContents compares a file with a byte array, and sees if they are equal.
*/
func compareFileAndContents(fileName1 string, file2Contents []byte) bool {
	file1Contents, err := os.ReadFile(fileName1)
	checkError(err)
	return bytes.Equal(file1Contents, file2Contents)
}

/*
filesEqual returns true if the specified files are of identical size and have the same contents
*/
func filesEqual(fileName1 string, fileName2 string) bool {
	// step 1: ensure both files exist
	const chunkSize = 64000
	f1, err := os.Open(fileName1)
	checkError(err)
	defer func(f *os.File) {
		checkError(f.Close())
	}(f1)

	f2, err := os.Open(fileName2)
	checkError(err)
	defer func(f *os.File) {
		checkError(f.Close())
	}(f2)

	// step 2: ensure both files are completely identical (byte by byte)
	for {
		b1 := make([]byte, chunkSize)
		_, err1 := f1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := f2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return true
			} else if err1 == io.EOF || err2 == io.EOF {
				return false
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			return false
		}
	}
}

/*
countNewestFileOccurrences determines the number of nodes that possess the newest version of the specified file
*/
func countNewestFileOccurrences(fileName string, bucketName string) int {
	newestVersion := time.Now().Add(-24 * time.Hour)
	numberNewest := 0
	for i := 0; i < getNumberNodes(); i++ {
		// see if file exists on node i
		localFileName := fmt.Sprintf("nodes/%d/%s/%s", i, bucketName, fileName)
		if _, err := os.Stat(localFileName); !errors.Is(err, os.ErrNotExist) {
			// get version, compare it with newest version
			_, version := ReadNodeFile(i, bucketName, fileName)
			if newestVersion.Before(version) { // newer version of file found
				newestVersion = version
				numberNewest = 1
			} else if newestVersion.Equal(version) { // newest version of file found again
				numberNewest++
			}
		}
	}
	return numberNewest
}
