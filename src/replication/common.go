package replication

import (
	"log"
)

var privateNumberNodes = 0
var privateWriteQuorum = 0
var privateReadQuorum = 0

// Propagate error if it exists
func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func setNumberNodes(nodes int) {
	privateNumberNodes = nodes
}

/*
getNumberNodes gets the number of nodes to read and write to
 */
func getNumberNodes() int {
	return privateNumberNodes
}

func setWriteQuorum(writeQuorum int) {
	privateWriteQuorum = writeQuorum
}

/*
getWriteQuorum should be used to determine how many nodes to write to
 */
func getWriteQuorum() int {
	return privateWriteQuorum
}

func setReadQuorum(readQuorum int) {
	privateReadQuorum = readQuorum
}

/*
getReadQuorum should be used to determine how many nodes to read from
*/
func getReadQuorum() int {
	return privateReadQuorum
}
