package replication

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
func RequestWriteFile(bucketName string, fileName string, fileContents []byte) {
	// TODO: implement this method
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
	return []byte("TODO")
}
