package main

type RecordId struct {
	filename string
	offset   uint64
}
type Record struct {
	id   RecordId
	line []byte
}
type RecordBatch = []Record

type TailReader interface {
	tail(folder string, offset RecordId) <-chan RecordBatch
}

type FsTailReader struct {
	batchSize uint
}

func (reader FsTailReader) tail(folder string, offset RecordId) <-chan RecordBatch {
  // setup notification over to a channel.
  // get list of unsent files, identify last one
	// send all files, keep last one open.
	// watch channel in a loop:
		// if wrote to last file, read/buffer and on newline, send.
		// if wrote to an already existing file (on our list), err
		// if created new file, assert it'd sort last, then open, read and send until end
	    // if channel is closed, close file and return.
}


