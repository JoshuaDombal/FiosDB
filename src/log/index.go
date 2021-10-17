package log

import (
	"encoding/binary"
	"log"
	"os"
)

const storeOffsetFieldWidthInBytes = 8


type index struct {
	file *os.File
	size int64 // number of records in this log
}

func newIndex(file *os.File) *index {
	fi, err := os.Stat(file.Name())
	if err != nil {
		log.Fatalf("Failure constructing index: %v", err)
	}
	size := fi.Size() / storeOffsetFieldWidthInBytes
	return &index{
		file: file,
		size: size,
	}
}

// Append Appends the store offset to the index file and returns the record index
func (i *index) Append(storeOffset int64) int64 {
	var buf = make([]byte, storeOffsetFieldWidthInBytes)
	binary.LittleEndian.PutUint64(buf, uint64(storeOffset))

	_, err := i.file.Write(buf)
	if err != nil {
		log.Fatalf("Failure writing length of record")
	}

	recordIndex := i.size
	i.size = i.size + 1
	return recordIndex
}


// Takes a record offset and returns the offset of that record in the store
func (i *index) Read(offset int64) int64 {
	recordLenBytes := make([]byte, recordLenFieldWidthInBytes)
	_, err := i.file.ReadAt(recordLenBytes, offset* storeOffsetFieldWidthInBytes)
	if err != nil {
		log.Fatalf("Failure reading length of record")
	}

	return int64(binary.LittleEndian.Uint64(recordLenBytes))
}

func (i *index) Flush() {
	err := i.file.Sync()
	if err != nil {
		log.Fatalf("Failure syncing index file to disk")
	}
}


