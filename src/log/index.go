package log

import (
	"encoding/binary"
	"fios-db/src/serialization"
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
	_, _ = i.file.Seek(0, 2)

	_, err := i.file.Write(serialization.Int64ToBytes(storeOffset))
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

func (i *index) Write(offset int64, storeOffset int64) {
	_, err := i.file.WriteAt(serialization.Int64ToBytes(storeOffset), offset * storeOffsetFieldWidthInBytes)
	if err != nil {
		log.Fatalf("Failure overwriting length of record")
	}
}

func (i *index) Flush() {
	err := i.file.Sync()
	if err != nil {
		log.Fatalf("Failure syncing index file to disk")
	}
}


