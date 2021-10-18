package log

import (
	"fios-db/src/serialization"
	"log"
	"os"
	"sync"
)

const recordLenFieldWidthInBytes = 8

type store struct {
	mu sync.Mutex
	file *os.File
	size int64
}

func newStore(file *os.File) *store {
	fi, err := os.Stat(file.Name())
	if err != nil {
		log.Fatalf("Failure constructing store: %v", err)
	}
	size := fi.Size()
	return &store{
		mu:   sync.Mutex{},
		file: file,
		size: size,
	}
}

func (s *store) Append(data []byte) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, _ = s.file.Seek(0, 2)

	_, err := s.file.Write(serialization.Int64ToBytes(int64(len(data))))
	if err != nil {
		log.Fatalf("Failure writing length of record")
	}

	_, err = s.file.Write(data)
	if err != nil {
		log.Fatalf("Failure writing data")
	}

	recordOffset := s.size
	s.size = s.size + recordLenFieldWidthInBytes + int64(len(data))
	return recordOffset
}

func (s *store) Read(offset int64) []byte {
	recordLenBytes := make([]byte, recordLenFieldWidthInBytes)
	_, err := s.file.ReadAt(recordLenBytes, offset)
	if err != nil {
		log.Fatalf("Failure reading length of record")
	}

	recordLen := serialization.BytesToInt64(recordLenBytes)
	data := make([]byte, recordLen)
	_, err = s.file.ReadAt(data, offset + recordLenFieldWidthInBytes)
	if err != nil {
		log.Fatalf("Failure reading data")
	}
	return data
}

func (s *store) Flush() {
	err := s.file.Sync()
	if err != nil {
		log.Fatalf("Failure syncing store file to disk")
	}
}
