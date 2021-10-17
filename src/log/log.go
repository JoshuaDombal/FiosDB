package log

import (
	"log"
	"os"
)

type Log struct {
	index *index
	store *store
}


func NewLog(fileName string) *Log {
	indexFile, err := os.OpenFile(fileName + ".index", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Failure opening index file")
	}
	storeFile, err := os.OpenFile(fileName + ".store", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Failure opening store file")
	}

	return &Log{
		index: newIndex(indexFile),
		store: newStore(storeFile),
	}
}

func (l *Log) Append(data []byte) int64 {
	storeOffset := l.store.Append(data)
	return l.index.Append(storeOffset)
}

func (l *Log) Read(offset int64) []byte {
	storeOffset := l.index.Read(offset)
	return l.store.Read(storeOffset)
}

func (l *Log) Size() int64 {
	return l.index.size
}

func (l *Log) Flush() {
	l.store.Flush()
	l.index.Flush()
}
