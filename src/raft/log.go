package raft

import (
	"encoding/binary"
	aol "fios-db/src/log"
)

// A Log implements a wrapper around a basic log
type Log struct {
	log *aol.Log
}

func NewLog(fileName string) *Log {
	return &Log{
		log: aol.NewLog(fileName),
	}
}

func (l *Log) Append(term int, command []byte) int64 {
	entry := Entry{
		Command: command,
		Term:    term,
	}
	entryBytes := l.serializeEntry(entry)
	idx := l.log.Append(entryBytes)
	l.log.Flush()
	return idx
}

func (l *Log) Put(entry Entry) {
	entrySerialized := l.serializeEntry(entry)
	l.log.Write(entrySerialized, entry.Index)
	l.log.Flush()
}

func (l *Log) BatchPut(entries []Entry) {
	for _, entry := range entries {
		l.Put(entry)
	}
}

func (l *Log) Get(idx int64) (Entry, error) {
	entrySerialized, err := l.log.Read(idx)
	if err != nil {
		return Entry{}, err
	}
	entry := l.deserializeEntry(entrySerialized)
	entry.Index = idx
	return entry, nil
}

func (l *Log) BatchGet(startIdx, endIdx int64) []Entry {
	entries := make([]Entry, 0)
	for i := startIdx; i < endIdx; i++ {
		entry, _ := l.Get(i)
		entries = append(entries, entry)
	}
	return entries
}

func (l *Log) GetLatestTerm() int {
	latestEntry, _ := l.Get(l.log.Size() - 1)
	return latestEntry.Term
}

func (l *Log) GetLatestIndex() int64 {
	return l.log.Size() - 1
}

func (l *Log) GetTermForIndex(idx int64) int {
	latestEntry, _ := l.Get(idx)
	return latestEntry.Term
}

func (l *Log) serializeEntry(entry Entry) []byte {
	var entryBytes = make([]byte, 4)
	binary.LittleEndian.PutUint32(entryBytes, uint32(entry.Term))
	entryBytes = append(entryBytes, entry.Command...)
	return entryBytes
}

func (l *Log) deserializeEntry(entryBytes []byte) Entry {
	term := int(binary.LittleEndian.Uint32(entryBytes[0:4]))
	command := entryBytes[4:]
	return Entry{
		Command: command,
		Term:    term,
	}
}
