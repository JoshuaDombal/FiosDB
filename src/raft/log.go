package raft

type Log struct {

}


func NewLog() *Log {
	return &Log{}
}

func (l *Log) Append(term int, command interface{}) int {

}

func (l *Log) Put(entry Entry) {

}

func (l *Log) BatchPut(entries []Entry) {

}

func (l *Log) Get(idx int) (Entry, error) {

}

func (l *Log) BatchGet(startIdx, endIdx int) []Entry {

}

func (l *Log) GetLatestTerm() int {

}

func (l *Log) GetLatestIndex() int {

}

func (l *Log) GetTermForIndex(idx int) int {

}

