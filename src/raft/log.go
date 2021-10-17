package raft

type Log struct {

}

// TODO: implement and can we make this generic enough to be used in the bplustree?
func NewLog() *Log {
	return &Log{}
}

func (l *Log) Append(term int, command interface{}) int {
	return 1
}

func (l *Log) Put(entry Entry) {

}

func (l *Log) BatchPut(entries []Entry) {

}

func (l *Log) Get(idx int) (Entry, error) {
	return Entry{}, nil
}

func (l *Log) BatchGet(startIdx, endIdx int) []Entry {
	return []Entry{}
}

func (l *Log) GetLatestTerm() int {
	return 1
}

func (l *Log) GetLatestIndex() int {
	return 1
}

func (l *Log) GetTermForIndex(idx int) int {
	return 1
}

