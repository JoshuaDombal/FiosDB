package bplustree

import (
	aol "fios-db/src/log"
	"fios-db/src/serialization"
	"log"
)

// Commit frame structure
// +--------------------------------+
// + frameType (2 bytes)            +
// +                                +
// +--------------------------------+

// Put frame structure
// +--------------------------------+
// + frameType (2 bytes)            +
// + pageData (4096 bytes)          +
// +                                +
// +--------------------------------+

type FrameType int16

const COMMIT FrameType = 1
const PUT FrameType = 2

type Frame struct {
	FrameType FrameType
	PageNum   int64
	Data      []byte
}

// A WAL implements a wrapper around a basic log. Rather than appending raw bytes, a client
// appends Frame objects. Rather than reading at an index in the log, a client retrieves
// page data given a page number.
type WAL struct {
	log             *aol.Log
	committedTxns   map[int64]int64 // PageNum to Data offset in WAL
	uncommittedTxns map[int64]int64 // PageNum to Data offset in WAL
}

func NewWAL(fileName string) *WAL {
	l := aol.NewLog(fileName)

	return &WAL{
		log:             l,
		committedTxns:   map[int64]int64{},
		uncommittedTxns: map[int64]int64{},
	}
}

// Append adds the given frame to the end of the WAL
func (wal *WAL) Append(frame Frame) {
	offset := wal.log.Append(wal.serializeFrame(&frame))

	if frame.FrameType == COMMIT {
		// add all uncommitted transactions to the committed transactions
		for pageNum, off := range wal.uncommittedTxns {
			wal.committedTxns[pageNum] = off
		}
		wal.uncommittedTxns = map[int64]int64{}
		// flush contents to stable storage
		wal.log.Flush()
	} else {
		// add this to the uncommitted transactions
		wal.uncommittedTxns[frame.PageNum] = offset
	}
}

// Read reads the page with the given pageNum out of the WAL
func (wal *WAL) Read(pageNum int64) ([]byte, bool) {
	if offset, ok := wal.uncommittedTxns[pageNum]; ok {
		frameBytes, _:= wal.log.Read(offset)
		return wal.deserializeFrame(frameBytes).Data, true
	}

	if offset, ok := wal.committedTxns[pageNum]; ok {
		frameBytes, _ := wal.log.Read(offset)
		return wal.deserializeFrame(frameBytes).Data, true
	}

	return nil, false
}

// ReadAllCommittedFrames recovers the state of the WAL before crash. It reads all the committed frames in the WAL and
// writes them to the channel. Frames which were not committed are discarded
// Sets the IO offset to the location after the last committed txn
func (wal *WAL) ReadAllCommittedFrames() <-chan *Frame {
	framesChan := make(chan *Frame)

	go func() {
		defer close(framesChan)
		var uncommittedFrames []*Frame

		for i := int64(0); i < wal.log.Size(); i++ {
			frameBytes, _ := wal.log.Read(i)
			frame := wal.deserializeFrame(frameBytes)
			if frame.FrameType == COMMIT {
				for _, uncommittedFrame := range uncommittedFrames {
					framesChan <- uncommittedFrame
				}
				uncommittedFrames = make([]*Frame, 0)
			} else {
				uncommittedFrames = append(uncommittedFrames, frame)
			}
		}
	}()

	return framesChan
}

func (wal *WAL) serializeFrame(f *Frame) []byte {
	buf := make([]byte, 0)
	buf = append(buf, serialization.Int16ToBytes(int16(f.FrameType))...)

	if f.FrameType != 1 {
		buf = append(buf, serialization.Int64ToBytes(f.PageNum)...)
		buf = append(buf, f.Data...)
	}
	return buf
}

func (wal *WAL) deserializeFrame(frameBytes []byte) *Frame {
	frameType := FrameType(serialization.BytesToInt16(frameBytes[0:FrameTypeSize]))
	if frameType == COMMIT {
		return &Frame{
			FrameType: frameType,
		}
	} else if frameType == PUT {
		pageNumber := serialization.BytesToInt64(frameBytes[FrameTypeSize:FrameTypeSize+PageRefSize])
		pageData := frameBytes[FrameTypeSize+PageRefSize:FrameTypeSize+PageRefSize+PageSize]
		return &Frame{
			FrameType: frameType,
			PageNum:   pageNumber,
			Data:      pageData,
		}
	}

	log.Fatalf("Unrecognized frame type: %d", frameType)
	return nil
}
