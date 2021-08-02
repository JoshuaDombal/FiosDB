package wal

import (
	"../constants"
	"../util"
	"log"
	"os"
)

type FrameType int16

const COMMIT FrameType = 1
const DELETE FrameType = 2
const PUT FrameType = 3

type Frame struct {
	FrameType FrameType
	PageNum   int64
	Data      []byte
}

type WAL struct {
	logFile         *os.File
	committedTxns   map[int64]int64 // PageNum to Data offset in WAL
	uncommittedTxns map[int64]int64 // PageNum to Data offset in WAL
}

func New(fileName string) *WAL {
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatalf("Failure opening file")
	}
	return &WAL{
		logFile:         logFile,
		committedTxns:   map[int64]int64{},
		uncommittedTxns: map[int64]int64{},
	}
}

// AddFrame adds the given frame to the end of the WAL
func (wal *WAL) AddFrame(frame Frame) {
	if frame.FrameType == PUT && frame.PageNum < 1 {
		log.Fatalf("PUT frame without page Data")
	}
	if frame.FrameType == PUT && len(frame.Data) != c.PageSize {
		log.Fatalf("Page Data is different from page size")
	}

	_, err := wal.logFile.Write(wal.serializeFrame(&frame))
	if err != nil {
		log.Fatalf("Failure writing frame")
	}

	if frame.FrameType == COMMIT {
		// add all uncommitted transactions to the committed transactions
		for pageNum, offset := range wal.uncommittedTxns {
			wal.committedTxns[pageNum] = offset
		}
		wal.uncommittedTxns = map[int64]int64{}
	} else {
		offset, err := wal.logFile.Seek(0, 2)
		if err != nil {
			log.Fatalf("Seek failed")
		}

		// add this to the uncommitted transactions
		wal.uncommittedTxns[frame.PageNum] = offset - c.PageSize
	}
}

// GetPage reads the page out of the WAL
func (wal *WAL) GetPage(pageNum int64) ([]byte, bool) {
	if offset, ok := wal.committedTxns[pageNum]; ok {
		buffer := make([]byte, c.PageSize)
		_, err := wal.logFile.ReadAt(buffer, offset)
		if err != nil {
			log.Fatalf("Failure reading frame")
		}

		return buffer, true
	}

	if offset, ok := wal.uncommittedTxns[pageNum]; ok {
		buffer := make([]byte, c.PageSize)
		_, err := wal.logFile.ReadAt(buffer, offset)
		if err != nil {
			log.Fatalf("Failure reading frame")
		}

		return buffer, true
	}

	log.Fatalf("Page not found")
	return nil, false
}

// Recover recovers the state of the WAL before crash. It reads all the committed frames in the WAL and
// writes them to the channel. Frames which were not committed are discarded
// It additionally rebuilds the committed frames map and sets the IO offset to the location after the last committed txn
func (wal *WAL) Recover() chan *Frame {
	eofOffset, err := wal.logFile.Seek(0, 2)
	if err != nil {
		log.Fatalf("Seek failed")
	}

	framesChan := make(chan *Frame)
	defer close(framesChan)

	var uncommittedFrames []*Frame

	var offset int64 = 0
	var lastCommittedTxnOffset int64 = 0
	for offset < eofOffset {
		frame, bytesRead := wal.readFrame(offset)
		if frame.FrameType == COMMIT {
			for _, uncommittedFrame := range uncommittedFrames {
				framesChan <- uncommittedFrame
				wal.committedTxns[frame.PageNum] = offset + c.FrameTypeSize + c.PageRefSize
			}
			uncommittedFrames = make([]*Frame, 0)
			lastCommittedTxnOffset = offset
		} else {
			uncommittedFrames = append(uncommittedFrames, frame)
		}
		offset += bytesRead
	}

	// move IO offset to location after last committed txn
	_, err = wal.logFile.Seek(lastCommittedTxnOffset, 0)
	if err != nil {
		log.Fatalf("Seek failed")
	}

	return framesChan
}

// reads a single frame starting at offset
// returns the frame and the number of bytes read
func (wal *WAL) readFrame(offset int64) (*Frame, int64) {
	buffer := make([]byte, c.FrameTypeSize)
	_, err := wal.logFile.ReadAt(buffer, offset)
	if err != nil {
		log.Fatalf("Reading frame failed")
	}
	frameType := FrameType(util.BytesToInt64(buffer))
	if frameType == COMMIT {
		return &Frame{
			FrameType: frameType,
		}, 8
	}

	buffer = make([]byte, c.PageRefSize)
	_, err = wal.logFile.ReadAt(buffer, offset+c.FrameTypeSize)
	if err != nil {
		log.Fatalf("Reading frame failed")
	}
	pageNumber := util.BytesToInt64(buffer)
	if frameType == DELETE {
		return &Frame{
			FrameType: frameType,
			PageNum:   pageNumber,
		}, c.FrameTypeSize + c.PageRefSize
	} else if frameType == PUT {
		data := make([]byte, c.PageSize)
		_, err = wal.logFile.ReadAt(buffer, offset+c.FrameTypeSize+c.PageRefSize)
		if err != nil {
			log.Fatalf("Reading frame failed")
		}

		return &Frame{
			FrameType: frameType,
			PageNum:   pageNumber,
			Data:      data,
		}, c.FrameTypeSize + c.PageRefSize + c.PageSize
	}

	log.Fatalf("Unrecognized frame type: %d", frameType)
	return nil, -1
}

func (wal *WAL) serializeFrame(f *Frame) []byte {
	buf := make([]byte, 0)
	buf = append(buf, util.Int64ToBytes(int64(f.FrameType))...)

	if f.FrameType != 1 {
		buf = append(buf, util.Int64ToBytes(f.PageNum)...)
		buf = append(buf, f.Data...)
	}
	return buf
}
