package wal

import (
	"../constants"
	"../serializer"
	"log"
	"os"
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

type WAL struct {
	needsRecovery   bool
	logFile         *os.File
	committedTxns   map[int64]int64 // PageNum to Data offset in WAL
	uncommittedTxns map[int64]int64 // PageNum to Data offset in WAL
}

func New(fileName string) *WAL {
	needsRecovery := false
	if fi, err := os.Stat(fileName); !os.IsNotExist(err) && fi.Size() > 0 {
		needsRecovery = true
	}

	logFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Failure opening file")
	}
	return &WAL{
		needsRecovery:   needsRecovery,
		logFile:         logFile,
		committedTxns:   map[int64]int64{},
		uncommittedTxns: map[int64]int64{},
	}
}

func (wal *WAL) NeedsRecovery() bool {
	return wal.needsRecovery
}

// AddFrame adds the given frame to the end of the WAL
func (wal *WAL) AddFrame(frame Frame) {
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
		// flush contents to stable storage
		err = wal.logFile.Sync()
		if err != nil {
			log.Fatalf("Failure syncing WAL")
		}
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
	if offset, ok := wal.uncommittedTxns[pageNum]; ok {
		buffer := make([]byte, c.PageSize)
		_, err := wal.logFile.ReadAt(buffer, offset)
		if err != nil {
			log.Fatalf("Failure reading frame")
		}

		return buffer, true
	}

	if offset, ok := wal.committedTxns[pageNum]; ok {
		buffer := make([]byte, c.PageSize)
		_, err := wal.logFile.ReadAt(buffer, offset)
		if err != nil {
			log.Fatalf("Failure reading frame")
		}

		return buffer, true
	}

	return nil, false
}

// RecoverAllCommittedFrames recovers the state of the WAL before crash. It reads all the committed frames in the WAL and
// writes them to the channel. Frames which were not committed are discarded
// Sets the IO offset to the location after the last committed txn
func (wal *WAL) RecoverAllCommittedFrames() <-chan *Frame {
	eofOffset, err := wal.logFile.Seek(0, 2)
	if err != nil {
		log.Fatalf("Seek failed")
	}

	framesChan := make(chan *Frame)

	go func () {
		defer close(framesChan)
		var uncommittedFrames []*Frame

		var offset int64 = 0
		var lastCommittedTxnOffset int64 = 0
		for offset < eofOffset {
			frame, bytesRead := wal.readFrame(offset)
			if frame.FrameType == COMMIT {
				for _, uncommittedFrame := range uncommittedFrames {
					framesChan <- uncommittedFrame
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

		wal.needsRecovery = false
	}()

	return framesChan
}


// Clear clears all of the frames out of the WAL. After Checkpointing to the BPlusTree on disk, we no longer
// need these frames. This makes for faster recovery
func (wal *WAL) Clear() {
	fileName := wal.logFile.Name()
	err := os.Remove(fileName)
	if err != nil {
		log.Fatalf("Failure removing file")
	}

	logFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Failure opening file")
	}
	wal.logFile = logFile
}

// reads a single frame starting at offset
// returns the frame and the number of bytes read
func (wal *WAL) readFrame(offset int64) (*Frame, int64) {
	buffer := make([]byte, c.FrameTypeSize)
	_, err := wal.logFile.ReadAt(buffer, offset)
	if err != nil {
		log.Fatalf("Reading frame failed")
	}
	frameType := FrameType(util.BytesToInt16(buffer))
	if frameType == COMMIT {
		return &Frame{
			FrameType: frameType,
		}, c.FrameTypeSize
	} else if frameType == PUT {

		buffer = make([]byte, c.PageRefSize)
		_, err = wal.logFile.ReadAt(buffer, offset+c.FrameTypeSize)
		if err != nil {
			log.Fatalf("Reading frame failed")
		}
		pageNumber := util.BytesToInt64(buffer)

		data := make([]byte, c.PageSize)
		_, err = wal.logFile.ReadAt(data, offset+c.FrameTypeSize+c.PageRefSize)
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
	buf = append(buf, util.Int16ToBytes(int16(f.FrameType))...)

	if f.FrameType != 1 {
		buf = append(buf, util.Int64ToBytes(f.PageNum)...)
		buf = append(buf, f.Data...)
	}
	return buf
}
