package wal

import (
	"../constants"
	"encoding/binary"
	"fmt"
	"github.com/go-errors/errors"
	"os"
)

type FrameType int64

const COMMIT FrameType = 1
const DELETE FrameType = 2
const PUT FrameType = 3

const FrameTypeLengthInBytes = 8
const PageSizeLengthInBytes = 8

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

func New(fileName string) (*WAL, error) {
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return &WAL{
		logFile:         logFile,
		committedTxns:   map[int64]int64{},
		uncommittedTxns: map[int64]int64{},
	}, nil
}

// AddFrame adds the given frame to the end of the WAL
func (wal *WAL) AddFrame(frame Frame) error {
	if frame.FrameType == PUT && frame.PageNum < 1 {
		return errors.New("PUT frame without page Data")
	}
	if frame.FrameType == PUT && len(frame.Data) != constants.PageSize {
		return errors.New("Page Data is different from page size")
	}

	_, err := wal.logFile.Write(wal.serializeFrame(&frame))
	if err != nil {
		return err
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
			return err
		}

		// add this to the uncommitted transactions
		wal.uncommittedTxns[frame.PageNum] = offset - constants.PageSize
	}

	return nil
}

// GetPage reads the page out of the WAL
func (wal *WAL) GetPage(pageNum int64) ([]byte, bool, error) {
	if offset, ok := wal.committedTxns[pageNum]; ok {
		buffer := make([]byte, constants.PageSize)
		_, err := wal.logFile.ReadAt(buffer, offset)
		if err != nil {
			return nil, false, err
		}

		return buffer, true, nil
	}

	if offset, ok := wal.uncommittedTxns[pageNum]; ok {
		buffer := make([]byte, constants.PageSize)
		_, err := wal.logFile.ReadAt(buffer, offset)
		if err != nil {
			return nil, false, err
		}

		return buffer, true, nil
	}

	return nil, false, nil
}

// Recover recovers the state of the WAL before crash. It reads all the committed frames in the WAL and
// writes them to the channel. Frames which were not committed are discarded
// It additionally rebuilds the committed frames map and sets the IO offset to the location after the last committed txn
func (wal *WAL) Recover() (chan *Frame, error) {
	eofOffset, err := wal.logFile.Seek(0, 2)
	if err != nil {
		return nil, err
	}

	framesChan := make(chan *Frame)
	defer close(framesChan)

	var uncommittedFrames []*Frame

	var offset int64 = 0
	var lastCommittedTxnOffset int64 = 0
	for offset < eofOffset {
		frame, bytesRead, err := wal.readFrame(offset)
		if err != nil {
			return nil, err
		}

		if frame.FrameType == COMMIT {
			for _, uncommittedFrame := range uncommittedFrames {
				framesChan <- uncommittedFrame
				wal.committedTxns[frame.PageNum] = offset + FrameTypeLengthInBytes + PageSizeLengthInBytes
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
		return nil, err
	}

	return framesChan, nil
}

// reads a single frame starting at offset
// returns the frame and the number of bytes read
func (wal *WAL) readFrame(offset int64) (*Frame, int64, error) {
	buffer := make([]byte, FrameTypeLengthInBytes)
	_, err := wal.logFile.ReadAt(buffer, offset)
	if err != nil {
		return nil, 0, err
	}
	frameType := FrameType(wal.bytesToInt64(buffer))
	if frameType == COMMIT {
		return &Frame{
			FrameType: frameType,
		}, 8, nil
	}

	buffer = make([]byte, PageSizeLengthInBytes)
	_, err = wal.logFile.ReadAt(buffer, offset+FrameTypeLengthInBytes)
	if err != nil {
		return nil, 0, err
	}
	pageNumber := wal.bytesToInt64(buffer)
	if frameType == DELETE {
		return &Frame{
			FrameType: frameType,
			PageNum:   pageNumber,
		}, FrameTypeLengthInBytes + PageSizeLengthInBytes, nil
	} else if frameType == PUT {
		data := make([]byte, constants.PageSize)
		_, err = wal.logFile.ReadAt(buffer, offset+FrameTypeLengthInBytes+PageSizeLengthInBytes)
		if err != nil {
			return nil, 0, err
		}

		return &Frame{
			FrameType: frameType,
			PageNum:   pageNumber,
			Data:      data,
		}, FrameTypeLengthInBytes + PageSizeLengthInBytes + constants.PageSize, nil
	}

	return nil, -1, errors.New(fmt.Sprintf("Unrecognized frame type: %d", frameType))
}

func (wal *WAL) serializeFrame(f *Frame) []byte {
	buf := make([]byte, 0)
	buf = append(buf, wal.int64ToBytes(int64(f.FrameType))...)

	if f.FrameType != 1 {
		buf = append(buf, wal.int64ToBytes(f.PageNum)...)
		buf = append(buf, f.Data...)
	}
	return buf
}

func (wal *WAL) int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(i))
	return buf
}

func (wal *WAL) bytesToInt64(buf []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buf))
}
