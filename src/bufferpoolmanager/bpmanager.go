package bufferpoolmanager

import (
	"../constants"
	writeAheadLog "../wal"
	"github.com/hashicorp/golang-lru"
	"log"
	"os"
)

// Metadata page structure
// +-----------------------------+
// + rootPage (8 bytes)          +
// + freeListStartPage (8 bytes) +
// +                             +
// +-----------------------------+

// Internal node page structure
// +----------------------------------+
// + pageType (2 bytes)               +
// + numKeys  (4 bytes)               +
// + keys (numKeys * 8 bytes)         +
// + children ((numKeys+1) * 8 bytes) +
// +                                  +
// +----------------------------------+

// Leaf node page structure
// +----------------------------------+
// + pageType (2 bytes)               +
// + numKeys  (4 bytes)               +
// + keys (numKeys * 8 bytes)         +
// + value (numKeys * 8 bytes)        +
// +                                  +
// +----------------------------------+

type PageType int64

const META PageType = 1
const INTERNAL PageType = 2
const LEAF PageType = 3

type FreePageListNode struct {
	pageNum int64
	next    *FreePageListNode
}

type BufferPoolManager struct {
	cache        *lru.Cache
	freePageList *FreePageListNode
	dbFile       *os.File
	wal          *writeAheadLog.WAL
}

func New(fileName string, cacheSize int, wal *writeAheadLog.WAL) (*BufferPoolManager, error) {
	dbFile, err := os.OpenFile(fileName, os.O_RDWR, os.ModeAppend)
	if err != nil {
		return nil, err
	}
	cache, err := lru.New(cacheSize)
	if err != nil {
		return nil, err
	}

	return &BufferPoolManager{
		cache:        cache,
		freePageList: nil,
		dbFile:       dbFile,
		wal:          wal,
	}, nil
}

func (bpm *BufferPoolManager) Recover() error {

	committedFrames, err := bpm.wal.Recover()
	if err != nil {
		return err
	}
	for frame := range committedFrames {
		err = bpm.SetPage(frame.PageNum, frame.Data)
		if err != nil {
			return err
		}
	}

	return nil
}


func (bpm *BufferPoolManager) GetPage(pageNum int64) ([]byte, error) {
	// check the cache
	page, ok := bpm.cache.Get(pageNum)
	if ok {
		return page.([]byte), nil
	}

	// check the WAL
	buffer, ok, err := bpm.wal.GetPage(pageNum)
	if err != nil {
		return nil, err
	}
	if ok {
		return buffer, nil
	}

	// read from disk
	buffer = make([]byte, constants.PageSize)
	_, err = bpm.dbFile.ReadAt(buffer, pageNum*constants.PageSize)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (bpm *BufferPoolManager) SetPage(pageNum int64, data []byte) error {
	err := bpm.wal.AddFrame(writeAheadLog.Frame{
		FrameType: writeAheadLog.PUT,
		PageNum:   pageNum,
		Data:      data,
	})
	if err != nil {
		return err
	}

	bpm.cache.Add(pageNum, data)
	return nil
}

func (bpm *BufferPoolManager) DeletePage(pageNum int64) error {
	err := bpm.wal.AddFrame(writeAheadLog.Frame{
		FrameType: writeAheadLog.DELETE,
		PageNum:   pageNum,
	})
	if err != nil {
		return err
	}

	bpm.cache.Remove(pageNum)
	bpm.freePageList = &FreePageListNode{
		pageNum: pageNum,
		next:    bpm.freePageList,
	}
	return nil
}

func (bpm *BufferPoolManager) Commit() error {
	return bpm.wal.AddFrame(writeAheadLog.Frame{
		FrameType: writeAheadLog.COMMIT,
	})
}

func (bpm *BufferPoolManager) GetFreePage() (int64, error) {
	if bpm.freePageList == nil {
		offset, err := bpm.dbFile.Seek(0, 2)
		if err != nil {
			return -1, err
		}
		if offset % constants.PageSize != 0 {
			log.Fatalf("DBFile size is not a multiple of page size")
		}
		return offset / constants.PageSize, nil
	}

	pageNum := bpm.freePageList.pageNum
	// Update the header to point to the new pageList
	// Write the header page out
	bpm.freePageList = bpm.freePageList.next
	return pageNum, nil
}


