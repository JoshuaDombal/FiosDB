package bufferpoolmanager

import (
	c "../constants"
	n "../node"
	"../util"
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

// Free page structure
// +---------------------------------------------+
// + pageType (2 bytes)                          +
// + nextFreePage (8 bytes)                      +
// +                                             +
// +---------------------------------------------+

// Internal node page structure
// +---------------------------------------------+
// + pageType (2 bytes)                          +
// + numKeys  (2 bytes)                          +
// + keys (numKeys * 8 bytes)                    +
// + children ((numKeys+1) * 8 bytes)            +
// +                                             +
// +---------------------------------------------+

// Leaf node page structure
// +---------------------------------------------+
// + pageType (2 bytes)                          +
// + numKeys  (2 bytes)                          +
// + keys (numKeys * 8 bytes)                    +
// + value (numKeys * 8 bytes)                   +
// +                                             +
// +---------------------------------------------+

type PageType int16

const INTERNAL PageType = 1
const LEAF PageType = 2
const FREE PageType = 3

type BufferPoolManager struct {
	cache         *lru.Cache
	dbFile        *os.File
	wal           *writeAheadLog.WAL
	rootPageNum   int64
	freePageStart int64
}

func New(fileName string, cacheSize int, wal *writeAheadLog.WAL) *BufferPoolManager {
	dbFile, err := os.OpenFile(fileName, os.O_RDWR, os.ModeAppend)
	if err != nil {
		log.Fatalf("Failure opening file")
	}
	cache, err := lru.New(cacheSize)
	if err != nil {
		log.Fatalf("Failure creating LRU cache")
	}

	return &BufferPoolManager{
		cache:  cache,
		dbFile: dbFile,
		wal:    wal,
	}
}

func (bpm *BufferPoolManager) Recover() {
	// Read pages from WAL
	committedFrames := bpm.wal.Recover()
	for frame := range committedFrames {
		bpm.setPage(frame.PageNum, frame.Data)
	}

	// Read metadata
	bpm.readMetadata()
}

func (bpm *BufferPoolManager) Get(pageNum int64) *n.Node {
	nodeBytes := bpm.getPage(pageNum)
	pageType := PageType(util.BytesToInt16(nodeBytes[:c.PageTypeSize]))
	if pageType != INTERNAL && pageType != LEAF {
		log.Fatalf("Page is not a leaf or internal node")
	}

	numKeys := util.BytesToInt16(nodeBytes[c.PageTypeSize : c.PageTypeSize+c.KeyCountSize])

	keys := make([]string, numKeys)
	keyBytes := nodeBytes[c.PageTypeSize+c.KeyCountSize : c.PageTypeSize+c.KeyCountSize+c.KeySize*numKeys]
	var i int16
	for i = 0; i < numKeys; i++ {
		keys[i] = string(keyBytes[c.KeySize*i : c.KeySize*(i+1)])
	}

	if pageType == INTERNAL {
		children := make([]int64, numKeys+1)
		childrenBytes := nodeBytes[c.PageTypeSize+c.KeyCountSize+c.KeySize*numKeys : c.PageTypeSize+c.KeyCountSize+c.KeySize*numKeys+c.PageRefSize*(1+numKeys)]
		var i int16
		for i = 0; i < numKeys+1; i++ {
			children[i] = util.BytesToInt64(childrenBytes[c.KeySize*i : c.KeySize*(i+1)])
		}

		return &n.Node{
			Keys:     keys,
			Children: children,
			PageNum:  pageNum,
			IsLeaf:   false,
		}
	} else {
		values := make([]string, numKeys)
		valueBytes := nodeBytes[c.PageTypeSize+c.KeyCountSize+c.KeySize*numKeys : c.PageTypeSize+c.KeyCountSize+c.KeySize*numKeys+c.ValueSize*numKeys]
		var i int16
		for i = 0; i < numKeys+1; i++ {
			values[i] = string(valueBytes[c.ValueSize*i : c.ValueSize*(i+1)])
		}

		return &n.Node{
			Keys:    keys,
			Values:  values,
			PageNum: pageNum,
			IsLeaf:  true,
		}
	}
}

func (bpm *BufferPoolManager) getPage(pageNum int64) []byte {
	// check the cache
	page, ok := bpm.cache.Get(pageNum)
	if ok {
		return page.([]byte)
	}

	// check the WAL
	buffer, ok := bpm.wal.GetPage(pageNum)
	if ok {
		return buffer
	}

	// read from disk
	buffer = make([]byte, c.PageSize)
	_, err := bpm.dbFile.ReadAt(buffer, pageNum*c.PageSize)
	if err != nil {
		log.Fatalf("Failed to read page")
	}

	return buffer
}

func (bpm *BufferPoolManager) Set(node *n.Node) {
	data := make([]byte, 0)

	var pageType PageType
	if node.IsLeaf {
		pageType = LEAF
	} else {
		pageType = INTERNAL
	}

	data = append(data, util.Int16ToBytes(int16(pageType))...)
	data = append(data, util.Int16ToBytes(int16(len(node.Keys)))...)
	for _, key := range node.Keys {
		data = append(data, util.KeyToBytes(key)...)
	}

	if node.IsLeaf {
		for _, value := range node.Values {
			data = append(data, util.ValueToBytes(value)...)
		}
	} else {
		for _, child := range node.Children {
			data = append(data, util.Int64ToBytes(child)...)
		}
	}

	bpm.setPage(node.PageNum, data)
}

func (bpm *BufferPoolManager) setPage(pageNum int64, data []byte) {
	bpm.wal.AddFrame(writeAheadLog.Frame{
		FrameType: writeAheadLog.PUT,
		PageNum:   pageNum,
		Data:      data,
	})

	bpm.cache.Add(pageNum, data)
}

func (bpm *BufferPoolManager) DeletePage(pageNum int64) {
	bpm.wal.AddFrame(writeAheadLog.Frame{
		FrameType: writeAheadLog.DELETE,
		PageNum:   pageNum,
	})

	bpm.cache.Remove(pageNum)

	pageBytes := make([]byte, c.PageSize)
	for idx, pageTypeByte := range util.Int16ToBytes(int16(FREE)) {
		pageBytes[idx] = pageTypeByte
	}
	for idx, nexPageByte := range util.Int64ToBytes(bpm.freePageStart) {
		pageBytes[idx+c.PageTypeSize] = nexPageByte
	}

	bpm.setPage(pageNum, pageBytes)
	bpm.setMetadata(bpm.rootPageNum, pageNum)
}

func (bpm *BufferPoolManager) Commit() {
	bpm.wal.AddFrame(writeAheadLog.Frame{
		FrameType: writeAheadLog.COMMIT,
	})
}

func (bpm *BufferPoolManager) GetFreePage() int64 {
	if bpm.freePageStart <= 0 {
		offset, err := bpm.dbFile.Seek(0, 2)
		if err != nil {
			log.Fatalf("Seek failed")
		}
		if offset%c.PageSize != 0 {
			log.Fatalf("DBFile size is not a multiple of page size")
		}
		return offset / c.PageSize
	}

	freePageNum := bpm.freePageStart
	pageBytes := bpm.getPage(freePageNum)
	pageType := PageType(util.BytesToInt16(pageBytes[:c.PageTypeSize]))
	if pageType != FREE {
		log.Fatalf("Page is not free")
		return -1
	} else {
		bpm.freePageStart = util.BytesToInt64(pageBytes[c.PageTypeSize : c.PageTypeSize+c.PageRefSize])
		return freePageNum
	}
}

func (bpm *BufferPoolManager) setMetadata(rootPage, freePageStart int64) {
	metadataBytes := make([]byte, c.PageSize)
	for idx, rootPageByte := range util.Int64ToBytes(rootPage) {
		metadataBytes[idx] = rootPageByte
	}
	for idx, freePageStartByte := range util.Int64ToBytes(freePageStart) {
		metadataBytes[idx+c.PageRefSize] = freePageStartByte
	}

	bpm.setPage(0, metadataBytes)

	bpm.rootPageNum = rootPage
	bpm.freePageStart = freePageStart
}

func (bpm *BufferPoolManager) readMetadata() {
	metadataBytes := bpm.getPage(0)

	bpm.rootPageNum = util.BytesToInt64(metadataBytes[:c.PageRefSize])
	bpm.freePageStart = util.BytesToInt64(metadataBytes[c.PageRefSize : 2*c.PageRefSize])
}
