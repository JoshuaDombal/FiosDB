package bufferpoolmanager

import (
	c "../constants"
	n "../node"
	"../util"
	writeAheadLog "../wal"
	"github.com/hashicorp/golang-lru"
	"io"
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

func New(fileName string, cacheSize int) *BufferPoolManager {
	dbFile, err := os.OpenFile(fileName + ".db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Failure opening file")
	}
	wal := writeAheadLog.New(fileName + ".log")
	cache, err := lru.New(cacheSize)
	if err != nil {
		log.Fatalf("Failure creating LRU cache")
	}

	bpm := &BufferPoolManager{
		cache:  cache,
		dbFile: dbFile,
		wal:    wal,
	}

	hadCommittedFrames := false
	if wal.NeedsRecovery() {
		hadCommittedFrames = bpm.Checkpoint()
	}

	if hadCommittedFrames {
		// Read metadata.
		bpm.readMetadata()
	} else {
		// if no frames were committed then the metadata page was not committed either
		bpm.setMetadata(1, -1)
		bpm.Set(&n.Node{
			Keys:    make([]string, 0),
			Values:  make([]string, 0),
			IsLeaf:  true,
			PageNum: bpm.GetFreePage(),
		})
	}

	return bpm
}

func (bpm *BufferPoolManager) Checkpoint() bool {
	committedFrames := bpm.wal.RecoverAllCommittedFrames()
	hadCommittedFrames := false
	for frame := range committedFrames {
		hadCommittedFrames = true
		_, err := bpm.dbFile.WriteAt(frame.Data, frame.PageNum * c.PageSize)
		if err != nil {
			log.Fatalf("Failure writing dbFile")
		}
	}
	bpm.wal.Clear()
	return hadCommittedFrames
}

func (bpm *BufferPoolManager) SetRoot(pageNum int64) {
	bpm.setMetadata(pageNum, bpm.freePageStart)
}

func (bpm *BufferPoolManager) GetRoot() *n.Node {
	return bpm.Get(bpm.rootPageNum)
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
		keys[i] = util.BytesToKey(keyBytes[c.KeySize*i : c.KeySize*(i+1)])
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
		for i = 0; i < numKeys; i++ {
			values[i] = util.BytesToValue(valueBytes[c.ValueSize*i : c.ValueSize*(i+1)])
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

	data = append(data, make([]byte, c.PageSize - len(data))...)

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
	bpm.cache.Remove(pageNum)

	pageBytes := make([]byte, c.PageSize)
	for idx, pageTypeByte := range util.Int16ToBytes(int16(FREE)) {
		pageBytes[idx] = pageTypeByte
	}
	for idx, nextPageByte := range util.Int64ToBytes(bpm.freePageStart) {
		pageBytes[idx+c.PageTypeSize] = nextPageByte
	}

	bpm.wal.AddFrame(writeAheadLog.Frame{
		FrameType: writeAheadLog.PUT,
		PageNum:   pageNum,
		Data: pageBytes,
	})
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
		offset, err := bpm.dbFile.Seek(0, io.SeekEnd)
		if err != nil {
			log.Fatalf("Seek failed")
		}
		if offset == 0 {
			// extend the file
			_, err = bpm.dbFile.Write(make([]byte, c.PageSize))
			if err != nil {
				log.Fatalf("Extending file failed")
			}
			offset += c.PageSize
		}
 		if offset%c.PageSize != 0 {
			log.Fatalf("DBFile size is not a multiple of page size")
		}
		// extend the file
		_, err = bpm.dbFile.Write(make([]byte, c.PageSize))
		if err != nil {
			log.Fatalf("Extending file failed")
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
		log.Printf("Free page num: %d\n", freePageNum)
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
