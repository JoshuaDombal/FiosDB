package bplustree

import (
	"fios-db/src/serialization"
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
	wal           *WAL
	rootPageNum   int64
	freePageStart int64
}

func NewBPM(fileName string, cacheSize int) *BufferPoolManager {
	dbFile, err := os.OpenFile(fileName + ".db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Failure opening file")
	}
	wal := NewWAL(fileName)
	cache, err := lru.New(cacheSize)
	if err != nil {
		log.Fatalf("Failure creating LRU cache")
	}

	bpm := &BufferPoolManager{
		cache:  cache,
		dbFile: dbFile,
		wal:    wal,
	}

	bpm.Recover()

	if fi, _ := bpm.dbFile.Stat(); fi.Size() > PageSize {
		// Successful initialization requires setting up both the metadata page and the root page. This seems like a bit
		// of a hack. maybe we can clean this up
		bpm.readMetadata()
	} else {
		bpm.initializeDbFile(1, -1)
		bpm.Set(&Node{
			Keys:    make([]string, 0),
			Values:  make([]string, 0),
			IsLeaf:  true,
			PageNum: 1,
		})
	}

	return bpm
}

func (bpm *BufferPoolManager) Recover() {
	committedFrames := bpm.wal.ReadAllCommittedFrames()
	for frame := range committedFrames {
		_, err := bpm.dbFile.WriteAt(frame.Data, frame.PageNum * PageSize)
		if err != nil {
			log.Fatalf("Failure writing dbFile")
		}
	}
}

func (bpm *BufferPoolManager) SetRoot(pageNum int64) {
	bpm.setMetadata(pageNum, bpm.freePageStart)
}

func (bpm *BufferPoolManager) GetRoot() *Node {
	return bpm.Get(bpm.rootPageNum)
}

func (bpm *BufferPoolManager) Get(pageNum int64) *Node {
	nodeBytes := bpm.getPage(pageNum)
	pageType := PageType(serialization.BytesToInt16(nodeBytes[:PageTypeSize]))
	if pageType != INTERNAL && pageType != LEAF {
		log.Fatalf("Page is not a leaf or internal node")
	}

	numKeys := serialization.BytesToInt16(nodeBytes[PageTypeSize : PageTypeSize+KeyCountSize])

	keys := make([]string, numKeys)
	keyBytes := nodeBytes[PageTypeSize+KeyCountSize : PageTypeSize+KeyCountSize+KeySize*numKeys]
	var i int16
	for i = 0; i < numKeys; i++ {
		keys[i] = serialization.FixedLengthBytesToString(keyBytes[KeySize*i : KeySize*(i+1)])
	}

	if pageType == INTERNAL {
		children := make([]int64, numKeys+1)
		childrenBytes := nodeBytes[PageTypeSize+KeyCountSize+KeySize*numKeys : PageTypeSize+KeyCountSize+KeySize*numKeys+PageRefSize*(1+numKeys)]
		var i int16
		for i = 0; i < numKeys+1; i++ {
			children[i] = serialization.BytesToInt64(childrenBytes[KeySize*i : KeySize*(i+1)])
		}

		return &Node{
			Keys:     keys,
			Children: children,
			PageNum:  pageNum,
			IsLeaf:   false,
		}
	} else {
		values := make([]string, numKeys)
		valueBytes := nodeBytes[PageTypeSize+KeyCountSize+KeySize*numKeys : PageTypeSize+KeyCountSize+KeySize*numKeys+ValueSize*numKeys]
		var i int16
		for i = 0; i < numKeys; i++ {
			values[i] = serialization.FixedLengthBytesToString(valueBytes[ValueSize*i : ValueSize*(i+1)])
		}

		return &Node{
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
	buffer, ok := bpm.wal.Read(pageNum)
	if ok {
		return buffer
	}

	// read from disk
	buffer = make([]byte, PageSize)
	_, err := bpm.dbFile.ReadAt(buffer, pageNum*PageSize)
	if err != nil {
		log.Fatalf("Failed to read page")
	}

	return buffer
}

func (bpm *BufferPoolManager) Set(node *Node) {
	data := make([]byte, 0)

	var pageType PageType
	if node.IsLeaf {
		pageType = LEAF
	} else {
		pageType = INTERNAL
	}

	data = append(data, serialization.Int16ToBytes(int16(pageType))...)
	data = append(data, serialization.Int16ToBytes(int16(len(node.Keys)))...)
	for _, key := range node.Keys {
		data = append(data, serialization.StringToBytes(key, KeySize)...)
	}

	if node.IsLeaf {
		for _, value := range node.Values {
			data = append(data, serialization.StringToBytes(value, ValueSize)...)
		}
	} else {
		for _, child := range node.Children {
			data = append(data, serialization.Int64ToBytes(child)...)
		}
	}

	data = append(data, make([]byte, PageSize - len(data))...)

	bpm.setPage(node.PageNum, data)
}

func (bpm *BufferPoolManager) setPage(pageNum int64, data []byte) {
	bpm.wal.Append(Frame{
		FrameType: PUT,
		PageNum:   pageNum,
		Data:      data,
	})

	bpm.cache.Add(pageNum, data)
}

func (bpm *BufferPoolManager) DeletePage(pageNum int64) {
	bpm.cache.Remove(pageNum)

	pageBytes := make([]byte, PageSize)
	for idx, pageTypeByte := range serialization.Int16ToBytes(int16(FREE)) {
		pageBytes[idx] = pageTypeByte
	}
	for idx, nextPageByte := range serialization.Int64ToBytes(bpm.freePageStart) {
		pageBytes[idx+PageTypeSize] = nextPageByte
	}

	bpm.wal.Append(Frame{
		FrameType: PUT,
		PageNum:   pageNum,
		Data: pageBytes,
	})
	bpm.setPage(pageNum, pageBytes)
	bpm.setMetadata(bpm.rootPageNum, pageNum)
}

func (bpm *BufferPoolManager) Commit() {
	bpm.wal.Append(Frame{
		FrameType: COMMIT,
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
			_, err = bpm.dbFile.Write(make([]byte, PageSize))
			if err != nil {
				log.Fatalf("Extending file failed")
			}
			offset += PageSize
		}
 		if offset%PageSize != 0 {
			log.Fatalf("DBFile size is not a multiple of page size")
		}
		// extend the file
		_, err = bpm.dbFile.Write(make([]byte, PageSize))
		if err != nil {
			log.Fatalf("Extending file failed")
		}

		return offset / PageSize
	}

	freePageNum := bpm.freePageStart
	pageBytes := bpm.getPage(freePageNum)
	pageType := PageType(serialization.BytesToInt16(pageBytes[:PageTypeSize]))
	if pageType != FREE {
		log.Fatalf("Page is not free")
		return -1
	} else {
		bpm.freePageStart = serialization.BytesToInt64(pageBytes[PageTypeSize : PageTypeSize+PageRefSize])
		bpm.setMetadata(bpm.rootPageNum, bpm.freePageStart)
		return freePageNum
	}
}

func (bpm *BufferPoolManager) initializeDbFile(rootPage, freePageStart int64) {
	metadataBytes := bpm.serializeMetadata(rootPage, freePageStart)
	_, err := bpm.dbFile.WriteAt(metadataBytes, 0)
	if err != nil {
		log.Fatalf("Failure initializing metadata")
	}

	bpm.rootPageNum = rootPage
	bpm.freePageStart = freePageStart

	// create space for root node
	_, err = bpm.dbFile.WriteAt(make([]byte, PageSize), PageSize)
	if err != nil {
		log.Fatalf("Failure initializing root node")
	}
}

func (bpm *BufferPoolManager) setMetadata(rootPage, freePageStart int64) {
	metadataBytes := bpm.serializeMetadata(rootPage, freePageStart)
	bpm.setPage(0, metadataBytes)

	bpm.rootPageNum = rootPage
	bpm.freePageStart = freePageStart
}

func (bpm *BufferPoolManager) readMetadata() {
	metadataBytes := bpm.getPage(0)

	bpm.rootPageNum = serialization.BytesToInt64(metadataBytes[:PageRefSize])
	bpm.freePageStart = serialization.BytesToInt64(metadataBytes[PageRefSize : 2*PageRefSize])
}

func (bpm *BufferPoolManager) serializeMetadata(rootPage, freePageStart int64) []byte {
	metadataBytes := make([]byte, PageSize)
	for idx, rootPageByte := range serialization.Int64ToBytes(rootPage) {
		metadataBytes[idx] = rootPageByte
	}
	for idx, freePageStartByte := range serialization.Int64ToBytes(freePageStart) {
		metadataBytes[idx+PageRefSize] = freePageStartByte
	}

	return metadataBytes
}
