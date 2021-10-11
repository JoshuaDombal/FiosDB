package bplustree


// PageSize is the number of bytes in a page. A page is considered
// the minimum amount of data which can be transferred to and from disk
// Each b tree node should fit within a page
const PageSize = 4096

// PageTypeSize is the number of bytes used to store the type
// of the b tree page (internal/leaf/free)
const PageTypeSize = 2

// FrameTypeSize is the number of bytes used to store the frame type
// in a WAL frame
const FrameTypeSize = 2

// KeyCountSize is the number of bytes used to store the number of keys
// in a block
const KeyCountSize = 2

// KeySize is the number of bytes used to store a key
const KeySize = 8

// ValueSize is the number of bytes used to store a value
const ValueSize = 8

// PageRefSize is the number of bytes used to store a page number
const PageRefSize = 8
