package bplustree

// Implementation of a right biased b+ tree
type BPlusTree struct {
	root *node
}

func New(minDeg, maxBlockSize int) BPlusTree {
	return BPlusTree{
		root: &node{
			isLeaf:       true,
			minDeg:       minDeg,
			num:          0,
			maxBlockSize: maxBlockSize,
			keys:         make([]string, 0),
			children:     nil,
			values:       make([]string, 0),
		},
	}
}

func (t *BPlusTree) Get(key string) (string, bool) {
	return t.root.Get(key)
}

func (t *BPlusTree) Set(key, value string) {
	result := t.root.Set(key, value)
	if result.didSplit {
		newRoot := node{
			isLeaf:   false,
			minDeg:   t.root.minDeg,
			num:      1,
			maxBlockSize:  t.root.maxBlockSize,
			keys:         make([]string, 0),
			children:     make([]*node, 0),
			values:       nil,
		}
		newRoot.keys = append(newRoot.keys, result.parentKey)
		newRoot.children = append(newRoot.children, result.newLeftNode)
		newRoot.children = append(newRoot.children, result.newRightNode)
		t.root = &newRoot
	}
}

func (t *BPlusTree) Delete(key string) {
	t.root.Delete(key)
}
