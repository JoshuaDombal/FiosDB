package bplustree

import "sync"

// BPlusTree Implementation of a right biased b+ tree
type BPlusTree struct {
	rwLock   *sync.RWMutex
	root     *node
	capacity int
}

func New(capacity int) BPlusTree {
	return BPlusTree{
		root: &node{
			Keys:   make([]string, 0),
			Values: make([]string, 0),
			IsLeaf: true,
		},
		capacity: capacity,
	}
}

func (t *BPlusTree) Get(key string) (string, bool) {
	t.rwLock.RLocker()
	defer t.rwLock.RUnlock()
	return t.get(key, t.root)
}

func (t *BPlusTree) get(key string, node *node) (string, bool) {
	if node.IsLeaf {
		i, keyExists := findKeyIndexInLeaf(key, node.Keys)
		if keyExists {
			return node.Values[i], true
		} else {
			return "", false
		}
	} else {
		i := findChildPointerIndex(key, node.Keys)
		return t.get(key, node.Children[i])
	}
}

func (t *BPlusTree) Set(key, value string) {
	t.rwLock.Lock()
	defer t.rwLock.Unlock()
	newNode, newKey, didSplit := t.set(key, value, t.root)
	if didSplit {
		newRoot := newInnerNode([]string{newKey}, []*node{t.root, newNode})
		t.root = newRoot
	}
}

func (t *BPlusTree) set(key string, value string, node *node) (*node, string, bool) {
	if node.IsLeaf {
		i, found := findKeyIndexInLeaf(key, node.Keys)
		if found {
			node.Values[i] = value
			return nil, "", false
		} else {
			node.insertKey(key, i)
			node.insertValue(value, i)
			if len(node.Keys) > t.capacity {
				nn := newLeafNode(node.Keys[len(node.Keys)/2:], node.Values[len(node.Values)/2:])
				node.Keys = node.Keys[:len(node.Keys)/2]
				node.Values = node.Values[:len(node.Values)/2]
				return nn, nn.Keys[0], true
			}
			return nil, "", false
		}
	} else {
		i := findChildPointerIndex(key, node.Keys)
		newNode, newKey, didSplit := t.set(key, value, node.Children[i])
		if !didSplit {
			return nil, "", false
		}
		node.insertKey(newKey, i)
		node.insertChild(newNode, i+1)
		if len(node.Keys) > t.capacity {
			nn := newInnerNode(node.Keys[len(node.Keys)/2+1:], node.Children[len(node.Children)/2+1:])
			middleKey := node.Keys[len(node.Keys)/2]
			nodeSize := len(node.Keys)
			node.Keys = node.Keys[:nodeSize/2]
			node.Children = node.Children[:nodeSize/2+1]
			return nn, middleKey, true
		}
		return nil, "", false
	}
}

func (t *BPlusTree) Delete(key string) {
	t.rwLock.Lock()
	defer t.rwLock.Unlock()
	underCapacity := t.delete(key, t.root)
	if underCapacity && !t.root.IsLeaf {
		t.root = t.root.Children[0]
	}
}

// returns whether nodes are underCapacity
func (t *BPlusTree) delete(key string, node *node) bool {
	if node.IsLeaf {
		i, found := findKeyIndexInLeaf(key, node.Keys)
		if !found {
			return false
		}
		node.deleteKey(i)
		node.deleteValue(i)
		return len(node.Keys) < t.capacity/2
	} else {
		i := findChildPointerIndex(key, node.Keys)
		childUnderCapacity := t.delete(key, node.Children[i])
		if childUnderCapacity {
			if t.canBorrowFromLeft(i, node) {
				k, v, child := node.Children[i-1].removeMax()
				if node.Keys[i-1] == key {
					node.Children[i].acceptMaxFromLeftChild(k, v, child)
				} else {
					node.Children[i].acceptMaxFromLeftChild(node.Keys[i-1], v, child)
				}
				node.Keys[i-1] = k
			} else if t.canBorrowFromRight(i, node) {
				k, v, child := node.Children[i+1].removeMin()
				node.Children[i].acceptMinFromRightChild(k, v, child)
				node.Keys[i] = node.Children[i+1].getMinKey()
			} else if t.canMergeWithLeft(i) {
				if node.Children[i-1].IsLeaf {
					node.Children[i-1].Keys = append(node.Children[i-1].Keys, node.Children[i].Keys...)
					node.Children[i-1].Values = append(node.Children[i-1].Values, node.Children[i].Values...)
				} else {
					node.Children[i-1].Keys = append(append(node.Children[i-1].Keys, node.Children[i].getMinKey()), node.Children[i].Keys...)
					node.Children[i-1].Children = append(node.Children[i-1].Children, node.Children[i].Children...)
				}
				node.deleteKey(i - 1)
				node.deleteChild(i)
			} else {
				if node.Children[i].IsLeaf {
					node.Children[i].Keys = append(node.Children[i].Keys, node.Children[i+1].Keys...)
					node.Children[i].Values = append(node.Children[i].Values, node.Children[i+1].Values...)
				} else {
					node.Children[i].Keys = append(append(node.Children[i].Keys, node.Children[i+1].getMinKey()), node.Children[i+1].Keys...)
					node.Children[i].Children = append(node.Children[i].Children, node.Children[i+1].Children...)
				}
				node.deleteKey(i)
				node.deleteChild(i + 1)
			}
		} else if i > 0 && node.Keys[i-1] == key {
			node.Keys[i-1] = node.Children[i].getMinKey()
		}

		return len(node.Keys) < t.capacity/2
	}
}

func (t *BPlusTree) canBorrowFromLeft(i int, node *node) bool {
	return i > 0 && node.Children[i-1].canLend(t.capacity)
}

func (t *BPlusTree) canBorrowFromRight(i int, node *node) bool {
	return i < len(node.Children)-1 && node.Children[i+1].canLend(t.capacity)
}

func (t *BPlusTree) canMergeWithLeft(i int) bool {
	return i > 0
}

func findKeyIndexInLeaf(key string, keys []string) (int, bool) {
	index, found := binarySearch(key, keys)
	if found {
		return index, found
	}
	if len(keys) > 0 && key >= keys[index] {
		return index + 1, false
	}
	return index, false
}

func findChildPointerIndex(key string, keys []string) int {
	index, _ := binarySearch(key, keys)
	if key >= keys[index] {
		return index + 1
	}
	return index
}

func binarySearch(key string, keys []string) (int, bool) {
	left, right := 0, len(keys)-1
	var mid int
	for left <= right {
		mid = (left + right) / 2
		currVal := keys[mid]
		if key < currVal { // search left
			right = mid - 1
		} else if key > currVal { // search right
			left = mid + 1
		} else {
			return mid, true
		}
	}
	return mid, false
}
