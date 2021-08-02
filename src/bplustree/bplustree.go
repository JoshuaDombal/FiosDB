package bplustree

import (
	"../bufferpoolmanager"
	n "../node"
	writeAheadLog "../wal"
	"sync"
)

// BPlusTree Implementation of a right biased b+ tree
type BPlusTree struct {
	rwLock   *sync.RWMutex
	root     *n.Node
	capacity int
	bpm      *bufferpoolmanager.BufferPoolManager
}

func New(fileName string, capacity, cacheSize int) BPlusTree {
	wal := writeAheadLog.New(fileName + ".log")
	bpm := bufferpoolmanager.New(fileName+".db", cacheSize, wal)
	return BPlusTree{
		root: &n.Node{
			Keys:   make([]string, 0),
			Values: make([]string, 0),
			IsLeaf: true,
		},
		capacity: capacity,
		bpm:      bpm,
	}
}

func (t *BPlusTree) Get(key string) (string, bool) {
	t.rwLock.RLocker()
	defer t.rwLock.RUnlock()
	return t.get(key, t.root)
}

func (t *BPlusTree) get(key string, node *n.Node) (string, bool) {
	if node.IsLeaf {
		i, keyExists := findKeyIndexInLeaf(key, node.Keys)
		if keyExists {
			return node.Values[i], true
		} else {
			return "", false
		}
	} else {
		i := findChildPointerIndex(key, node.Keys)
		return t.get(key, t.bpm.Get(node.Children[i]))
	}
}

func (t *BPlusTree) Set(key, value string) {
	t.rwLock.Lock()
	defer t.rwLock.Unlock()
	newNode, newKey, didSplit := t.set(key, value, t.root)
	if didSplit {
		newRoot := n.NewInnerNode(t.bpm.GetFreePage(), []string{newKey}, []int64{t.root.PageNum, newNode.PageNum})
		t.root = newRoot
	}
	t.bpm.Commit()
}

func (t *BPlusTree) set(key string, value string, node *n.Node) (*n.Node, string, bool) {
	if node.IsLeaf {
		i, found := findKeyIndexInLeaf(key, node.Keys)
		if found {
			node.Values[i] = value
			return nil, "", false
		} else {
			node.InsertKey(key, i)
			node.InsertValue(value, i)
			if len(node.Keys) > t.capacity {
				nn := n.NewLeafNode(t.bpm.GetFreePage(), node.Keys[len(node.Keys)/2:], node.Values[len(node.Values)/2:])
				node.Keys = node.Keys[:len(node.Keys)/2]
				node.Values = node.Values[:len(node.Values)/2]
				t.bpm.Set(node)
				t.bpm.Set(nn)
				return nn, nn.Keys[0], true
			}
			t.bpm.Set(node)
			return nil, "", false
		}
	} else {
		i := findChildPointerIndex(key, node.Keys)
		newNode, newKey, didSplit := t.set(key, value, t.bpm.Get(node.Children[i]))
		if !didSplit {
			return nil, "", false
		}
		node.InsertKey(newKey, i)
		node.InsertChild(newNode.PageNum, i+1)
		if len(node.Keys) > t.capacity {
			nn := n.NewInnerNode(t.bpm.GetFreePage(), node.Keys[len(node.Keys)/2+1:], node.Children[len(node.Children)/2+1:])
			middleKey := node.Keys[len(node.Keys)/2]
			nodeSize := len(node.Keys)
			node.Keys = node.Keys[:nodeSize/2]
			node.Children = node.Children[:nodeSize/2+1]
			t.bpm.Set(node)
			t.bpm.Set(nn)
			return nn, middleKey, true
		}
		t.bpm.Set(node)

		return nil, "", false
	}
}

func (t *BPlusTree) Delete(key string) {
	t.rwLock.Lock()
	defer t.rwLock.Unlock()
	underCapacity := t.delete(key, t.root)
	if underCapacity && !t.root.IsLeaf {
		t.root = t.bpm.Get(t.root.Children[0])
	}
	t.bpm.Commit()
}

// returns whether nodes are underCapacity
func (t *BPlusTree) delete(key string, node *n.Node) bool {
	if node.IsLeaf {
		i, found := findKeyIndexInLeaf(key, node.Keys)
		if !found {
			return false
		}
		node.DeleteKey(i)
		node.DeleteValue(i)
		t.bpm.Set(node)
		return len(node.Keys) < t.capacity/2
	} else {
		i := findChildPointerIndex(key, node.Keys)
		childUnderCapacity := t.delete(key, t.bpm.Get(node.Children[i]))
		if childUnderCapacity {
			if t.canBorrowFromLeft(i, node) {
				leftChild := t.bpm.Get(node.Children[i-1])
				rightChild := t.bpm.Get(node.Children[i])
				k, v, child := leftChild.RemoveMax()
				if node.Keys[i-1] == key {
					rightChild.AcceptMaxFromLeftChild(k, v, child)
				} else {
					rightChild.AcceptMaxFromLeftChild(node.Keys[i-1], v, child)
				}
				node.Keys[i-1] = k
				t.bpm.Set(node)
				t.bpm.Set(leftChild)
				t.bpm.Set(rightChild)
			} else if t.canBorrowFromRight(i, node) {
				leftChild := t.bpm.Get(node.Children[i])
				rightChild := t.bpm.Get(node.Children[i+1])
				k, v, child := rightChild.RemoveMin()
				leftChild.AcceptMinFromRightChild(k, v, child)
				node.Keys[i] = t.getMinKey(rightChild)
				t.bpm.Set(node)
				t.bpm.Set(leftChild)
				t.bpm.Set(rightChild)
			} else if t.canMergeWithLeft(i) {
				leftChild := t.bpm.Get(node.Children[i-1])
				rightChild := t.bpm.Get(node.Children[i])
				if leftChild.IsLeaf {
					leftChild.Keys = append(leftChild.Keys, rightChild.Keys...)
					leftChild.Values = append(leftChild.Values, rightChild.Values...)
				} else {
					leftChild.Keys = append(append(leftChild.Keys, t.getMinKey(rightChild)), rightChild.Keys...)
					leftChild.Children = append(leftChild.Children, rightChild.Children...)
				}
				node.DeleteKey(i - 1)
				node.DeleteChild(i)
				t.bpm.Set(node)
				t.bpm.Set(leftChild)
				t.bpm.Set(rightChild)
			} else {
				leftChild := t.bpm.Get(node.Children[i])
				rightChild := t.bpm.Get(node.Children[i+1])
				if leftChild.IsLeaf {
					leftChild.Keys = append(leftChild.Keys, rightChild.Keys...)
					leftChild.Values = append(leftChild.Values, rightChild.Values...)
				} else {
					leftChild.Keys = append(append(leftChild.Keys, t.getMinKey(rightChild)), rightChild.Keys...)
					leftChild.Children = append(leftChild.Children, rightChild.Children...)
				}
				node.DeleteKey(i)
				node.DeleteChild(i + 1)
				t.bpm.Set(node)
				t.bpm.Set(leftChild)
				t.bpm.Set(rightChild)
			}
		} else if i > 0 && node.Keys[i-1] == key {
			node.Keys[i-1] = t.getMinKey(t.bpm.Get(node.Children[i]))
			t.bpm.Set(node)
		}

		return len(node.Keys) < t.capacity/2
	}
}

func (t *BPlusTree) canBorrowFromLeft(i int, node *n.Node) bool {
	return i > 0 && t.bpm.Get(node.Children[i-1]).CanLend(t.capacity)
}

func (t *BPlusTree) canBorrowFromRight(i int, node *n.Node) bool {
	return i < len(node.Children)-1 && t.bpm.Get(node.Children[i+1]).CanLend(t.capacity)
}

func (t *BPlusTree) canMergeWithLeft(i int) bool {
	return i > 0
}

func (t *BPlusTree) getMinKey(n *n.Node) string {
	if n.IsLeaf {
		return n.Keys[0]
	} else {
		return t.getMinKey(t.bpm.Get(n.Children[0]))
	}
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
