package bplustree

import (
	"../bufferpoolmanager"
	n "../node"
	"log"
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
	bpm := bufferpoolmanager.New(fileName, cacheSize)
	return BPlusTree{
		rwLock: &sync.RWMutex{},
		root: bpm.GetRoot(),
		capacity: capacity,
		bpm:      bpm,
	}
}

// ValidateTreeStructure is used for debugging. Traverses the tree and does some simple sanity checks
func (t *BPlusTree) ValidateTreeStructure() {
	t.rwLock.RLock()
	defer t.rwLock.RUnlock()
	t.validateTreeStructure("", "", t.root)
}

func (t *BPlusTree) validateTreeStructure(leftParentKey, rightParentKey string, node *n.Node) {
	seenKey := map[string]bool{}
	for _, key := range node.Keys {
		if _, ok := seenKey[key]; ok {
			log.Fatalf("Duplicate key")
		}
		seenKey[key] = true
		if leftParentKey != "" {
			if key < leftParentKey {
				log.Fatalf("Bad structure")
			}
		}
		if rightParentKey != "" {
			if key >= rightParentKey {
				log.Fatalf("Bad structure")
			}
		}
	}
	if !node.IsLeaf {
		for idx, child := range node.Children {
			lpk, rpk := "", ""
			if idx > 0 {
				lpk = node.Keys[idx - 1]
			}
			if idx < len(node.Children) - 1 {
				rpk = node.Keys[idx]
			}

			t.validateTreeStructure(lpk, rpk, t.bpm.Get(child))
		}
	}
}

func (t *BPlusTree) Get(key string) (string, bool) {
	t.rwLock.RLock()
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
		t.bpm.Set(t.root)
		t.bpm.SetRoot(t.root.PageNum)
	}
	t.bpm.Commit()
}

func (t *BPlusTree) set(key string, value string, node *n.Node) (*n.Node, string, bool) {
	if node.IsLeaf {
		i, found := findKeyIndexInLeaf(key, node.Keys)
		if found {
			node.Values[i] = value
			t.bpm.Set(node)
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
			nodeSize := len(node.Keys)
			nn := n.NewInnerNode(t.bpm.GetFreePage(), node.Keys[nodeSize/2+1:], node.Children[nodeSize/2+1:])
			middleKey := node.Keys[len(node.Keys)/2]
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
		oldRootPageNumber := t.root.PageNum
		t.root = t.bpm.Get(t.root.Children[0])
		t.bpm.DeletePage(oldRootPageNumber)
		t.bpm.SetRoot(t.root.PageNum)
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
					minKey := k
					if !rightChild.IsLeaf {
						minKey = t.getMinKey(rightChild)
					}
					rightChild.AcceptMaxFromLeftChild(minKey, v, child)
				} else {
					maxKey := k
					if !rightChild.IsLeaf {
						maxKey = node.Keys[i-1]
					}
					rightChild.AcceptMaxFromLeftChild(maxKey, v, child)
				}
				node.Keys[i-1] = k
				t.bpm.Set(node)
				t.bpm.Set(leftChild)
				t.bpm.Set(rightChild)
			} else if t.canBorrowFromRight(i, node) {
				leftChild := t.bpm.Get(node.Children[i])
				rightChild := t.bpm.Get(node.Children[i+1])
				minKey := t.getMinKey(rightChild)
				_, v, child := rightChild.RemoveMin()
				leftChild.AcceptMinFromRightChild(minKey, v, child)
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
				t.bpm.DeletePage(rightChild.PageNum)
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
				t.bpm.DeletePage(rightChild.PageNum)
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
