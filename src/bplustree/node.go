package bplustree

type node struct {
	isLeaf       bool     // True when leaf node
	minDeg       int      // Minimum degree of B-tree node
	num          int      // Number of keys of node
	maxBlockSize int      // max number of keys in node
	keys         []string // keys of nodes
	children     []*node  // children
	values       []string // values

	parent *node
	leftSibling *node
	rightSibling *node
}

type setResult struct {
	newLeftNode *node
	newRightNode *node
	parentKey string
	didSplit  bool
}

func (n *node) Get(key string) (string, bool) {
	idx := n.search(key)
	if n.isLeaf {
		if n.keys[idx] == key {
			return n.values[idx], true
		} else {
			return "", false
		}
	} else if key >= n.keys[idx] {
		return n.children[idx + 1].Get(key)
	} else {
		return n.children[idx].Get(key)
	}
}


// if this is a leaf node:
//     if this key already exists:
//         set value and return
//     insert key and value at appropriate position
//     if we have overflowed our block:
//         split block into two
//         return two new blocks and middle key (new blocks include the key)
// else:
//     call Set on appropriate child
//     if child their overflowed:
//         insert new key and children at appropriate positions
//         if we have overflowed our block:
//             split block into two
//             return two new blocks and middle key (new blocks exclude the key)
func (n *node) Set(key, value string) setResult {
	if n.isLeaf {
		idx := n.search(key)
		if n.num > idx && n.keys[idx] == key {
			n.values[idx] = value
			return setResult{
				didSplit:     false,
			}
		} else {
			var newIdx int
			if n.num > idx && n.keys[idx] <= key {
				newIdx = idx + 1
			} else {
				newIdx = idx
			}

			n.keys = n.insertStringAt(n.keys, key, newIdx)
			n.values = n.insertStringAt(n.values, value, newIdx)
			n.num++
		}

		if n.num > n.maxBlockSize {
			// we don't have space, we need to split
			newParentKey := n.keys[len(n.keys) / 2]
			leftKeys := n.splitStringSlice(n.keys, 0, len(n.keys) / 2)
			leftValues := n.splitStringSlice(n.values, 0, len(n.keys) / 2)
			rightKeys := n.splitStringSlice(n.keys, len(n.keys) / 2, len(n.keys))
			rightValues := n.splitStringSlice(n.values, len(n.keys) / 2, len(n.keys))
			leftNode := node{
				isLeaf:   true,
				minDeg:   n.minDeg,
				maxBlockSize: n.maxBlockSize,
				num:      len(leftKeys),
				keys:     leftKeys,
				children: nil,
				values:   leftValues,
				leftSibling: n.leftSibling,
			}
			rightNode := node{
				isLeaf:   true,
				minDeg:   n.minDeg,
				num:      len(rightKeys),
				maxBlockSize: n.maxBlockSize,
				keys:     rightKeys,
				children: nil,
				values:   rightValues,
				leftSibling: &leftNode,
				rightSibling: n.rightSibling,
			}
			leftNode.rightSibling = &rightNode
			return setResult{
				newLeftNode:  &leftNode,
				newRightNode: &rightNode,
				parentKey:    newParentKey,
				didSplit:     true,
			}
		} else {
			return setResult{
				didSplit:     false,
			}
		}
	} else {
		idx := n.search(key)
		var result setResult
		if key >= n.keys[idx] {
			result = n.children[idx + 1].Set(key, value)
		} else {
			result = n.children[idx].Set(key, value)
		}
		if result.didSplit {
			var newIdx int
			if result.parentKey >= n.keys[idx] {
				newIdx = idx + 1
			} else {
				newIdx = idx
			}

			n.keys = n.insertStringAt(n.keys, result.parentKey, newIdx)
			n.children[newIdx] = result.newLeftNode
			n.children = n.insertNodeAt(n.children, result.newRightNode, newIdx + 1)
			n.num++

			if n.num > n.maxBlockSize {
				// we don't have space, we need to split
				newParentKey := n.keys[len(n.keys) / 2]
				leftKeys := n.splitStringSlice(n.keys, 0, len(n.keys) / 2)
				leftChildren :=  n.splitNodeSlice(n.children, 0, len(n.keys) / 2 + 1)
				rightKeys := n.splitStringSlice(n.keys, len(n.keys) / 2 + 1, len(n.keys))
				rightChildren := n.splitNodeSlice(n.children, len(n.children) / 2 + 1, len(n.keys) + 1)
				leftNode := node{
					isLeaf:   false,
					minDeg:   n.minDeg,
					maxBlockSize: n.maxBlockSize,
					num:      len(leftKeys),
					keys:     leftKeys,
					children: leftChildren,
					values:   nil,
					leftSibling: n.leftSibling,
				}
				rightNode := node{
					isLeaf:   false,
					minDeg:   n.minDeg,
					maxBlockSize: n.maxBlockSize,
					num:      len(rightKeys),
					keys:     rightKeys,
					children: rightChildren,
					values:   nil,
					leftSibling: &leftNode,
					rightSibling: n.rightSibling,
				}
				leftNode.rightSibling = &rightNode
				return setResult{
					newLeftNode:  &leftNode,
					newRightNode: &rightNode,
					parentKey:    newParentKey,
					didSplit:     true,
				}
			} else {
				return setResult{
					didSplit:     false,
				}
			}
		} else {
			return setResult{
				didSplit:     false,
			}
		}
	}
}

// func (n *node) Delete(key string) (string, *node) {
// 	if n.isLeaf {
// 		idx := n.search(key)
// 		if n.keys[idx] == key {
// 			for i := idx; i < len(n.keys) - 1; i++ {
// 				n.keys[i] = n.keys[i + 1]
// 				n.values[i] = n.values[i + 1]
// 			}
// 			n.num--
// 		}

// 		if n.num < n.minDeg {
// 			if n.rightSibling != nil && n.rightSibling.num > n.minDeg {
// 				k, v := n.rightSibling.borrowSmallestFromLeaf()
// 				n.keys = append(n.keys, k)
// 				n.values = append(n.values, v)
// 				return "BORROW", nil
// 			} else if n.leftSibling != nil && n.leftSibling.num > n.minDeg {
// 				k, v := n.leftSibling.borrowLargestFromLeaf()
// 				n.keys = n.insertStringAt(n.keys, k, 0)
// 				n.values = n.insertStringAt(n.values, v, 0)
// 				return "BORROW", nil
// 			} else if n.rightSibling != nil {
// 				var keys []string
// 				var values []string
// 				for i, k := range n.keys {
// 					keys = append(keys, k)
// 					values = append(values, n.values[i])
// 				}
// 				for i, k := range n.rightSibling.keys {
// 					keys = append(keys, k)
// 					values = append(values, n.rightSibling.values[i])
// 				}
// 				newBlock := node{
// 					isLeaf:       false,
// 					minDeg:       n.minDeg,
// 					num:          n.num + n.rightSibling.num,
// 					maxBlockSize: n.maxBlockSize,
// 					keys:         keys,
// 					children:     nil,
// 					values:       values,
// 					leftSibling:  n.leftSibling,
// 					rightSibling: n.rightSibling.rightSibling,
// 				}
// 				return "MERGE_RIGHT", &newBlock
// 			} else {
// 				var keys []string
// 				var values []string
// 				for i, k := range n.keys {
// 					keys = append(keys, k)
// 					values = append(values, n.values[i])
// 				}
// 				for i, k := range n.leftSibling.keys {
// 					keys = append(keys, k)
// 					values = append(values, n.leftSibling.values[i])
// 				}
// 				newBlock := node{
// 					isLeaf:       false,
// 					minDeg:       n.minDeg,
// 					num:          n.num + n.leftSibling.num,
// 					maxBlockSize: n.maxBlockSize,
// 					keys:         keys,
// 					children:     nil,
// 					values:       values,
// 					leftSibling:  n.leftSibling.leftSibling,
// 					rightSibling: n.rightSibling,
// 				}
// 				return "MERGE_LEFT", &newBlock
// 			}
// 		} else {
// 			return "", nil
// 		}
// 	} else {
// 		idx := n.search(key)
// 		var status string
// 		var newBlock *node
// 		if key >= n.keys[idx] {
// 			status, newBlock = n.children[idx + 1].Delete(key)
// 		} else {
// 			status, newBlock = n.children[idx].Delete(key)
// 		}

// 		if status == "BORROW" {
// 			if key >= n.keys[idx] {
// 				n.keys[idx] = n.children[idx + 1].smallest()
// 			} else {
// 				n.keys[idx] = n.children[idx].largest()
// 			}
// 			return "", nil
// 		} else if status == "MERGE_RIGHT" {
// 			if key >= n.keys[idx] {
// 				n.keys = append(n.keys[:idx + 1], n.keys[idx + 2:]...)
// 				n.children = append(append(n.children[:idx + 1], newBlock), n.children[idx + 3:]...)
// 			} else {
// 				n.keys = append(n.keys[:idx], n.keys[idx + 1:]...)
// 				n.children = append(append(n.children[:idx], newBlock), n.children[idx + 2:]...)
// 			}
// 			n.num--
// 		} else if status == "MERGE_LEFT" {
// 			if key >= n.keys[idx] {
// 				n.keys = append(n.keys[:idx], n.keys[idx + 1:]...)
// 				n.children = append(append(n.children[:idx], newBlock), n.children[idx + 2:]...)
// 			} else {
// 				n.keys = append(n.keys[:idx - 1], n.keys[idx:]...)
// 				n.children = append(append(n.children[:idx - 1], newBlock), n.children[idx + 1:]...)
// 			}
// 			n.num--
// 		}

// 		if n.num < n.minDeg {
// 			if n.rightSibling != nil && n.rightSibling.num > n.minDeg {
// 				k, c := n.rightSibling.borrowSmallestFromInternal()
// 				parentIdx := n.parent.search(key)
// 				var newKey string
// 				if k >= n.parent.keys[parentIdx] {
// 					newKey = n.parent.keys[parentIdx]
// 				} else {
// 					newKey = n.parent.keys[parentIdx - 1]
// 				}
// 				n.keys = n.insertStringAt(n.keys, newKey, n.num)
// 				n.children = n.insertNodeAt(n.children, c, n.num + 1)
// 				n.parent.keys[parentIdx] = k
// 			} else if n.leftSibling != nil && n.leftSibling.num > n.minDeg {
// 				k, c := n.rightSibling.borrowLargestFromInternal()
// 				parentIdx := n.parent.search(key)
// 				var newKey string
// 				if k >= n.parent.keys[parentIdx] {
// 					newKey = n.parent.keys[parentIdx]
// 				} else {
// 					newKey = n.parent.keys[parentIdx - 1]
// 				}
// 				n.keys = n.insertStringAt(n.keys, newKey, 0)
// 				n.children = n.insertNodeAt(n.children, c, 0)
// 				n.parent.keys[parentIdx] = k
// 			} else if n.leftSibling != nil {
// 				// TODO:
// 			} else if n.rightSibling != nil{

// 			}
// 		}

// 		// if this node contains the key, replace it with the smallest value of right subtree



// 		tempReturnNode := node{
// 			isLeaf:   false,
// 			minDeg:   n.minDeg,
// 			maxBlockSize: n.maxBlockSize,
// 			num:      len(rightKeys),
// 			keys:     rightKeys,
// 			children: rightChildren,
// 			values:   nil,
// 			leftSibling: &leftNode,
// 			rightSibling: n.rightSibling,
// 		}
// 		return "tempReturn", &tempReturnNode
// 	}
// }

func (n *node) smallest() string {
	if n.isLeaf {
		return n.keys[0]
	} else {
		return n.children[0].smallest()
	}
}

func (n *node) largest() string {
	if n.isLeaf {
		return n.keys[n.num - 1]
	} else {
		return n.children[n.num].largest()
	}
}

func (n *node) borrowSmallestFromLeaf() (string, string) {
	key, value := n.keys[0], n.values[0]
	n.keys = n.keys[1:]
	n.values = n.values[1:]
	n.num--
	return key, value
}

func (n *node) borrowLargestFromLeaf() (string, string) {
	key, value := n.keys[n.num - 1], n.values[n.num - 1]
	n.keys = n.keys[:n.num- 1]
	n.values = n.values[1:]
	n.num--
	return key, value
}

func (n *node) borrowSmallestFromInternal() (string, *node) {
	key, child := n.keys[0], n.children[0]
	n.keys = n.keys[1:]
	n.children = n.children[1:]
	n.num--
	return key, child
}

func (n *node) borrowLargestFromInternal() (string, *node) {
	key, child := n.keys[n.num - 1], n.children[n.num - 1]
	n.keys = n.keys[:n.num - 1]
	n.children = n.children[:n.num]
	n.num--
	return key, child
}

// returns resulting index from binary search
func (n *node) search(key string) int {
	left, right := 0, n.num - 1
	var mid int
	for left <= right {
		mid = (left + right) / 2
		currVal := n.keys[mid]
		if key < currVal { // search left
			right = mid - 1
		} else if key > currVal { // search right
			left = mid + 1
		} else {
			break
		}
	}

	return mid
}

func (n *node) insertStringAt(values []string, value string, idx int) []string {
	values = append(values, "")
	copy(values[idx + 1:], values[idx:])
	values[idx] = value
	return values
}

func (n *node) insertNodeAt(values []*node, value *node, idx int) []*node {
	values = append(values, nil)
	copy(values[idx + 1:], values[idx:])
	values[idx] = value
	return values
}

// Create a new slice rather than using go's [:] syntax to be able to append values to end of new sub-slice
// without clobbering values from values not in the sub-slice
func (n *node) splitStringSlice(values []string, startIdx, endIdx int) []string {
	result := make([]string, 0)
	for i := startIdx; i < endIdx; i++ {
		result = append(result, values[i])
	}
	return result
}

// Create a new slice rather than using go's [:] syntax to be able to append values to end of new sub-slice
// without clobbering values from values not in the sub-slice
func (n *node) splitNodeSlice(values []*node, startIdx, endIdx int) []*node {
	result := make([]*node, 0)
	for i := startIdx; i < endIdx; i++ {
		result = append(result, values[i])
	}
	return result
}
