package node

type Node struct {
	Keys     []string // Keys of nodes
	Values   []string // values
	Children []int64  // Children
	PageNum  int64
	IsLeaf   bool
}

func NewLeafNode(pageNum int64, keys []string, values []string) *Node {
	keysCopied := make([]string, len(keys))
	for idx, elt := range keys {
		keysCopied[idx] = elt
	}
	valuesCopied := make([]string, len(values))
	for idx, elt := range values {
		valuesCopied[idx] = elt
	}

	return &Node{
		Keys:    keysCopied,
		Values:  valuesCopied,
		PageNum: pageNum,
		IsLeaf:  true,
	}
}

func NewInnerNode(pageNum int64, keys []string, children []int64) *Node {
	keysCopied := make([]string, len(keys))
	for idx, elt := range keys {
		keysCopied[idx] = elt
	}
	childrenCopied := make([]int64, len(children))
	for idx, elt := range children {
		childrenCopied[idx] = elt
	}

	return &Node{
		Keys:     keysCopied,
		Children: childrenCopied,
		PageNum:  pageNum,
		IsLeaf:   false,
	}
}

func (n *Node) InsertKey(key string, idx int) {
	if idx == len(n.Keys) {
		n.Keys = append(n.Keys, key)
	} else {
		n.Keys = append(n.Keys[:idx+1], n.Keys[idx:]...)
		n.Keys[idx] = key
	}
}

func (n *Node) InsertValue(value string, idx int) {
	if idx == len(n.Values) {
		n.Values = append(n.Values, value)
	} else {
		n.Values = append(n.Values[:idx+1], n.Values[idx:]...)
		n.Values[idx] = value
	}
}

func (n *Node) InsertChild(child int64, idx int) {
	if idx == len(n.Children) {
		n.Children = append(n.Children, child)
	} else {
		n.Children = append(n.Children[:idx+1], n.Children[idx:]...)
		n.Children[idx] = child
	}
}

func (n *Node) DeleteKey(idx int) {
	n.Keys = append(n.Keys[0:idx], n.Keys[idx+1:]...)
}

func (n *Node) DeleteValue(idx int) {
	n.Values = append(n.Values[0:idx], n.Values[idx+1:]...)
}

func (n *Node) DeleteChild(idx int) {
	n.Children = append(n.Children[0:idx], n.Children[idx+1:]...)
}

func (n *Node) CanLend(capacity int) bool {
	return len(n.Keys) > capacity/2
}

func (n *Node) RemoveMax() (string, string, int64) {
	maxKey := n.Keys[len(n.Keys)-1]
	n.Keys = n.Keys[:len(n.Keys)-1]
	if n.IsLeaf {
		value := n.Values[len(n.Values)-1]
		n.Values = n.Values[:len(n.Values)-1]
		return maxKey, value, -1
	} else {
		child := n.Children[len(n.Children)-1]
		n.Children = n.Children[:len(n.Children)-1]
		return maxKey, "", child
	}
}

func (n *Node) RemoveMin() (string, string, int64) {
	minKeys := n.Keys[0]
	n.Keys = n.Keys[1:]
	if n.IsLeaf {
		value := n.Values[0]
		n.Values = n.Values[1:]
		return minKeys, value, -1
	} else {
		child := n.Children[0]
		n.Children = n.Children[1:]
		return minKeys, "", child
	}
}

func (n *Node) AcceptMaxFromLeftChild(key string, value string, child int64) {
	n.InsertKey(key, 0)
	if n.IsLeaf {
		n.InsertValue(value, 0)
	} else {
		n.InsertChild(child, 0)
	}
}

func (n *Node) AcceptMinFromRightChild(key string, value string, child int64) {
	n.InsertKey(key, len(n.Keys))
	if n.IsLeaf {
		n.InsertValue(value, len(n.Keys))
	} else {
		n.InsertChild(child, len(n.Keys))
	}
}
