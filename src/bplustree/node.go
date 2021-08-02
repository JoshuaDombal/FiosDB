package bplustree

func newLeafNode(keys []string, values []string) *node {
	keysCopied := make([]string, len(keys))
	for idx, elt := range keys {
		keysCopied[idx] = elt
	}
	valuesCopied := make([]string, len(values))
	for idx, elt := range values {
		valuesCopied[idx] = elt
	}

	return &node{
		Keys:   keysCopied,
		Values: valuesCopied,
		IsLeaf: true,
	}
}

func newInnerNode(keys []string, children []*node) *node {
	keysCopied := make([]string, len(keys))
	for idx, elt := range keys {
		keysCopied[idx] = elt
	}
	childrenCopied := make([]*node, len(children))
	for idx, elt := range children {
		childrenCopied[idx] = elt
	}

	return &node{
		Keys:     keysCopied,
		Children: childrenCopied,
		IsLeaf:   false,
	}
}

type node struct {
	Keys     []string // Keys of nodes
	Values   []string // values
	Children []*node  // Children
	IsLeaf   bool
}

func (n *node) insertKey(key string, idx int) {
	if idx == len(n.Keys) {
		n.Keys = append(n.Keys, key)
	} else {
		n.Keys = append(n.Keys[:idx+1], n.Keys[idx:]...)
		n.Keys[idx] = key
	}
}

func (n *node) insertValue(value string, idx int) {
	if idx == len(n.Values) {
		n.Values = append(n.Values, value)
	} else {
		n.Values = append(n.Values[:idx+1], n.Values[idx:]...)
		n.Values[idx] = value
	}
}

func (n *node) insertChild(child *node, idx int) {
	if idx == len(n.Children) {
		n.Children = append(n.Children, child)
	} else {
		n.Children = append(n.Children[:idx+1], n.Children[idx:]...)
		n.Children[idx] = child
	}
}

func (n *node) deleteKey(idx int) {
	n.Keys = append(n.Keys[0:idx], n.Keys[idx+1:]...)
}

func (n *node) deleteValue(idx int) {
	n.Values = append(n.Values[0:idx], n.Values[idx+1:]...)
}

func (n *node) deleteChild(idx int) {
	n.Children = append(n.Children[0:idx], n.Children[idx+1:]...)
}

func (n *node) canLend(capacity int) bool {
	return len(n.Keys) > capacity/2
}

func (n *node) getMinKey() string {
	if n.IsLeaf {
		return n.Keys[0]
	} else {
		return n.Children[0].getMinKey()
	}
}

func (n *node) removeMax() (string, string, *node) {
	maxKey := n.Keys[len(n.Keys)-1]
	n.Keys = n.Keys[:len(n.Keys)-1]
	if n.IsLeaf {
		value := n.Values[len(n.Values)-1]
		n.Values = n.Values[:len(n.Values)-1]
		return maxKey, value, nil
	} else {
		child := n.Children[len(n.Children)-1]
		n.Children = n.Children[:len(n.Children)-1]
		return maxKey, "", child
	}
}

func (n *node) removeMin() (string, string, *node) {
	minKeys := n.Keys[0]
	n.Keys = n.Keys[1:]
	if n.IsLeaf {
		value := n.Values[0]
		n.Values = n.Values[1:]
		return minKeys, value, nil
	} else {
		child := n.Children[0]
		n.Children = n.Children[1:]
		return minKeys, "", child
	}
}

func (n *node) acceptMaxFromLeftChild(key string, value string, child *node) {
	n.insertKey(key, 0)
	if n.IsLeaf {
		n.insertValue(value, 0)
	} else {
		n.insertChild(child, 0)
	}
}

func (n *node) acceptMinFromRightChild(key string, value string, child *node) {
	n.insertKey(key, len(n.Keys))
	if n.IsLeaf {
		n.insertValue(value, len(n.Keys))
	} else {
		n.insertChild(child, len(n.Keys))
	}
}
