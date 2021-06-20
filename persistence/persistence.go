package persistence

import "github.com/go-errors/errors"


type Node struct {
	isLeaf bool // True when leaf node
	minDeg int // Minimum degree of B-tree node
	num int // Number of keys of node
	keys []string // keys of nodes
	children []Node // children
	values []string // values
}

func (n *Node) Get(key string) (string, error) {
	left, right := 0, n.num - 1
	mid := (left + right) / 2
	for left <= right {
		currVal := n.keys[mid]
		if key < currVal { // search left
			right = mid - 1
		} else if key > currVal { // search right
			left = mid + 1
		} else {
			break
		}
	}

	if n.isLeaf {
		if n.keys[mid] == key {
			return n.values[mid], nil
		} else {
			return "", errors.New("Key not found")
		}
	} else if n.keys[mid] >= key {
		return n.children[mid].Get(key)
	} else {
		return n.children[mid + 1].Get(key)
	}
}

func (n *Node) Set(key, value string) {

}

func (n *Node) Delete(key string) {

}
