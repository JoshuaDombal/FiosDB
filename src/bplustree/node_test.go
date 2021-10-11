package bplustree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewLeafNode(t *testing.T) {
	// Arrange
	keys := []string{"a", "b"}
	values := []string{"a", "b"}
	var pageNum int64 = 1

	// Act
	node := NewLeafNode(pageNum, keys, values)

	// Assert
	assert.Equal(t, []string{"a", "b"}, node.Keys)
	assert.Equal(t, []string{"a", "b"}, node.Values)
	assert.Equal(t, pageNum, node.PageNum)
	assert.True(t, node.IsLeaf)
	keys[0] = "c"
	values[0] = "c"
	assert.Equal(t, []string{"a", "b"}, node.Keys)
	assert.Equal(t, []string{"a", "b"}, node.Values)
}

func TestNewInnerNode(t *testing.T) {
	// Arrange
	keys := []string{"a", "b"}
	children := []int64{1, 2, 3}
	var pageNum int64 = 1

	// Act
	node := NewInnerNode(pageNum, keys, children)

	// Assert
	assert.Equal(t, []string{"a", "b"}, node.Keys)
	assert.Equal(t, []int64{1, 2, 3}, node.Children)
	assert.Equal(t, pageNum, node.PageNum)
	assert.False(t, node.IsLeaf)
	keys[0] = "c"
	children[0] = 3
	assert.Equal(t, []string{"a", "b"}, node.Keys)
	assert.Equal(t, []int64{1, 2, 3}, node.Children)
}

func TestInsertKey(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Values:  []string{"a", "b"},
		PageNum: 1,
		IsLeaf:  true,
	}

	// Act
	node.InsertKey("c", 2)

	// Assert
	assert.Equal(t, []string{"a", "b", "c"}, node.Keys)
}

func TestInsertValue(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Values:  []string{"a", "b"},
		PageNum: 1,
		IsLeaf:  true,
	}

	// Act
	node.InsertValue("c", 2)

	// Assert
	assert.Equal(t, []string{"a", "b"}, node.Keys)
	assert.Equal(t, []string{"a", "b", "c"}, node.Values)
}

func TestInsertChild(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Children:  []int64{1, 2, 3},
		PageNum: 1,
		IsLeaf:  false,
	}

	// Act
	node.InsertChild(4, 3)

	// Assert
	assert.Equal(t, []string{"a", "b"}, node.Keys)
	assert.Equal(t, []int64{1, 2, 3, 4}, node.Children)
}

func TestDeleteKey(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Values:  []string{"a", "b"},
		PageNum: 1,
		IsLeaf:  true,
	}

	// Act
	node.DeleteKey(0)

	// Assert
	assert.Equal(t, []string{"b"}, node.Keys)
	assert.Equal(t, []string{"a", "b"}, node.Values)
}

func TestDeleteValue(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Values:  []string{"a", "b"},
		PageNum: 1,
		IsLeaf:  true,
	}

	// Act
	node.DeleteValue(0)

	// Assert
	assert.Equal(t, []string{"a", "b"}, node.Keys)
	assert.Equal(t, []string{"b"}, node.Values)
}

func TestDeleteChild(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Children:  []int64{1, 2, 3},
		PageNum: 1,
		IsLeaf:  false,
	}

	// Act
	node.DeleteChild(0)

	// Assert
	assert.Equal(t, []string{"a", "b"}, node.Keys)
	assert.Equal(t, []int64{2, 3}, node.Children)
}

func TestCanLend(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Children:  []int64{1, 2, 3},
		PageNum: 1,
		IsLeaf:  false,
	}

	// Act/Assert
	assert.True(t, node.CanLend(3))
	assert.False(t, node.CanLend(4))
}

func TestRemoveMaxLeafNode(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Values:  []string{"a", "b"},
		PageNum: 1,
		IsLeaf:  true,
	}

	// Act
	key, value, _ := node.RemoveMax()

	// Assert
	assert.Equal(t, "b", key)
	assert.Equal(t, "b", value)
	assert.Equal(t, []string{"a"}, node.Keys)
	assert.Equal(t, []string{"a"}, node.Values)
}

func TestRemoveMaxInnerNode(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Children:  []int64{1, 2, 3},
		PageNum: 1,
		IsLeaf:  false,
	}

	// Act
	key, _, child := node.RemoveMax()

	// Assert
	assert.Equal(t, "b", key)
	assert.Equal(t, int64(3), child)
	assert.Equal(t, []string{"a"}, node.Keys)
	assert.Equal(t, []int64{1, 2}, node.Children)
}

func TestRemoveMinLeafNode(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Values:  []string{"a", "b"},
		PageNum: 1,
		IsLeaf:  true,
	}

	// Act
	key, value, _ := node.RemoveMin()

	// Assert
	assert.Equal(t, "a", key)
	assert.Equal(t, "a", value)
	assert.Equal(t, []string{"b"}, node.Keys)
	assert.Equal(t, []string{"b"}, node.Values)
}

func TestRemoveMinInnerNode(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Children:  []int64{1, 2, 3},
		PageNum: 1,
		IsLeaf:  false,
	}

	// Act
	key, _, child := node.RemoveMin()

	// Assert
	assert.Equal(t, "a", key)
	assert.Equal(t, int64(1), child)
	assert.Equal(t, []string{"b"}, node.Keys)
	assert.Equal(t, []int64{2, 3}, node.Children)
}

func TestAcceptMaxFromLeftChildLeafNode(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"b", "c"},
		Values:  []string{"b", "c"},
		PageNum: 1,
		IsLeaf:  true,
	}

	// Act
	node.AcceptMaxFromLeftChild("a", "a", -1)

	// Assert
	assert.Equal(t, []string{"a", "b", "c"}, node.Keys)
	assert.Equal(t, []string{"a", "b", "c"}, node.Values)
}

func TestAcceptMaxFromLeftChildInnerNode(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"b", "c"},
		Children:  []int64{2, 3, 4},
		PageNum: 1,
		IsLeaf:  false,
	}

	// Act
	node.AcceptMaxFromLeftChild("a", "", 1)

	// Assert
	assert.Equal(t, []string{"a", "b", "c"}, node.Keys)
	assert.Equal(t, []int64{1, 2, 3, 4}, node.Children)
}

func TestAcceptMinFromRightChildLeafNode(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Values:  []string{"a", "b"},
		PageNum: 1,
		IsLeaf:  true,
	}

	// Act
	node.AcceptMinFromRightChild("c", "c", -1)

	// Assert
	assert.Equal(t, []string{"a", "b", "c"}, node.Keys)
	assert.Equal(t, []string{"a", "b", "c"}, node.Values)
}

func TestAcceptMinFromRightChildInnerNode(t *testing.T) {
	// Arrange
	node := &Node{
		Keys:    []string{"a", "b"},
		Children:  []int64{1, 2, 3},
		PageNum: 1,
		IsLeaf:  false,
	}

	// Act
	node.AcceptMinFromRightChild("c", "", 4)

	// Assert
	assert.Equal(t, []string{"a", "b", "c"}, node.Keys)
	assert.Equal(t, []int64{1, 2, 3, 4}, node.Children)
}
