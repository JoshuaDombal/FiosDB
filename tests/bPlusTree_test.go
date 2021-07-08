package bPlusTree_test

import (
	"../src/bplustree"
	"testing"
)

const minDegree int = 1
const maxBlockSize int = 3

func Get_oneValue(t *testing.T) {
	// Arrange
	var bPlusTree = bplustree.New(minDegree, maxBlockSize)

	bPlusTree.Set("key1", "5")

	// Act
	value, _ := bPlusTree.Get("key1")

	// Assert
	if value != "5" {
		t.Errorf("expected '5', actual='%s'", value)
	}
}

func Get_keyDoesNotExist(t *testing.T) {
	// Arrange
	var bPlusTree = bplustree.New(minDegree, maxBlockSize)

	bPlusTree.Set("key1", "5")

	// Act
	value, found := bPlusTree.Get("key2")

	// Assert
	if found {
		t.Errorf("expected 'false', actual='%t'", found)
	}
	if value != "" {
		t.Errorf("expected \"\", actual='%s'", value)
	}
}

func Get_fiveValues(t *testing.T) {
	// Arrange
	var bPlusTree = bplustree.New(minDegree, maxBlockSize)

	bPlusTree.Set("key1", "5")
	bPlusTree.Set("key2", "10")
	bPlusTree.Set("key3", "0")
	bPlusTree.Set("key4", "100")
	bPlusTree.Set("key5", "-5")

	// Act
	value1, _ := bPlusTree.Get("key1")
	value2, _ := bPlusTree.Get("key2")
	value3, _ := bPlusTree.Get("key3")
	value4, _ := bPlusTree.Get("key4")
	value5, _ := bPlusTree.Get("key5")


	// Assert
	if value1 != "5" {
		t.Errorf("expected 'key1', actual='%s'", value1)
	}
	if value2 != "10" {
		t.Errorf("expected 'key1', actual='%s'", value2)
	}
	if value3 != "0" {
		t.Errorf("expected 'key1', actual='%s'", value3)
	}
	if value4 != "100" {
		t.Errorf("expected 'key1', actual='%s'", value4)
	}
	if value5 != "-5" {
		t.Errorf("expected 'key1', actual='%s'", value5)
	}
}


func Get_duplicateKeyDifferentValue(t *testing.T) {
	// Arrange
	var bPlusTree = bplustree.New(minDegree, maxBlockSize)

	bPlusTree.Set("key1", "5")
	bPlusTree.Set("key1", "10")

	// Act
	value, _ := bPlusTree.Get("key1")

	// Assert
	if value != "10" {
		t.Errorf("expected '10', actual='%s'", value)
	}
}