package log

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const TestDir = "./test"
const TestFile = TestDir + "/log"

func TestCreateLogFromScratch(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	defer func() {_ = os.RemoveAll(TestDir)}()

	// Act/Assert
	l := NewLog(TestFile)
	assert.Equal(t, int64(0), l.Size())
}

func TestAppend(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	l := NewLog(TestFile)
	defer func() {_ = os.RemoveAll(TestDir)}()
	data := []byte("Hello world")

	// Act/Assert
	offset := l.Append(data)
	assert.Equal(t, int64(0), offset)
	assert.Equal(t, int64(1), l.Size())
}

func TestRead(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	l := NewLog(TestFile)
	defer func() {_ = os.RemoveAll(TestDir)}()
	data1 := []byte("Hello world")
	data2 := []byte("Goodbye world")

	// Act/Assert
	offset1 := l.Append(data1)
	offset2 := l.Append(data2)
	assert.Equal(t, int64(0), offset1)
	assert.Equal(t, int64(1), offset2)
	assert.Equal(t, int64(2), l.Size())
	data1Actual, _ := l.Read(0)
	assert.Equal(t, data1, data1Actual)
	data2Actual, _ := l.Read(1)
	assert.Equal(t, data2, data2Actual)
}

func TestWrite(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	l := NewLog(TestFile)
	defer func() {_ = os.RemoveAll(TestDir)}()
	data1 := []byte("Hello world")
	data2 := []byte("Goodbye world")
	_ = l.Append(data1)

	// Act/Assert
	l.Write(data2, 0)
	assert.Equal(t, int64(1), l.Size())
	data1Actual, _ := l.Read(0)
	assert.Equal(t, data2, data1Actual)
}

func TestCreateLogAfterCrash(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	l1 := NewLog(TestFile)
	defer func() {_ = os.RemoveAll(TestDir)}()
	data1 := []byte("Hello world")
	data2 := []byte("Goodbye world")
	_ = l1.Append(data1)
	_ = l1.Append(data2)
	l1.Flush()

	// Act/Assert
	l2 := NewLog(TestFile)
	assert.Equal(t, int64(2), l2.Size())
}

func TestAppendAfterCrash(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	l1 := NewLog(TestFile)
	defer func() {_ = os.RemoveAll(TestDir)}()
	data1 := []byte("Hello world")
	data2 := []byte("Goodbye world")
	_ = l1.Append(data1)
	l1.Flush()

	// Act/Assert
	l2 := NewLog(TestFile)
	offset := l2.Append(data2)
	assert.Equal(t, int64(1), offset)
	assert.Equal(t, int64(2), l2.Size())
}

func TestReadAfterCrash(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	l1 := NewLog(TestFile)
	defer func() {_ = os.RemoveAll(TestDir)}()
	data1 := []byte("Hello world")
	data2 := []byte("Goodbye world")
	offset1 := l1.Append(data1)
	offset2 := l1.Append(data2)
	l1.Flush()

	// Act/Assert
	l2 := NewLog(TestFile)
	assert.Equal(t, int64(0), offset1)
	assert.Equal(t, int64(1), offset2)
	assert.Equal(t, int64(2), l2.Size())
	data1Actual, _ := l2.Read(0)
	assert.Equal(t, data1, data1Actual)
	data2Actual, _ := l2.Read(1)
	assert.Equal(t, data2, data2Actual)
}

func TestWriteAfterCrash(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	l1 := NewLog(TestFile)
	defer func() {_ = os.RemoveAll(TestDir)}()
	data1 := []byte("Hello world")
	data2 := []byte("Goodbye world")
	_ = l1.Append(data1)

	// Act/Assert
	l2 := NewLog(TestFile)
	l2.Write(data2, 0)
	assert.Equal(t, int64(1), l2.Size())
	data1Actual, _ := l2.Read(0)
	assert.Equal(t, data2, data1Actual)
}
