package bplustree

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

const TestDir = "./test"
const TestFile = TestDir + "/db"

type Pair struct {
	key string
	value string
}

func init() {
	_ = os.RemoveAll(TestDir)
}

func TestOddCapacityLargeCache(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 100, 5)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	tuplesToInsert := []Pair{{"pp", "pp"}, {"hh", "hh"}, {"n", "n"}, {"x", "x"}, {"jj", "jj"}, {"ff", "ff"}, {"c", "c"}, {"ss", "ss"}, {"mm", "mm"}, {"l", "l"}, {"zz", "zz"}, {"a", "a"}, {"gg", "gg"}, {"j", "j"}, {"u", "u"}, {"ii", "ii"}, {"k", "k"}, {"q", "q"}, {"rr", "rr"}, {"dd", "dd"}, {"v", "v"}, {"nn", "nn"}, {"s", "s"}, {"ee", "ee"}, {"g", "g"}, {"aa", "aa"}, {"xx", "xx"}, {"w", "w"}, {"e", "e"}, {"r", "r"}, {"vv", "vv"}, {"uu", "uu"}, {"i", "i"}, {"oo", "oo"}, {"f", "f"}, {"z", "z"}, {"tt", "tt"}, {"h", "h"}, {"b", "b"}, {"m", "m"}, {"d", "d"}, {"t", "t"}, {"y", "y"}, {"yy", "yy"}, {"cc", "cc"}, {"kk", "kk"}, {"ll", "ll"}, {"p", "p"}, {"ww", "ww"}, {"o", "o"}, {"qq", "qq"}, {"bb", "bb"}}
	tuplesToDelete := []Pair{{"rr", "rr"}, {"ss", "ss"}, {"e", "e"}, {"o", "o"}, {"h", "h"}, {"uu", "uu"}, {"tt", "tt"}, {"yy", "yy"}, {"vv", "vv"}, {"v", "v"}, {"bb", "bb"}, {"jj", "jj"}, {"c", "c"}, {"ee", "ee"}, {"qq", "qq"}, {"ww", "ww"}, {"w", "w"}, {"z", "z"}, {"hh", "hh"}, {"dd", "dd"}, {"ff", "ff"}, {"l", "l"}, {"t", "t"}, {"j", "j"}, {"kk", "kk"}, {"mm", "mm"}, {"nn", "nn"}, {"pp", "pp"}, {"d", "d"}, {"ll", "ll"}, {"b", "b"}, {"m", "m"}, {"zz", "zz"}, {"a", "a"}, {"s", "s"}, {"f", "f"}, {"oo", "oo"}, {"u", "u"}, {"i", "i"}, {"k", "k"}, {"x", "x"}, {"gg", "gg"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"xx", "xx"}, {"g", "g"}, {"q", "q"}, {"cc", "cc"}, {"ii", "ii"}, {"n", "n"}, {"r", "r"}}


	// Act/Assert
	// insert all tuples
	for idx, pair := range tuplesToInsert {
		bpt.Set(pair.key, pair.value)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToInsert[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}
	}

	// delete all tuples
	for idx, pair := range tuplesToDelete {
		bpt.Delete(pair.key)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToDelete[i]
			_, present :=  bpt.Get(p.key)
			assert.False(t, present)
		}
		for i := idx + 1; i < len(tuplesToDelete); i++ {
			p := tuplesToDelete[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}
	}
}

func TestEvenCapacityLargeCache(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 100, 4)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	tuplesToInsert := []Pair{{"d", "d"}, {"nn", "nn"}, {"m", "m"}, {"uu", "uu"}, {"kk", "kk"}, {"s", "s"}, {"t", "t"}, {"jj", "jj"}, {"ff", "ff"}, {"dd", "dd"}, {"x", "x"}, {"ii", "ii"}, {"ww", "ww"}, {"b", "b"}, {"e", "e"}, {"pp", "pp"}, {"l", "l"}, {"gg", "gg"}, {"j", "j"}, {"g", "g"}, {"y", "y"}, {"zz", "zz"}, {"w", "w"}, {"k", "k"}, {"a", "a"}, {"qq", "qq"}, {"hh", "hh"}, {"v", "v"}, {"c", "c"}, {"oo", "oo"}, {"f", "f"}, {"u", "u"}, {"o", "o"}, {"xx", "xx"}, {"q", "q"}, {"i", "i"}, {"ll", "ll"}, {"yy", "yy"}, {"ss", "ss"}, {"ee", "ee"}, {"z", "z"}, {"h", "h"}, {"cc", "cc"}, {"vv", "vv"}, {"aa", "aa"}, {"mm", "mm"}, {"n", "n"}, {"tt", "tt"}, {"r", "r"}, {"p", "p"}, {"bb", "bb"}, {"rr", "rr"},}
	tuplesToDelete := []Pair{{"ee", "ee"}, {"zz", "zz"}, {"r", "r"}, {"t", "t"}, {"g", "g"}, {"k", "k"}, {"o", "o"}, {"tt", "tt"}, {"cc", "cc"}, {"qq", "qq"}, {"rr", "rr"}, {"oo", "oo"}, {"m", "m"}, {"pp", "pp"}, {"xx", "xx"}, {"x", "x"}, {"j", "j"}, {"mm", "mm"}, {"ss", "ss"}, {"l", "l"}, {"q", "q"}, {"z", "z"}, {"gg", "gg"}, {"hh", "hh"}, {"nn", "nn"}, {"kk", "kk"}, {"yy", "yy"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"uu", "uu"}, {"s", "s"}, {"ff", "ff"}, {"h", "h"}, {"e", "e"}, {"jj", "jj"}, {"ll", "ll"}, {"d", "d"}, {"w", "w"}, {"bb", "bb"}, {"vv", "vv"}, {"u", "u"}, {"n", "n"}, {"b", "b"}, {"v", "v"}, {"c", "c"}, {"ii", "ii"}, {"a", "a"}, {"ww", "ww"}, {"f", "f"}, {"dd", "dd"}, {"i", "i"},}

	// Act/Assert
	// insert all tuples
	for idx, pair := range tuplesToInsert {
		bpt.Set(pair.key, pair.value)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToInsert[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}
	}

	// delete all tuples
	for idx, pair := range tuplesToDelete {
		bpt.Delete(pair.key)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToDelete[i]
			_, present :=  bpt.Get(p.key)
			assert.False(t, present)
		}
		for i := idx + 1; i < len(tuplesToDelete); i++ {
			p := tuplesToDelete[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}
	}
}

func TestOddCapacitySmallCache(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 1, 5)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	tuplesToInsert := []Pair{{"pp", "pp"}, {"hh", "hh"}, {"n", "n"}, {"x", "x"}, {"jj", "jj"}, {"ff", "ff"}, {"c", "c"}, {"ss", "ss"}, {"mm", "mm"}, {"l", "l"}, {"zz", "zz"}, {"a", "a"}, {"gg", "gg"}, {"j", "j"}, {"u", "u"}, {"ii", "ii"}, {"k", "k"}, {"q", "q"}, {"rr", "rr"}, {"dd", "dd"}, {"v", "v"}, {"nn", "nn"}, {"s", "s"}, {"ee", "ee"}, {"g", "g"}, {"aa", "aa"}, {"xx", "xx"}, {"w", "w"}, {"e", "e"}, {"r", "r"}, {"vv", "vv"}, {"uu", "uu"}, {"i", "i"}, {"oo", "oo"}, {"f", "f"}, {"z", "z"}, {"tt", "tt"}, {"h", "h"}, {"b", "b"}, {"m", "m"}, {"d", "d"}, {"t", "t"}, {"y", "y"}, {"yy", "yy"}, {"cc", "cc"}, {"kk", "kk"}, {"ll", "ll"}, {"p", "p"}, {"ww", "ww"}, {"o", "o"}, {"qq", "qq"}, {"bb", "bb"}}
	tuplesToDelete := []Pair{{"rr", "rr"}, {"ss", "ss"}, {"e", "e"}, {"o", "o"}, {"h", "h"}, {"uu", "uu"}, {"tt", "tt"}, {"yy", "yy"}, {"vv", "vv"}, {"v", "v"}, {"bb", "bb"}, {"jj", "jj"}, {"c", "c"}, {"ee", "ee"}, {"qq", "qq"}, {"ww", "ww"}, {"w", "w"}, {"z", "z"}, {"hh", "hh"}, {"dd", "dd"}, {"ff", "ff"}, {"l", "l"}, {"t", "t"}, {"j", "j"}, {"kk", "kk"}, {"mm", "mm"}, {"nn", "nn"}, {"pp", "pp"}, {"d", "d"}, {"ll", "ll"}, {"b", "b"}, {"m", "m"}, {"zz", "zz"}, {"a", "a"}, {"s", "s"}, {"f", "f"}, {"oo", "oo"}, {"u", "u"}, {"i", "i"}, {"k", "k"}, {"x", "x"}, {"gg", "gg"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"xx", "xx"}, {"g", "g"}, {"q", "q"}, {"cc", "cc"}, {"ii", "ii"}, {"n", "n"}, {"r", "r"}}


	// Act/Assert
	// insert all tuples
	for idx, pair := range tuplesToInsert {
		bpt.Set(pair.key, pair.value)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToInsert[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}
	}

	// delete all tuples
	for idx, pair := range tuplesToDelete {
		bpt.Delete(pair.key)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToDelete[i]
			_, present :=  bpt.Get(p.key)
			assert.False(t, present)
		}
		for i := idx + 1; i < len(tuplesToDelete); i++ {
			p := tuplesToDelete[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}
	}
}

func TestEvenCapacitySmallCache(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 1, 4)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	tuplesToInsert := []Pair{{"d", "d"}, {"nn", "nn"}, {"m", "m"}, {"uu", "uu"}, {"kk", "kk"}, {"s", "s"}, {"t", "t"}, {"jj", "jj"}, {"ff", "ff"}, {"dd", "dd"}, {"x", "x"}, {"ii", "ii"}, {"ww", "ww"}, {"b", "b"}, {"e", "e"}, {"pp", "pp"}, {"l", "l"}, {"gg", "gg"}, {"j", "j"}, {"g", "g"}, {"y", "y"}, {"zz", "zz"}, {"w", "w"}, {"k", "k"}, {"a", "a"}, {"qq", "qq"}, {"hh", "hh"}, {"v", "v"}, {"c", "c"}, {"oo", "oo"}, {"f", "f"}, {"u", "u"}, {"o", "o"}, {"xx", "xx"}, {"q", "q"}, {"i", "i"}, {"ll", "ll"}, {"yy", "yy"}, {"ss", "ss"}, {"ee", "ee"}, {"z", "z"}, {"h", "h"}, {"cc", "cc"}, {"vv", "vv"}, {"aa", "aa"}, {"mm", "mm"}, {"n", "n"}, {"tt", "tt"}, {"r", "r"}, {"p", "p"}, {"bb", "bb"}, {"rr", "rr"},}
	tuplesToDelete := []Pair{{"ee", "ee"}, {"zz", "zz"}, {"r", "r"}, {"t", "t"}, {"g", "g"}, {"k", "k"}, {"o", "o"}, {"tt", "tt"}, {"cc", "cc"}, {"qq", "qq"}, {"rr", "rr"}, {"oo", "oo"}, {"m", "m"}, {"pp", "pp"}, {"xx", "xx"}, {"x", "x"}, {"j", "j"}, {"mm", "mm"}, {"ss", "ss"}, {"l", "l"}, {"q", "q"}, {"z", "z"}, {"gg", "gg"}, {"hh", "hh"}, {"nn", "nn"}, {"kk", "kk"}, {"yy", "yy"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"uu", "uu"}, {"s", "s"}, {"ff", "ff"}, {"h", "h"}, {"e", "e"}, {"jj", "jj"}, {"ll", "ll"}, {"d", "d"}, {"w", "w"}, {"bb", "bb"}, {"vv", "vv"}, {"u", "u"}, {"n", "n"}, {"b", "b"}, {"v", "v"}, {"c", "c"}, {"ii", "ii"}, {"a", "a"}, {"ww", "ww"}, {"f", "f"}, {"dd", "dd"}, {"i", "i"},}

	// Act/Assert
	// insert all tuples
	for idx, pair := range tuplesToInsert {
		bpt.Set(pair.key, pair.value)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToInsert[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}
	}

	// delete all tuples
	for idx, pair := range tuplesToDelete {
		bpt.Delete(pair.key)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToDelete[i]
			_, present :=  bpt.Get(p.key)
			assert.False(t, present)
		}
		for i := idx + 1; i < len(tuplesToDelete); i++ {
			p := tuplesToDelete[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}
	}
}

func TestDeleteKeyThatDoesNotExist(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 1, 4)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// seed the bplus tree with some data
	tuples := []Pair{{"a", "a"}, {"b", "b"}, {"c", "c"}, {"d", "d"}, {"e", "e"}}
	for _, pair := range tuples {
		bpt.Set(pair.key, pair.value)
	}

	// Act/Assert
	bpt.Delete("f")
	bpt.ValidateTreeStructure()
	// verify other values still exist
	for _, pair := range tuples {
		value, present :=  bpt.Get(pair.key)
		assert.True(t, present)
		assert.Equal(t, pair.value, value)
	}
}

func TestGetKeyThatDoesNotExist(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 1, 4)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// seed the bplus tree with some data
	tuples := []Pair{{"a", "a"}, {"b", "b"}, {"c", "c"}, {"d", "d"}, {"e", "e"}}
	for _, pair := range tuples {
		bpt.Set(pair.key, pair.value)
	}

	// Act/Assert
	_, present :=  bpt.Get("f")
	assert.False(t, present)
	bpt.ValidateTreeStructure()
}

func TestSetKeyThatAlreadyExists(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 1, 4)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// seed the bplus tree with some data
	tuples := []Pair{{"a", "a"}, {"b", "b"}, {"c", "c"}, {"d", "d"}, {"e", "e"}}
	for _, pair := range tuples {
		bpt.Set(pair.key, pair.value)
	}

	// Act/Assert
	bpt.Set("a", "a-new")
	bpt.ValidateTreeStructure()
	for _, pair := range tuples {
		value, present :=  bpt.Get(pair.key)
		assert.True(t, present)
		if pair.key == "a" {
			assert.Equal(t, "a-new", value)
		} else {
			assert.Equal(t, pair.value, value)
		}
	}
}

func TestOddCapacityLargeCacheRebootAfterCrash(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 100, 5)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	tuplesToInsert := []Pair{{"pp", "pp"}, {"hh", "hh"}, {"n", "n"}, {"x", "x"}, {"jj", "jj"}, {"ff", "ff"}, {"c", "c"}, {"ss", "ss"}, {"mm", "mm"}, {"l", "l"}, {"zz", "zz"}, {"a", "a"}, {"gg", "gg"}, {"j", "j"}, {"u", "u"}, {"ii", "ii"}, {"k", "k"}, {"q", "q"}, {"rr", "rr"}, {"dd", "dd"}, {"v", "v"}, {"nn", "nn"}, {"s", "s"}, {"ee", "ee"}, {"g", "g"}, {"aa", "aa"}, {"xx", "xx"}, {"w", "w"}, {"e", "e"}, {"r", "r"}, {"vv", "vv"}, {"uu", "uu"}, {"i", "i"}, {"oo", "oo"}, {"f", "f"}, {"z", "z"}, {"tt", "tt"}, {"h", "h"}, {"b", "b"}, {"m", "m"}, {"d", "d"}, {"t", "t"}, {"y", "y"}, {"yy", "yy"}, {"cc", "cc"}, {"kk", "kk"}, {"ll", "ll"}, {"p", "p"}, {"ww", "ww"}, {"o", "o"}, {"qq", "qq"}, {"bb", "bb"}}
	tuplesToDelete := []Pair{{"rr", "rr"}, {"ss", "ss"}, {"e", "e"}, {"o", "o"}, {"h", "h"}, {"uu", "uu"}, {"tt", "tt"}, {"yy", "yy"}, {"vv", "vv"}, {"v", "v"}, {"bb", "bb"}, {"jj", "jj"}, {"c", "c"}, {"ee", "ee"}, {"qq", "qq"}, {"ww", "ww"}, {"w", "w"}, {"z", "z"}, {"hh", "hh"}, {"dd", "dd"}, {"ff", "ff"}, {"l", "l"}, {"t", "t"}, {"j", "j"}, {"kk", "kk"}, {"mm", "mm"}, {"nn", "nn"}, {"pp", "pp"}, {"d", "d"}, {"ll", "ll"}, {"b", "b"}, {"m", "m"}, {"zz", "zz"}, {"a", "a"}, {"s", "s"}, {"f", "f"}, {"oo", "oo"}, {"u", "u"}, {"i", "i"}, {"k", "k"}, {"x", "x"}, {"gg", "gg"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"xx", "xx"}, {"g", "g"}, {"q", "q"}, {"cc", "cc"}, {"ii", "ii"}, {"n", "n"}, {"r", "r"}}


	// Act/Assert
	// insert all tuples
	for idx, pair := range tuplesToInsert {
		bpt.Set(pair.key, pair.value)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToInsert[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}

		// creating a new btree simulates recovering from a crash
		bpt = NewBPlusTree(TestFile, 100, 5)
	}

	// delete all tuples
	for idx, pair := range tuplesToDelete {
		bpt.Delete(pair.key)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToDelete[i]
			_, present :=  bpt.Get(p.key)
			assert.False(t, present)
		}
		for i := idx + 1; i < len(tuplesToDelete); i++ {
			p := tuplesToDelete[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}

		// creating a new btree simulates recovering from a crash
		bpt = NewBPlusTree(TestFile, 100, 5)
	}
}

func TestEvenCapacityLargeCacheRebootAfterCrash(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 100, 4)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	tuplesToInsert := []Pair{{"d", "d"}, {"nn", "nn"}, {"m", "m"}, {"uu", "uu"}, {"kk", "kk"}, {"s", "s"}, {"t", "t"}, {"jj", "jj"}, {"ff", "ff"}, {"dd", "dd"}, {"x", "x"}, {"ii", "ii"}, {"ww", "ww"}, {"b", "b"}, {"e", "e"}, {"pp", "pp"}, {"l", "l"}, {"gg", "gg"}, {"j", "j"}, {"g", "g"}, {"y", "y"}, {"zz", "zz"}, {"w", "w"}, {"k", "k"}, {"a", "a"}, {"qq", "qq"}, {"hh", "hh"}, {"v", "v"}, {"c", "c"}, {"oo", "oo"}, {"f", "f"}, {"u", "u"}, {"o", "o"}, {"xx", "xx"}, {"q", "q"}, {"i", "i"}, {"ll", "ll"}, {"yy", "yy"}, {"ss", "ss"}, {"ee", "ee"}, {"z", "z"}, {"h", "h"}, {"cc", "cc"}, {"vv", "vv"}, {"aa", "aa"}, {"mm", "mm"}, {"n", "n"}, {"tt", "tt"}, {"r", "r"}, {"p", "p"}, {"bb", "bb"}, {"rr", "rr"},}
	tuplesToDelete := []Pair{{"ee", "ee"}, {"zz", "zz"}, {"r", "r"}, {"t", "t"}, {"g", "g"}, {"k", "k"}, {"o", "o"}, {"tt", "tt"}, {"cc", "cc"}, {"qq", "qq"}, {"rr", "rr"}, {"oo", "oo"}, {"m", "m"}, {"pp", "pp"}, {"xx", "xx"}, {"x", "x"}, {"j", "j"}, {"mm", "mm"}, {"ss", "ss"}, {"l", "l"}, {"q", "q"}, {"z", "z"}, {"gg", "gg"}, {"hh", "hh"}, {"nn", "nn"}, {"kk", "kk"}, {"yy", "yy"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"uu", "uu"}, {"s", "s"}, {"ff", "ff"}, {"h", "h"}, {"e", "e"}, {"jj", "jj"}, {"ll", "ll"}, {"d", "d"}, {"w", "w"}, {"bb", "bb"}, {"vv", "vv"}, {"u", "u"}, {"n", "n"}, {"b", "b"}, {"v", "v"}, {"c", "c"}, {"ii", "ii"}, {"a", "a"}, {"ww", "ww"}, {"f", "f"}, {"dd", "dd"}, {"i", "i"},}

	// Act/Assert
	// insert all tuples
	for idx, pair := range tuplesToInsert {
		bpt.Set(pair.key, pair.value)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToInsert[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}

		// creating a new btree simulates recovering from a crash
		bpt = NewBPlusTree(TestFile, 100, 4)
	}

	// delete all tuples
	for idx, pair := range tuplesToDelete {
		bpt.Delete(pair.key)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToDelete[i]
			_, present :=  bpt.Get(p.key)
			assert.False(t, present)
		}
		for i := idx + 1; i < len(tuplesToDelete); i++ {
			p := tuplesToDelete[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}

		// creating a new btree simulates recovering from a crash
		bpt = NewBPlusTree(TestFile, 100, 4)
	}
}

func TestOddCapacitySmallCacheRebootAfterCrash(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 1, 5)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	tuplesToInsert := []Pair{{"pp", "pp"}, {"hh", "hh"}, {"n", "n"}, {"x", "x"}, {"jj", "jj"}, {"ff", "ff"}, {"c", "c"}, {"ss", "ss"}, {"mm", "mm"}, {"l", "l"}, {"zz", "zz"}, {"a", "a"}, {"gg", "gg"}, {"j", "j"}, {"u", "u"}, {"ii", "ii"}, {"k", "k"}, {"q", "q"}, {"rr", "rr"}, {"dd", "dd"}, {"v", "v"}, {"nn", "nn"}, {"s", "s"}, {"ee", "ee"}, {"g", "g"}, {"aa", "aa"}, {"xx", "xx"}, {"w", "w"}, {"e", "e"}, {"r", "r"}, {"vv", "vv"}, {"uu", "uu"}, {"i", "i"}, {"oo", "oo"}, {"f", "f"}, {"z", "z"}, {"tt", "tt"}, {"h", "h"}, {"b", "b"}, {"m", "m"}, {"d", "d"}, {"t", "t"}, {"y", "y"}, {"yy", "yy"}, {"cc", "cc"}, {"kk", "kk"}, {"ll", "ll"}, {"p", "p"}, {"ww", "ww"}, {"o", "o"}, {"qq", "qq"}, {"bb", "bb"}}
	tuplesToDelete := []Pair{{"rr", "rr"}, {"ss", "ss"}, {"e", "e"}, {"o", "o"}, {"h", "h"}, {"uu", "uu"}, {"tt", "tt"}, {"yy", "yy"}, {"vv", "vv"}, {"v", "v"}, {"bb", "bb"}, {"jj", "jj"}, {"c", "c"}, {"ee", "ee"}, {"qq", "qq"}, {"ww", "ww"}, {"w", "w"}, {"z", "z"}, {"hh", "hh"}, {"dd", "dd"}, {"ff", "ff"}, {"l", "l"}, {"t", "t"}, {"j", "j"}, {"kk", "kk"}, {"mm", "mm"}, {"nn", "nn"}, {"pp", "pp"}, {"d", "d"}, {"ll", "ll"}, {"b", "b"}, {"m", "m"}, {"zz", "zz"}, {"a", "a"}, {"s", "s"}, {"f", "f"}, {"oo", "oo"}, {"u", "u"}, {"i", "i"}, {"k", "k"}, {"x", "x"}, {"gg", "gg"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"xx", "xx"}, {"g", "g"}, {"q", "q"}, {"cc", "cc"}, {"ii", "ii"}, {"n", "n"}, {"r", "r"}}


	// Act/Assert
	// insert all tuples
	for idx, pair := range tuplesToInsert {
		bpt.Set(pair.key, pair.value)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToInsert[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}

		// creating a new btree simulates recovering from a crash
		bpt = NewBPlusTree(TestFile, 1, 5)
	}

	// delete all tuples
	for idx, pair := range tuplesToDelete {
		bpt.Delete(pair.key)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToDelete[i]
			_, present :=  bpt.Get(p.key)
			assert.False(t, present)
		}
		for i := idx + 1; i < len(tuplesToDelete); i++ {
			p := tuplesToDelete[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}

		// creating a new btree simulates recovering from a crash
		bpt = NewBPlusTree(TestFile, 1, 5)
	}
}

func TestEvenCapacitySmallCacheRebootAfterCrash(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 1, 4)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	tuplesToInsert := []Pair{{"d", "d"}, {"nn", "nn"}, {"m", "m"}, {"uu", "uu"}, {"kk", "kk"}, {"s", "s"}, {"t", "t"}, {"jj", "jj"}, {"ff", "ff"}, {"dd", "dd"}, {"x", "x"}, {"ii", "ii"}, {"ww", "ww"}, {"b", "b"}, {"e", "e"}, {"pp", "pp"}, {"l", "l"}, {"gg", "gg"}, {"j", "j"}, {"g", "g"}, {"y", "y"}, {"zz", "zz"}, {"w", "w"}, {"k", "k"}, {"a", "a"}, {"qq", "qq"}, {"hh", "hh"}, {"v", "v"}, {"c", "c"}, {"oo", "oo"}, {"f", "f"}, {"u", "u"}, {"o", "o"}, {"xx", "xx"}, {"q", "q"}, {"i", "i"}, {"ll", "ll"}, {"yy", "yy"}, {"ss", "ss"}, {"ee", "ee"}, {"z", "z"}, {"h", "h"}, {"cc", "cc"}, {"vv", "vv"}, {"aa", "aa"}, {"mm", "mm"}, {"n", "n"}, {"tt", "tt"}, {"r", "r"}, {"p", "p"}, {"bb", "bb"}, {"rr", "rr"},}
	tuplesToDelete := []Pair{{"ee", "ee"}, {"zz", "zz"}, {"r", "r"}, {"t", "t"}, {"g", "g"}, {"k", "k"}, {"o", "o"}, {"tt", "tt"}, {"cc", "cc"}, {"qq", "qq"}, {"rr", "rr"}, {"oo", "oo"}, {"m", "m"}, {"pp", "pp"}, {"xx", "xx"}, {"x", "x"}, {"j", "j"}, {"mm", "mm"}, {"ss", "ss"}, {"l", "l"}, {"q", "q"}, {"z", "z"}, {"gg", "gg"}, {"hh", "hh"}, {"nn", "nn"}, {"kk", "kk"}, {"yy", "yy"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"uu", "uu"}, {"s", "s"}, {"ff", "ff"}, {"h", "h"}, {"e", "e"}, {"jj", "jj"}, {"ll", "ll"}, {"d", "d"}, {"w", "w"}, {"bb", "bb"}, {"vv", "vv"}, {"u", "u"}, {"n", "n"}, {"b", "b"}, {"v", "v"}, {"c", "c"}, {"ii", "ii"}, {"a", "a"}, {"ww", "ww"}, {"f", "f"}, {"dd", "dd"}, {"i", "i"},}

	// Act/Assert
	// insert all tuples
	for idx, pair := range tuplesToInsert {
		bpt.Set(pair.key, pair.value)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToInsert[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}

		// creating a new btree simulates recovering from a crash
		bpt = NewBPlusTree(TestFile, 1, 4)
	}

	// delete all tuples
	for idx, pair := range tuplesToDelete {
		bpt.Delete(pair.key)
		bpt.ValidateTreeStructure()
		for i := 0; i <= idx; i++ {
			p := tuplesToDelete[i]
			_, present :=  bpt.Get(p.key)
			assert.False(t, present)
		}
		for i := idx + 1; i < len(tuplesToDelete); i++ {
			p := tuplesToDelete[i]
			value, present :=  bpt.Get(p.key)
			assert.True(t, present)
			assert.Equal(t, p.value, value)
		}

		// creating a new btree simulates recovering from a crash
		bpt = NewBPlusTree(TestFile, 1, 4)
	}
}

// Sort of like a fuzz test. idea with this test was to throw a large amount of values at the tree with some restarts
// thrown in over multiple iterations and see if it can survive
func TestLargeAmountOfEntriesWithDefaultConfig(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 64, -1)
	defer func() {_ = os.RemoveAll(TestDir)}()
	templatePairs := []Pair{
		{"a", "a"}, {"b", "b"}, {"c", "c"},
		{"d", "d"}, {"e", "e"}, {"f", "f"},
		{"g", "g"}, {"h", "h"}, {"i", "i"},
		{"j", "j"}, {"k", "k"}, {"l", "l"},
		{"m", "m"}, {"n", "n"}, {"o", "o"},
		{"p", "p"}, {"q", "q"}, {"r", "r"},
		{"s", "s"}, {"t", "t"}, {"u", "u"},
		{"v", "v"}, {"w", "w"}, {"x", "x"},
		{"y", "y"}, {"z", "z"},
	}

	tuplesToInsert := make([]Pair, 0)
	for i := 0; i < 10; i++ {
		for _, templatePair := range templatePairs {
			tuplesToInsert = append(tuplesToInsert, Pair{
				key:   fmt.Sprintf("%s%d", templatePair.key, i),
				value: fmt.Sprintf("%s%d", templatePair.value, i),
			})
		}
	}
	tuplesToDelete := make([]Pair, 0)
	for i := 0; i < 10; i++ {
		for _, templatePair := range templatePairs {
			tuplesToDelete = append(tuplesToDelete, Pair{
				key:   fmt.Sprintf("%s%d", templatePair.key, i),
				value: fmt.Sprintf("%s%d", templatePair.value, i),
			})
		}
	}

	// Act/Assert
	for i := 0; i < 4; i++ {
		// insert all tuples
		for idx, pair := range tuplesToInsert {
			bpt.Set(pair.key, pair.value)
			bpt.ValidateTreeStructure()
			for i := 0; i <= idx; i++ {
				p := tuplesToInsert[i]
				value, present :=  bpt.Get(p.key)
				assert.True(t, present)
				assert.Equal(t, p.value, value)
			}
		}

		if i % 3 == 0 {
			// restart the bpt on one of the iterations. Number was chosen randomly
			bpt = NewBPlusTree(TestFile, 64, -1)
		}

		// delete all tuples
		for idx, pair := range tuplesToDelete {
			bpt.Delete(pair.key)
			bpt.ValidateTreeStructure()
			for i := 0; i <= idx; i++ {
				p := tuplesToDelete[i]
				_, present :=  bpt.Get(p.key)
				assert.False(t, present)
			}
			for i := idx + 1; i < len(tuplesToDelete); i++ {
				p := tuplesToDelete[i]
				value, present :=  bpt.Get(p.key)
				assert.True(t, present)
				assert.Equal(t, p.value, value)
			}
		}

		if i % 2 == 0 {
			// restart the bpt on one of the iterations. Number was chosen randomly
			bpt = NewBPlusTree(TestFile, 64, -1)
		}
	}
}

func TestConcurrentAccessSmallCache(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 1, 4)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	templateTuplesToInsert := []Pair{{"d", "d"}, {"nn", "nn"}, {"m", "m"}, {"uu", "uu"}, {"kk", "kk"}, {"s", "s"}, {"t", "t"}, {"jj", "jj"}, {"ff", "ff"}, {"dd", "dd"}, {"x", "x"}, {"ii", "ii"}, {"ww", "ww"}, {"b", "b"}, {"e", "e"}, {"pp", "pp"}, {"l", "l"}, {"gg", "gg"}, {"j", "j"}, {"g", "g"}, {"y", "y"}, {"zz", "zz"}, {"w", "w"}, {"k", "k"}, {"a", "a"}, {"qq", "qq"}, {"hh", "hh"}, {"v", "v"}, {"c", "c"}, {"oo", "oo"}, {"f", "f"}, {"u", "u"}, {"o", "o"}, {"xx", "xx"}, {"q", "q"}, {"i", "i"}, {"ll", "ll"}, {"yy", "yy"}, {"ss", "ss"}, {"ee", "ee"}, {"z", "z"}, {"h", "h"}, {"cc", "cc"}, {"vv", "vv"}, {"aa", "aa"}, {"mm", "mm"}, {"n", "n"}, {"tt", "tt"}, {"r", "r"}, {"p", "p"}, {"bb", "bb"}, {"rr", "rr"},}
	templateTuplesToDelete := []Pair{{"ee", "ee"}, {"zz", "zz"}, {"r", "r"}, {"t", "t"}, {"g", "g"}, {"k", "k"}, {"o", "o"}, {"tt", "tt"}, {"cc", "cc"}, {"qq", "qq"}, {"rr", "rr"}, {"oo", "oo"}, {"m", "m"}, {"pp", "pp"}, {"xx", "xx"}, {"x", "x"}, {"j", "j"}, {"mm", "mm"}, {"ss", "ss"}, {"l", "l"}, {"q", "q"}, {"z", "z"}, {"gg", "gg"}, {"hh", "hh"}, {"nn", "nn"}, {"kk", "kk"}, {"yy", "yy"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"uu", "uu"}, {"s", "s"}, {"ff", "ff"}, {"h", "h"}, {"e", "e"}, {"jj", "jj"}, {"ll", "ll"}, {"d", "d"}, {"w", "w"}, {"bb", "bb"}, {"vv", "vv"}, {"u", "u"}, {"n", "n"}, {"b", "b"}, {"v", "v"}, {"c", "c"}, {"ii", "ii"}, {"a", "a"}, {"ww", "ww"}, {"f", "f"}, {"dd", "dd"}, {"i", "i"},}

	wg := sync.WaitGroup{}
	for threadId := 0; threadId < 5; threadId++ {
		wg.Add(1)
		go func (threadId int) {
			defer wg.Done()

			tuplesToInsert := make([]Pair, 0)
			tuplesToDelete := make([]Pair, 0)
			for _, tuple := range templateTuplesToInsert {
				tuplesToInsert = append(tuplesToInsert, Pair{
					key:   fmt.Sprintf("%s%d", tuple.key, threadId),
					value: fmt.Sprintf("%s%d", tuple.value, threadId),
				})
			}
			for _, tuple := range templateTuplesToDelete {
				tuplesToDelete = append(tuplesToDelete, Pair{
					key:   fmt.Sprintf("%s%d", tuple.key, threadId),
					value: fmt.Sprintf("%s%d", tuple.value, threadId),
				})
			}

			for idx, pair := range tuplesToInsert {
				bpt.Set(pair.key, pair.value)
				bpt.ValidateTreeStructure()
				for i := 0; i <= idx; i++ {
					p := tuplesToInsert[i]
					value, present :=  bpt.Get(p.key)
					assert.True(t, present)
					assert.Equal(t, p.value, value)
				}
			}

			// delete all tuples
			for idx, pair := range tuplesToDelete {
				bpt.Delete(pair.key)
				bpt.ValidateTreeStructure()
				for i := 0; i <= idx; i++ {
					p := tuplesToDelete[i]
					_, present :=  bpt.Get(p.key)
					assert.False(t, present)
				}
				for i := idx + 1; i < len(tuplesToDelete); i++ {
					p := tuplesToDelete[i]
					value, present :=  bpt.Get(p.key)
					assert.True(t, present)
					assert.Equal(t, p.value, value)
				}
			}
		}(threadId)
	}
	wg.Wait()
}

func TestConcurrentAccessLargeCache(t *testing.T) {
	// Arrange
	_ = os.Mkdir(TestDir, 0755)
	bpt := NewBPlusTree(TestFile, 500, 4)
	defer func() {_ = os.RemoveAll(TestDir)}()
	// NOTE: these values were ordered specially to hit all delete and set cases
	templateTuplesToInsert := []Pair{{"d", "d"}, {"nn", "nn"}, {"m", "m"}, {"uu", "uu"}, {"kk", "kk"}, {"s", "s"}, {"t", "t"}, {"jj", "jj"}, {"ff", "ff"}, {"dd", "dd"}, {"x", "x"}, {"ii", "ii"}, {"ww", "ww"}, {"b", "b"}, {"e", "e"}, {"pp", "pp"}, {"l", "l"}, {"gg", "gg"}, {"j", "j"}, {"g", "g"}, {"y", "y"}, {"zz", "zz"}, {"w", "w"}, {"k", "k"}, {"a", "a"}, {"qq", "qq"}, {"hh", "hh"}, {"v", "v"}, {"c", "c"}, {"oo", "oo"}, {"f", "f"}, {"u", "u"}, {"o", "o"}, {"xx", "xx"}, {"q", "q"}, {"i", "i"}, {"ll", "ll"}, {"yy", "yy"}, {"ss", "ss"}, {"ee", "ee"}, {"z", "z"}, {"h", "h"}, {"cc", "cc"}, {"vv", "vv"}, {"aa", "aa"}, {"mm", "mm"}, {"n", "n"}, {"tt", "tt"}, {"r", "r"}, {"p", "p"}, {"bb", "bb"}, {"rr", "rr"},}
	templateTuplesToDelete := []Pair{{"ee", "ee"}, {"zz", "zz"}, {"r", "r"}, {"t", "t"}, {"g", "g"}, {"k", "k"}, {"o", "o"}, {"tt", "tt"}, {"cc", "cc"}, {"qq", "qq"}, {"rr", "rr"}, {"oo", "oo"}, {"m", "m"}, {"pp", "pp"}, {"xx", "xx"}, {"x", "x"}, {"j", "j"}, {"mm", "mm"}, {"ss", "ss"}, {"l", "l"}, {"q", "q"}, {"z", "z"}, {"gg", "gg"}, {"hh", "hh"}, {"nn", "nn"}, {"kk", "kk"}, {"yy", "yy"}, {"p", "p"}, {"aa", "aa"}, {"y", "y"}, {"uu", "uu"}, {"s", "s"}, {"ff", "ff"}, {"h", "h"}, {"e", "e"}, {"jj", "jj"}, {"ll", "ll"}, {"d", "d"}, {"w", "w"}, {"bb", "bb"}, {"vv", "vv"}, {"u", "u"}, {"n", "n"}, {"b", "b"}, {"v", "v"}, {"c", "c"}, {"ii", "ii"}, {"a", "a"}, {"ww", "ww"}, {"f", "f"}, {"dd", "dd"}, {"i", "i"},}

	wg := sync.WaitGroup{}
	for threadId := 0; threadId < 5; threadId++ {
		wg.Add(1)
		go func (threadId int) {
			defer wg.Done()

			tuplesToInsert := make([]Pair, 0)
			tuplesToDelete := make([]Pair, 0)
			for _, tuple := range templateTuplesToInsert {
				tuplesToInsert = append(tuplesToInsert, Pair{
					key:   fmt.Sprintf("%s%d", tuple.key, threadId),
					value: fmt.Sprintf("%s%d", tuple.value, threadId),
				})
			}
			for _, tuple := range templateTuplesToDelete {
				tuplesToDelete = append(tuplesToDelete, Pair{
					key:   fmt.Sprintf("%s%d", tuple.key, threadId),
					value: fmt.Sprintf("%s%d", tuple.value, threadId),
				})
			}

			for idx, pair := range tuplesToInsert {
				bpt.Set(pair.key, pair.value)
				bpt.ValidateTreeStructure()
				for i := 0; i <= idx; i++ {
					p := tuplesToInsert[i]
					value, present :=  bpt.Get(p.key)
					assert.True(t, present)
					assert.Equal(t, p.value, value)
				}
			}

			// delete all tuples
			for idx, pair := range tuplesToDelete {
				bpt.Delete(pair.key)
				bpt.ValidateTreeStructure()
				for i := 0; i <= idx; i++ {
					p := tuplesToDelete[i]
					_, present :=  bpt.Get(p.key)
					assert.False(t, present)
				}
				for i := idx + 1; i < len(tuplesToDelete); i++ {
					p := tuplesToDelete[i]
					value, present :=  bpt.Get(p.key)
					assert.True(t, present)
					assert.Equal(t, p.value, value)
				}
			}
		}(threadId)
	}
	wg.Wait()
}
