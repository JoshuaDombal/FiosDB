package serialization

import (
	"fios-db/src/bplustree"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInt64SerializationAndDeserialization(t *testing.T) {
	// Arrange
	var int64s = []int64{-9223372036854775808, 0, 9223372036854775807}

	// Act/Assert
	for _, int64ToSerialize := range int64s {
		int64Serialized := Int64ToBytes(int64ToSerialize)
		assert.Equal(t, len(int64Serialized), 8)

		int64Deserialized := BytesToInt64(int64Serialized)
		assert.Equal(t, int64Deserialized, int64ToSerialize)
	}
}

func TestInt16SerializationAndDeserialization(t *testing.T) {
	// Arrange
	var int16s = []int16{-32768, 0, 32767}

	// Act/Assert
	for _, int16ToSerialize := range int16s {
		int16Serialized := Int16ToBytes(int16ToSerialize)
		assert.Equal(t, len(int16Serialized), 2)

		int16Deserialized := BytesToInt16(int16Serialized)
		assert.Equal(t, int16Deserialized, int16ToSerialize)
	}
}

func TestStringSerializationAndDeserialization(t *testing.T) {
	// Arrange
	var keys = []string{"alpha", "123123", "!#$*(@"}

	// Act/Assert
	for _, key := range keys {
		keySerialized := StringToBytes(key, 8)
		assert.Equal(t, len(keySerialized), bplustree.KeySize)

		keyDeserialized := FixedLengthBytesToString(keySerialized)
		assert.Equal(t, key, keyDeserialized)
	}
}
