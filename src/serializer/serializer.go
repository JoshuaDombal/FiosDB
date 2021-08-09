package util

import (
	c "../constants"
	"encoding/binary"
	"fmt"
)

func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(i))
	return buf
}

func BytesToInt64(buf []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buf))
}

func Int16ToBytes(i int16) []byte {
	var buf = make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(i))
	return buf
}

func BytesToInt16(buf []byte) int16 {
	return int16(binary.LittleEndian.Uint16(buf))
}

func KeyToBytes(key string) []byte {
	keyAsBytes := make([]byte, c.KeySize)
	for idx, keyByte := range []byte(key) {
		keyAsBytes[idx] = keyByte
	}
	return keyAsBytes
}

func ValueToBytes(value string) []byte {
	valueAsBytes := make([]byte, c.KeySize)
	for idx, keyByte := range []byte(value) {
		valueAsBytes[idx] = keyByte
	}
	return valueAsBytes
}

func BytesToKey(keyAsBytes []byte) string {
	idx := len(keyAsBytes)
	for i, b := range keyAsBytes {
		if b == 0 {
			idx = i
			break
		}
	}
	return fmt.Sprintf("%s", keyAsBytes[:idx])
}

func BytesToValue(valueAsBytes []byte) string {
	idx := len(valueAsBytes)
	for i, b := range valueAsBytes {
		if b == 0 {
			idx = i
			break
		}
	}
	return fmt.Sprintf("%s", valueAsBytes[:idx])
}
