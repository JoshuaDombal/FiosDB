package serialization

import (
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

func StringToBytes(str string, len int64) []byte {
	strAsBytes := make([]byte, len)
	for idx, keyByte := range []byte(str) {
		strAsBytes[idx] = keyByte
	}
	return strAsBytes
}

func FixedLengthBytesToString(bytes []byte) string {
	idx := len(bytes)
	for i, b := range bytes {
		if b == 0 {
			idx = i
			break
		}
	}
	return fmt.Sprintf("%s", bytes[:idx])
}

