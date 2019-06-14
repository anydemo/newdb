package newdb

import (
	"bytes"
	"encoding/binary"
)

var (
	// DefaultOrder default binary.LittleEndian
	DefaultOrder = binary.LittleEndian
)

// PutInt64 marshal int64
func PutInt64(buf []byte, num int64) error {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	return binary.Write(buffer, DefaultOrder, num)
}

// ParseInt64 unmarshalint64
func ParseInt64(raw []byte) (int64, error) {
	var num int64
	reader := bytes.NewReader(raw)
	return num, binary.Read(reader, DefaultOrder, &num)
}

// Int64ToRaw int64 to byte
func Int64ToRaw(num int64) ([]byte, error) {
	buf := make([]byte, 8)
	err := PutInt64(buf, num)
	return buf, err
}
