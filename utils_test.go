package newdb

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByte(t *testing.T) {
	buf := make([]byte, 2)
	assert.Equal(t, 2, binary.PutVarint(buf, int64(255)))
	assert.Equal(t, []byte{0xfe, 0x3}, buf)
}
