package newdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInt64(t *testing.T) {
	type suit struct {
		K      int64
		Wanted []byte
	}
	var (
		err  error
		res  int64
		buf  []byte
		vals = []suit{
			{K: 1, Wanted: []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
			{K: 2333, Wanted: []byte{0x1d, 0x9, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
			{K: 0, Wanted: []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
			{K: -2333, Wanted: []byte{0xe3, 0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		}
	)
	for _, val := range vals {
		buf, err = Int64ToRaw(val.K)
		assert.NoError(t, err, "must no error")
		assert.Equal(t, val.Wanted, buf, "byte must equal")
		res, err = ParseInt64(buf)
		assert.NoError(t, err, "must no error")
		assert.Equal(t, val.K, res, "must equal")
	}
}
