package newdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTxID(t *testing.T) {
	txID := NewTxID()
	assert.NotNil(t, txID)
}

func BenchmarkNewTxID(b *testing.B) {
	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				id := NewTxID()
				b.Log(id.ID)
			}
		},
	)
}

func TestNewTx(t *testing.T) {
	// TODO: how to test NewTx
	tx1 := NewTx()
	tx2 := NewTx()
	assert.NotEqual(t, tx1, tx2)
}

func TestPermission_String(t *testing.T) {
	tests := []struct {
		P Permission
		N string
		V int
	}{
		{PermReadOnly, "READ_ONLY", 0},
		{PermReadWrite, "READ_WRITE", 1},
	}
	for _, test := range tests {
		assert.Equal(t, test.N, test.P.String())
		assert.Equal(t, test.V, int(test.P))
	}
}
