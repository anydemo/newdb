package newdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTxID(t *testing.T) {
	txID := NewTxID()
	assert.Equal(t, uint64(1), txID.ID)
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
