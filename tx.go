package newdb

import "sync/atomic"

var (
	txIDPool uint64
	txL      = log.WithField("name", "tx")
)

// TxID transaction id
type TxID struct {
	ID uint64
}

// NewTxID new one *TxID
func NewTxID() *TxID {
	return &TxID{
		ID: atomic.AddUint64(&txIDPool, 1),
	}
}

// Tx transaction
type Tx struct {
	TxID *TxID
}

// NewTx new Tx with NewTxID
func NewTx() *Tx {
	return &Tx{
		TxID: NewTxID(),
	}
}
