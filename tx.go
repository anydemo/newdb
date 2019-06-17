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
	var id uint64
	var retry = true
	for retry {
		var oID = txIDPool
		id = oID + 1
		retry = !atomic.CompareAndSwapUint64(&txIDPool, oID, id)
		txL.WithField("old_id", oID).WithField("new_id", id).Debugf("retry: %v", retry)
	}
	return &TxID{
		ID: id,
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
