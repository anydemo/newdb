package newdb

import "sync/atomic"

var (
	atomicTxID uint64
	txL        = log.WithField("name", "tx")
)

// TxID transaction id
type TxID struct {
	ID uint64
}

// NewTxID new one *TxID
func NewTxID() *TxID {
	ret := &TxID{
		ID: atomic.AddUint64(&atomicTxID, 1),
	}
	txL.WithField("tx_id", ret).Infof("start tx")
	return ret
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

// Finish clean Tx
func (tx *Tx) Finish() {}
