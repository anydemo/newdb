package newdb

import (
	"crypto/sha1"
	"fmt"
	"os"
)

var (
	// PageSize the os dependency pagesize
	PageSize = os.Getpagesize()
)

// PageID page id
type PageID interface {
	PageNum() int
	TableID() int
}

//Page page
type Page interface {
	// PageID get the PageID
	PageID() PageID
	// MarkDirty mark the page dirty
	// if TxID is nil, Mark not dirty
	MarkDirty(*TxID)
}

// DBFile The interface for database files on disk.
// Each table is represented by a single DBFile.
// DbFiles can fetch pages and iterate through tuples.
// Each file has a unique id used to store metadata about the table in the Catalog.
// DbFiles are generally accessed through the buffer pool, rather than directly by operators.
type DBFile interface {
	ID() string

	ReadPage(pid PageID) (Page, error)
	WritePage(p Page) error

	InsertTuple(TxID, Tuple) ([]Tuple, error)
	DeleteTuple(TxID, Tuple) ([]Tuple, error)
	TupleDesc() *TupleDesc
}

var _ PageID = (*HeapPageID)(nil)

// HeapPageID HeapPageID
type HeapPageID struct {
	TID  int
	PNum int
}

// TableID table ID
func (hid HeapPageID) TableID() int {
	return hid.TID
}

// PageNum page num
func (hid HeapPageID) PageNum() int {
	return hid.PNum
}

// NewHeapPageID new HeapPageID
func NewHeapPageID(tID, pn int) PageID {
	return &HeapPageID{TID: tID, PNum: pn}
}

var _ DBFile = (*HeapFile)(nil)

// HeapFile HeapFile
type HeapFile struct {
	File *os.File
	TD   *TupleDesc
}

// ID int
func (hf HeapFile) ID() string {
	return fmt.Sprintf("%x", sha1.Sum([]byte(hf.File.Name())))
}

// ReadPage read one page
func (hf HeapFile) ReadPage(pid PageID) (Page, error) {
	panic("not implemented")
}

// WritePage write one page
func (hf *HeapFile) WritePage(p Page) error {
	panic("not implemented")
}

// InsertTuple insert tuple to the HeapPage
func (hf *HeapFile) InsertTuple(TxID, Tuple) ([]Tuple, error) {
	panic("not implemented")
}

// DeleteTuple del tuple to the HeapPage
func (hf *HeapFile) DeleteTuple(TxID, Tuple) ([]Tuple, error) {
	panic("not implemented")
}

// TupleDesc return TupleDesc
func (hf HeapFile) TupleDesc() *TupleDesc {
	return hf.TD
}

var _ Page = (*HeapPage)(nil)

// HeapPage heap page
type HeapPage struct {
	PID         *HeapPageID
	TD          *TupleDesc
	Head        []byte
	Tuples      []Tuple
	TxMarkDirty *TxID
}

// NewHeapPage new HeapPage
func NewHeapPage(pid *HeapPageID, data []byte) *HeapPage {
	panic("not implemented")
}

// PageID get pageID
func (hp HeapPage) PageID() PageID {
	return hp.PID
}

// MarkDirty mark the page dirty
// if TxID is nil, Mark not dirty
func (hp *HeapPage) MarkDirty(txID *TxID) {
	hp.TxMarkDirty = txID
}

// IsDirty if return *TxID != nil, is dirty
func (hp HeapPage) IsDirty() *TxID {
	return hp.TxMarkDirty
}

// NumOfTuples retrieve the number of tuples on this page.
func (hp HeapPage) NumOfTuples() int {
	return (PageSize * 8) / (hp.TD.Size()*8 + 1)
}

// HeaderSize computes the number of bytes in the header of
// a page in a HeapFile with each tuple occupying tupleSize bytes
func (hp HeapPage) HeaderSize() int {
	return (hp.NumOfTuples() + 7) / 8
}
