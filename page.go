package newdb

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"os"

	"github.com/anydemo/newdb/pkg/bitset"
)

var (
	// PageSize the os dependency pagesize
	PageSize = os.Getpagesize()
)

// PageID page id
type PageID interface {
	PageNum() int
	TableID() string
}

//Page page
type Page interface {
	// PageID get the PageID
	PageID() PageID
	// MarkDirty mark the page dirty
	// if TxID is nil, Mark not dirty
	MarkDirty(*TxID)
	TupleDesc() *TupleDesc
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
	// TID TableID
	TID string
	// PNum PageNum
	PNum int
}

// TableID table ID
func (hid HeapPageID) TableID() string {
	return hid.TID
}

// PageNum page num
func (hid HeapPageID) PageNum() int {
	return hid.PNum
}

// NewHeapPageID new HeapPageID
func NewHeapPageID(tID string, pn int) *HeapPageID {
	return &HeapPageID{TID: tID, PNum: pn}
}

var _ DBFile = (*HeapFile)(nil)

// HeapFile HeapFile
type HeapFile struct {
	File *os.File
	TD   *TupleDesc
}

// NewHeapFile new HeapFile
func NewHeapFile(file *os.File, td *TupleDesc) *HeapFile {
	return &HeapFile{
		File: file,
		TD:   td,
	}
}

// ID string
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
	Tuples      []*Tuple
	TxMarkDirty *TxID
}

// NewHeapPage new HeapPage
func NewHeapPage(pid *HeapPageID, data []byte) (*HeapPage, error) {
	ret := HeapPage{}
	ret.TD = DB.Catalog.GetTableByID(pid.TableID()).TupleDesc()
	ret.PID = pid

	bufReader := bytes.NewReader(data)
	ret.Head = make([]byte, ret.HeaderSize())
	n, err := bufReader.Read(ret.Head)
	if err != nil {
		err = fmt.Errorf("read header error %v", err)
		log.WithError(err).Warnf("read header error")
		return nil, err
	}

	if n < ret.HeaderSize() {
		err = fmt.Errorf("read head want %v, get %v", ret.HeaderSize(), n)
		log.WithError(err).Warnf("read head error")
		return nil, err
	}
	// TODO: implement here
	ret.Tuples = make([]*Tuple, ret.NumOfTuples())
	for i := 0; i < ret.NumOfTuples(); i++ {
		ret.Tuples[i], err = ret.readNextTuple(bufReader, i)
		if err != nil && err != io.EOF {
			err = fmt.Errorf("read tuple %vth err: %v", i, err)
			return nil, err
		}
		if err == io.EOF {
			log.Debugf("read %vth tuple, and end", i)
		}
	}
	return &ret, nil
}

// TupleDesc get the tuple desc
func (hp HeapPage) TupleDesc() *TupleDesc {
	return hp.TD
}

// PageID get pageID
func (hp HeapPage) PageID() PageID {
	return hp.PID
}

// Bitset get bitset
func (hp *HeapPage) Bitset() bitset.BitSet {
	return bitset.Bytes(hp.Head)
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

func (hp HeapPage) readNextTuple(r io.Reader, slotID int) (*Tuple, error) {
	// if page not used
	if !hp.Bitset().Get(uint(slotID)) {
		buf := make([]byte, hp.TD.Size())
		n, err := r.Read(buf)
		if err != nil {
			return nil, err
		}
		if n != hp.TD.Size() {
			err = fmt.Errorf("read size want: %v, get: %v", hp.TD.Size(), n)
		}
		assertEqual(n, hp.TD.Size())
		return nil, nil
	}
	// else if page is used, read the Tuple
	ret := &Tuple{}
	for _, field := range hp.TD.TdItems {
		f, err := field.Type.Parse(r)
		if err != nil {
			return nil, err
		}
		ret.Fields = append(ret.Fields, f)
	}
	return ret, nil
}
