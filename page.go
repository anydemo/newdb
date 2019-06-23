package newdb

import (
	"bytes"
	"crypto/sha1"
	"encoding"
	"fmt"
	"io"
	"os"

	"github.com/anydemo/newdb/pkg/bitset"
)

// PageID page id
type PageID interface {
	ID() string
	PageNum() int
	TableID() string
}

//Page page
type Page interface {
	encoding.BinaryMarshaler
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

	InsertTuple(*TxID, *Tuple) ([]Page, error)
	DeleteTuple(*TxID, *Tuple) ([]Page, error)
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

// ID ${TableID}-${PageNum} identify the PageID
func (hid HeapPageID) ID() string {
	return fmt.Sprintf("%v-%v", hid.TID, hid.PNum)
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

var (
	_     DBFile = (*HeapFile)(nil)
	hfLog        = log.WithField("name", "heapfile")
)

// HeapFile HeapFile
//
// file format:
//
// [Page][Page][Page][Page]...
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
	seek, err := hf.File.Seek(int64(pid.PageNum()*DB.B().PageSize()), 0)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, DB.B().PageSize())
	n, err := hf.File.Read(buf)
	if err != nil {
		return nil, err
	}
	hfLog.WithField("op", "read_page").WithField("seek", seek).WithField("read_len", n).Infof("read page from HeapFile")
	heapPID, ok := pid.(*HeapPageID)
	if !ok {
		return nil, fmt.Errorf("pid is not HeapPageID")
	}
	page, err := NewHeapPage(heapPID, buf)
	if err != nil {
		return nil, err
	}
	return page, err
}

// WritePage write one page
func (hf *HeapFile) WritePage(page Page) error {
	seek, err := hf.File.Seek(int64(page.PageID().PageNum()*DB.B().PageSize()), 0)
	if err != nil {
		return err
	}
	buf, err := page.MarshalBinary()
	if err != nil {
		return err
	}
	n, err := hf.File.Write(buf)
	if err != nil {
		return err
	}
	err = hf.File.Sync()
	hfLog.WithField("op", "write_page").WithField("seek", seek).WithField("write_size", n).Infof("write page to HeapFile")
	return err
}

// NumPagesInFile get real num pages in file
func (hf HeapFile) NumPagesInFile() int64 {
	info, err := hf.File.Stat()
	if err != nil {
		hfLog.WithError(err).WithField("id", hf.ID())
		return 0
	}
	return info.Size()
}

// InsertTuple insert tuple to the HeapPage
func (hf *HeapFile) InsertTuple(txID *TxID, tuple *Tuple) (ret []Page, err error) {
	for i := 0; int64(i) <= hf.NumPagesInFile(); i++ {
		HPID := NewHeapPageID(hf.ID(), i)
		var page Page
		if int64(i) < hf.NumPagesInFile() {
			page, err = DB.B().GetPage(txID, HPID, PermReadWrite)
			if err != nil {
				return nil, err
			}
		} else {
			page, err = NewHeapPage(NewHeapPageID(hf.ID(), i), HeapPageCreateEmptyPageData())
			if err != nil {
				return nil, err
			}
		}
		heapPage, ok := page.(*HeapPage)
		if !ok {
			return nil, fmt.Errorf("assign page HeapPage error")
		}
		if heapPage.EmptyTupleNum() > 0 {
			heapPage.InsertTuple(tuple)
			// flush newly created page to disk
			if int64(i) == hf.NumPagesInFile() {
				hf.WritePage(page)
				hfLog.WithField("pid", page.PageID).Infof("page full, write to disk")
			}
			ret = append(ret, heapPage)
			return
		}
	}
	return nil, fmt.Errorf("failed to insert this tuple")
}

// DeleteTuple del tuple to the HeapPage
func (hf *HeapFile) DeleteTuple(*TxID, *Tuple) ([]Page, error) {
	panic("not implemented")
}

// TupleDesc return TupleDesc
func (hf HeapFile) TupleDesc() *TupleDesc {
	return hf.TD
}

var _ Page = (*HeapPage)(nil)

// HeapPage heap page
//
// file format:
//
// | header bit set | [Tuple][Tuple][Tuple][Tuple] |
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
	ret.TD = DB.C().GetTableByID(pid.TableID()).TupleDesc()
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

// EmptyTupleNum num of empty tuple
func (hp HeapPage) EmptyTupleNum() (ret int) {
	for i := 0; i < hp.NumOfTuples(); i++ {
		if !hp.Bitset().Get(uint(i)) {
			ret++
		}
	}
	return
}

// NumOfTuples retrieve the number of tuples on this page.
func (hp HeapPage) NumOfTuples() int {
	return (DB.B().PageSize() * 8) / (hp.TD.Size()*8 + 1)
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
			return nil, err
		}
		assertEqual(n, hp.TD.Size())
		return nil, nil
	}
	// else if page is used, read the Tuple
	ret := &Tuple{TD: hp.TupleDesc(), RecordID: NewRecordID(hp.PageID(), slotID)}
	for _, field := range hp.TD.TdItems {
		f, err := field.Type.Parse(r)
		if err != nil {
			return nil, err
		}
		ret.Fields = append(ret.Fields, f)
	}
	return ret, nil
}

// InsertTuple insert one tuple one the page
func (hp *HeapPage) InsertTuple(tuple *Tuple) error {
	if !hp.TupleDesc().Equal(tuple.TD) {
		return fmt.Errorf("tuple desc is diff")
	}
	for i := 0; i < hp.NumOfTuples(); i++ {
		if !hp.Bitset().Get(uint(i)) {
			hp.Tuples[i] = tuple
			tuple.RecordID = NewRecordID(hp.PID, i)
			hp.Bitset().Set(uint(i))
			return nil
		}
	}
	return fmt.Errorf("page is full")
}

// MarshalBinary implement encoding.BinaryMarshaler
func (hp HeapPage) MarshalBinary() (data []byte, err error) {
	data = make([]byte, DB.B().PageSize())
	var n = copy(data, []byte(hp.Head))
	tupleSize := hp.TupleDesc().Size()
	var buf []byte
	for index, tuple := range hp.Tuples {
		if hp.Bitset().Get(uint(index)) {
			buf, err = tuple.MarshalBinary()
			if err != nil {
				return nil, err
			}
		} else {
			buf = make([]byte, tupleSize)
		}
		n += copy(data[n:], buf)
	}
	return
}

// HeapPageCreateEmptyPageData create emptyPageDate
func HeapPageCreateEmptyPageData() []byte {
	return make([]byte, DB.B().PageSize())
}
