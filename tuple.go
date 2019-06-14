package newdb

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
)

var (
	// IntType type of int
	IntType = IntField{}.Type()
)

// Iterator can iterate
type Iterator interface {
	HasNext() bool
	Next() interface{}
}

// Field identify one filed like int 1
type Field interface {
	fmt.Stringer
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	Len() uintptr
	Type() reflect.Type
}

// IntField int filed
type IntField struct {
	Val int64
}

var _ Field = (*IntField)(nil)

// NewIntField constructor of IntField
func NewIntField(val int64) Field {
	return &IntField{Val: val}
}

// Len the len of int field
func (i IntField) Len() uintptr {
	return Sizeof(i.Val)
}

// Type the type of int
func (i IntField) Type() reflect.Type {
	return reflect.TypeOf(i.Val)
}

// String the readable IntField
func (i IntField) String() string {
	return fmt.Sprintf("int(%v)", i.Val)
}

// MarshalBinary implement encoding.BinaryMarshaler
func (i IntField) MarshalBinary() (data []byte, err error) {
	data = make([]byte, i.Len())
	buffer := bytes.NewBuffer(data)
	buffer.Reset()
	err = binary.Write(buffer, binary.LittleEndian, i.Val)
	return
}

// UnmarshalBinary implement encoding.BinaryUnmarshaler
func (i *IntField) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)
	return binary.Read(reader, DefaultOrder, &i.Val)
}

type tdItem struct {
	Type reflect.Type
	Name string
}

func (ti tdItem) String() string {
	return fmt.Sprintf("%v(%v)", ti.Name, ti.Type.String())
}

// TupleDesc the tuple descrition
type TupleDesc struct {
	tdItems []tdItem
}

func (td TupleDesc) String() string {
	var inn []string
	for _, it := range td.tdItems {
		inn = append(inn, it.String())
	}
	return strings.Join(inn, ",")
}

// Tuple one record
type Tuple struct {
	Fields []Field
	TD     TupleDesc
}

func (tp Tuple) String() string {
	var cols []string
	for _, f := range tp.Fields {
		cols = append(cols, f.String())
	}
	return strings.Join(cols, "\t")
}

// PageID page id
type PageID interface{}

//Page page
type Page interface{}

// Tx transaction
type Tx interface{}

// DBFile The interface for database files on disk.
// Each table is represented by a single DBFile.
// DbFiles can fetch pages and iterate through tuples.
// Each file has a unique id used to store metadata about the table in the Catalog.
// DbFiles are generally accessed through the buffer pool, rather than directly by operators.
type DBFile interface {
	ReadPage(pid PageID) (Page, error)
	WritePage(p Page) error

	InsertTuple(Tx, Tuple) ([]Tuple, error)
	DeleteTuple(Tx, Tuple) ([]Tuple, error)
	GetID() int
	GetTupleDesc() TupleDesc
}
