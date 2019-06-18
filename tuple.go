package newdb

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"

	"github.com/sirupsen/logrus"
)

var (
	log = logrus.New()

	// IntType enum of Type int
	IntType = &Type{Name: reflect.TypeOf(int64(0)).Name(), Len: Sizeof(int64(0))}
	// StringType enum of Type string
	StringType = &Type{Name: reflect.TypeOf("").Name(), Len: 16}
)

// Iterator can iterate
type Iterator interface {
	HasNext() bool
	Next() interface{}
}

// Type type of fields
type Type struct {
	Name string
	Len  uintptr
}

func (t Type) String() string {
	return fmt.Sprintf("%v(%v)", t.Name, t.Len)
}

// Field identify one filed like int 1
type Field interface {
	fmt.Stringer
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	Type() *Type
}

// IntField int filed
type IntField struct {
	Val      int64
	TypeReal *Type
}

var _ Field = (*IntField)(nil)

// NewIntField constructor of IntField
func NewIntField(val int64) Field {
	return &IntField{Val: val, TypeReal: IntType}
}

// Type the type of int
func (i IntField) Type() *Type {
	return i.TypeReal
}

// String the readable IntField
func (i IntField) String() string {
	return fmt.Sprintf("int(%v)", i.Val)
}

// MarshalBinary implement encoding.BinaryMarshaler
func (i IntField) MarshalBinary() (data []byte, err error) {
	data = make([]byte, i.TypeReal.Len)
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

// TdItem tuple desc item
type TdItem struct {
	Type *Type
	Name string
}

func (ti TdItem) String() string {
	return fmt.Sprintf("%v(%v)", ti.Name, ti.Type.String())
}

// TupleDesc the tuple descrition
type TupleDesc struct {
	TdItems []TdItem
}

func (td TupleDesc) String() string {
	var inn []string
	for _, it := range td.TdItems {
		inn = append(inn, it.String())
	}
	return strings.Join(inn, ",")
}

// Size get size of fields
func (td TupleDesc) Size() int {
	var ret uintptr
	for _, item := range td.TdItems {
		ret += item.Type.Len
	}
	return int(ret)
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