package newdb

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strings"
)

var (

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

// Parse parse the real Field
func (t Type) Parse(r io.Reader) (Field, error) {
	var (
		field Field
		err   error
	)
	buf := make([]byte, t.Len)
	_, err = r.Read(buf)
	if err != nil {
		return nil, err
	}
	switch t.Name {
	case "string":
		panic("unsupported type")
	case "int64":
		i := &IntField{TypeReal: IntType}
		err = i.UnmarshalBinary(buf)
		if err != nil {
			return nil, err
		}
		field = i
	}
	return field, err
}

// Field identify one filed like int 1
type Field interface {
	fmt.Stringer
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	Type() *Type
	Compare(Op, Field) bool
}

// Op enum of Op
type Op int

const (
	// OpEquals ==
	OpEquals Op = iota
	// OpGreaterThan >
	OpGreaterThan
	// OpLessThan <
	OpLessThan
	// OpGreaterThanOrEq >=
	OpGreaterThanOrEq
	// OpLessThanOrEq <=
	OpLessThanOrEq
	// OpLike LIKE
	OpLike
	// OpNotEquals !=
	OpNotEquals
)

func (op Op) String() (ret string) {
	switch op {
	case OpEquals:
		ret = "="
	case OpGreaterThan:
		ret = ">"
	case OpLessThan:
		ret = "<"
	case OpLessThanOrEq:
		ret = "<="
	case OpGreaterThanOrEq:
		ret = ">="
	case OpLike:
		ret = "LIKE"
	case OpNotEquals:
		ret = "!="
	default:
		ret = "UnsupportedOp"
	}
	return
}

// Predicate compares tuples to a specified Field value
type Predicate struct {
	Field   int
	Op      Op
	Operand Field
}

// Filter compares the field number of t specified in the constructor to the
// operand field specified in the constructor using the operator specific in
// the constructor. The comparison can be made through Field's compare
// method.
func (p Predicate) Filter(tuple *Tuple) bool {
	if p.Field >= len(tuple.Fields) {
		return false
	}
	return tuple.Fields[p.Field].Compare(p.Op, p.Operand)
}

func (p Predicate) String() string {
	return fmt.Sprintf("f=%v\top=%v\toperand=%v", p.Field, p.Op.String(), p.Operand.String())
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

// Compare Compare the specified field to the value of this Field.
// Return semantics are as specified by Field.compare
func (i IntField) Compare(op Op, val Field) (ret bool) {
	iV, ok := val.(*IntField)
	if !ok {
		return
	}
	switch op {
	case OpEquals:
		ret = i.Val == iV.Val
	case OpGreaterThan:
		ret = i.Val > iV.Val
	case OpLessThan:
		ret = i.Val < iV.Val
	case OpLessThanOrEq:
		ret = i.Val <= iV.Val
	case OpGreaterThanOrEq:
		ret = i.Val >= iV.Val
	case OpLike:
		ret = i.Val == iV.Val
	case OpNotEquals:
		ret = i.Val != iV.Val
	}
	return
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

// Equal 2 tuple desc is equal
func (td TupleDesc) Equal(target *TupleDesc) (ret bool) {
	// FIXME: need beauty implement
	return td.String() != "" && td.String() == target.String()
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
//
// Marshal format
// field-val1 | field-val2 |...
type Tuple struct {
	Fields   []Field
	TD       *TupleDesc
	RecordID *RecordID
}

func (tp Tuple) String() string {
	var cols []string
	for _, f := range tp.Fields {
		cols = append(cols, f.String())
	}
	return strings.Join(cols, "\t")
}

// MarshalBinary marshal tuple
func (tp Tuple) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 0, tp.TD.Size())
	for _, field := range tp.Fields {
		buf, err := field.MarshalBinary()
		if err != nil {
			return nil, err
		}
		data = append(data, buf...)
	}
	return data, err
}

// RecordID record id: PageID + TupleNum
type RecordID struct {
	PID      PageID
	TupleNum int
}

// NewRecordID NewRecordID Pointer
func NewRecordID(pid PageID, tupleNum int) *RecordID {
	return &RecordID{
		PID:      pid,
		TupleNum: tupleNum,
	}
}
