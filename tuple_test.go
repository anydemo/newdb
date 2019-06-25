package newdb

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntField_String(t *testing.T) {
	fields := []Field{&IntField{Val: 1, TypeReal: IntType}, &IntField{Val: 3, TypeReal: IntType}}
	intType := IntType
	assert.Equal(t, uintptr(8), fields[0].Type().Len)
	td := &TupleDesc{
		TdItems: []TdItem{TdItem{Name: "a", Type: intType}, TdItem{Name: "b", Type: intType}},
	}
	tp := Tuple{
		Fields: fields,
		TD:     td,
	}
	assert.Equal(t, "int(1)\tint(3)", tp.String())
	assert.Equal(t, uintptr(8), Sizeof(int(1)))
}

func TestIntField_MarshalBinary(t *testing.T) {
	type suit struct {
		K      int64
		Wanted []byte
	}
	var (
		suits = []suit{
			{K: 0, Wanted: []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
			{K: 1, Wanted: []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
			{K: -1, Wanted: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
			{K: 16, Wanted: []byte{0x10, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}},
			{K: -16, Wanted: []byte{0xf0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
			{K: math.MaxInt64, Wanted: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}},
			{K: math.MinInt64, Wanted: []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80}},
		}
	)
	for _, s := range suits {
		var intField = NewIntField(s.K)
		var buf, err = intField.MarshalBinary()
		assert.NoError(t, err, "marshal field error")
		assert.Equalf(t, s.Wanted, buf, "marshal []byte: input:%v, get:%v", s, intField)
		var f = NewIntField(0)
		err = f.UnmarshalBinary(buf)
		assert.NoErrorf(t, err, "unmarshal error input:%v, get:%v", s, f)
		assert.Equalf(t, intField, f, "input:%v, get:%v", s, f)
		assert.Equalf(t, s.K, f.(*IntField).Val, "input:%v, get:%v", s, f)
	}
}

func TestType_Parse(t *testing.T) {
	intField1 := NewIntField(1)
	ifbuf, err := intField1.MarshalBinary()
	assert.NoError(t, err)
	r := bytes.NewReader(ifbuf)
	intf1, err := IntType.Parse(r)
	assert.NoError(t, err)
	assert.Equal(t, intField1, intf1)
	assert.Equal(t, "int(1)", intf1.String())
}

func TestIntField_Compare(t *testing.T) {
	var tests = []struct {
		op     Op
		f1     Field
		f2     Field
		wanted bool
	}{
		{OpEquals, NewIntField(1), NewIntField(2), false},
		{OpNotEquals, NewIntField(1), NewIntField(2), true},
		{Op(999), NewIntField(1), NewIntField(2), false},
		{Op(999), NewIntField(1), NewIntField(1), false},
	}
	for _, test := range tests {
		assert.Equal(t, test.wanted, test.f1.Compare(test.op, test.f2))
	}
}
