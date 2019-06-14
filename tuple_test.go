package newdb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntField_String(t *testing.T) {
	fields := []Field{&IntField{Val: 1}, &IntField{Val: 3}}
	intType := IntType
	assert.Equal(t, uintptr(8), fields[0].Len())
	td := TupleDesc{
		tdItems: []tdItem{tdItem{Name: "a", Type: intType}, tdItem{Name: "b", Type: intType}},
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
