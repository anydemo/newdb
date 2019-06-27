package newdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOp_String(t *testing.T) {
	tests := []struct {
		name    string
		op      Op
		wantRet string
	}{
		{"eq", OpEquals, "="},
		{"not_eq", OpNotEquals, "!="},
		{"not_supported", Op(11), "UnsupportedOp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRet := tt.op.String(); gotRet != tt.wantRet {
				t.Errorf("Op.String() = %v, want %v", gotRet, tt.wantRet)
			}
		})
	}
}

func TestPredicate_Filter(t *testing.T) {
	var tests = []struct {
		name    string
		f       int
		op      Op
		operand Field
		t       *Tuple
		wanted  bool
	}{
		{"eq", 0, OpEquals, NewIntField(1), &Tuple{Fields: []Field{NewIntField(1)}}, true},
		{"not eq", 0, OpEquals, NewIntField(1), &Tuple{Fields: []Field{NewIntField(2)}}, false},
		{"not eq", 0, OpNotEquals, NewIntField(1), &Tuple{Fields: []Field{NewIntField(2)}}, true},
	}
	for _, test := range tests {
		p := Predicate{Field: test.f, Op: test.op, Operand: test.operand}
		assert.Equal(t, test.wanted, p.Filter(test.t), test.name)
	}
}

func TestFilter_TupleDesc(t *testing.T) {
	pred := &Predicate{
		Field:   0,
		Op:      OpEquals,
		Operand: NewIntField(1),
	}
	op := NewFilter(pred, NewMockScan(-5, 5, 2))
	expected := GetTupleDesc(2, "scan")
	actual := op.TupleDesc()
	assert.Equal(t, expected, actual)
}

func TestFilter_filterAllLessThan(t *testing.T) {
	pred := &Predicate{
		Field:   0,
		Op:      OpLessThan,
		Operand: NewIntField(1),
	}
	op := NewFilter(pred, NewMockScan(-5, 5, 2))
	expected := GetTupleDesc(2, "scan")
	actual := op.TupleDesc()
	assert.Equal(t, expected, actual)
	require.NoError(t, op.Open())
	i := -5
	for op.HasNext() {
		next := op.Next()
		assert.NoError(t, op.Error())
		assert.Equal(t, fmt.Sprintf("int(%v)\tint(%v)", i, i), next.String())
		i++
	}
	require.Equal(t, 1, i)
}

func TestTupleIterator_AllTest(t *testing.T) {
	td := NewTupleDesc(GetTypes(2), GetStrings(2, "it"))
	var tuples = make([]*Tuple, 10)
	for i := 0; i < 10; i++ {
		tuples[i] = &Tuple{TD: td, Fields: GetFields(2)}
	}
	tuples[5] = nil
	it := NewTupleIterator(td, tuples)
	err := it.Open()
	assert.NoError(t, err)
	i := 0
	for it.HasNext() {
		next := it.Next()
		assert.NotNil(t, next)
		i++
	}
	assert.Equal(t, 9, i)
}

func TestNewSeqScan(t *testing.T) {
	// prepare data
	dbFile := DB.C().GetTableByID(singleFieldTableID)
	txID := NewTxID()
	td := dbFile.TupleDesc()
	tuple := &Tuple{
		TD:     td,
		Fields: []Field{NewIntField(1)},
	}
	err := DB.B().InsertTuple(txID, singleFieldTableID, tuple)
	assert.NoError(t, err)

	it := NewSeqScan(NewTxID(), singleFieldTableID, "seq_scan")
	err = it.Open()
	require.NoError(t, err)
	var i int
	for it.HasNext() {
		i++
		next := it.Next()
		assert.NoError(t, it.Error())
		assert.NotNil(t, next)
	}
	assert.NotEqual(t, 0, i)
}
