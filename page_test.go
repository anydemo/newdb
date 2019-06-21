package newdb

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHeapPage(t *testing.T) {
	dbfile := DB.Catalog.TableID2DBFile[singleFieldTableID]
	assert.Equal(t, &TupleDesc{TdItems: []TdItem{TdItem{Name: "name1", Type: IntType}}}, dbfile.TupleDesc())

	emptyPage := make([]byte, PageSize)
	page, err := NewHeapPage(NewHeapPageID(singleFieldTableID, 1), emptyPage)
	require.NoError(t, err, "new HeapPage and parse the []byte must no error")
	require.NotEqual(t, nil, page)

	emptyPage = make([]byte, PageSize)
	emptyPage[0] = 0x1
	tp := &Tuple{TD: page.TupleDesc(), Fields: []Field{NewIntField(8)}}
	tpBuf, err := tp.MarshalBinary()
	assert.NoErrorf(t, err, "marshal tuple must no error")
	assert.Equal(t, []byte{0x8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, tpBuf)
	copy(emptyPage[page.HeaderSize():page.HeaderSize()+tp.TD.Size()], tpBuf)
	page, err = NewHeapPage(NewHeapPageID(singleFieldTableID, 1), emptyPage)
	assert.NotNil(t, page.TupleDesc())
	assert.Equal(t, true, page.Bitset().Get(0), "the first byte of head is 0")
	assert.Equal(t, false, page.Bitset().Get(1), "the second bit of head is 0")
	assert.NotEmpty(t, page)
	t.Log(spew.Sdump(page.Tuples[0]))
	assert.Equal(t, "name1(int64(8))", page.TD.String())
	assert.Equal(t, "int(8)", page.Tuples[0].String(), "the first tuple must eq int(8)")
	assert.Equal(t, 1, NumOfNotNilPage(page))
}

func TestHeapFile_WritePage(t *testing.T) {
	heapFile := DB.Catalog.GetTableByID(singleFieldTableID)
	pageBuf, err := GeneratePageBytes(3)
	assert.NoError(t, err, "generate page []byte must no error")
	page, err := NewHeapPage(NewHeapPageID(singleFieldTableID, 0), pageBuf)
	assert.NotNil(t, page.TupleDesc())
	assert.NoError(t, err, "new HeapPage err")
	err = heapFile.WritePage(page)
	assert.NoError(t, err, "write page to file")

	pageRead, err := heapFile.ReadPage(page.PageID())
	assert.NoError(t, err)
	assert.Equal(t, page, pageRead)
}

func TestRecodeID(t *testing.T) {
	pageBuf, err := GeneratePageBytes(3)
	require.NoError(t, err, "generate page []byte must no error")
	page, err := NewHeapPage(NewHeapPageID(singleFieldTableID, 1), pageBuf)
	require.NotNil(t, page.TupleDesc())
	require.NoError(t, err, "new HeapPage err")

	secondTuple := page.Tuples[1]
	require.NotNil(t, secondTuple)
	assert.Equal(t, NewRecordID(page.PageID(), 1), secondTuple.RecordID)
}
