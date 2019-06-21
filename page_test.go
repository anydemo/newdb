package newdb

import (
	"os"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func init() {
	tmpfile := "data/a.db"
	_, err := os.Create(tmpfile)
	if err != nil {
		log.WithError(err).WithField("name", "page_test_init")
	}
	var schema = strings.NewReader("[{\"Filename\":\"data/a.db\",\"TD\":[{\"Name\":\"name1\",\"Type\":\"int\"}]}]")
	DB.Catalog.LoadSchema(schema)
}

func NumOfNotNilPage(page *HeapPage) (ret int) {
	for _, p := range page.Tuples {
		if p != nil {
			ret++
		}
	}
	return
}

func TestNewHeapPage(t *testing.T) {
	tableID := "20ccb2e256cc729496851f0c3f4f597324cb20b9"
	dbfile := DB.Catalog.TableID2DBFile[tableID]
	assert.Equal(t, &TupleDesc{TdItems: []TdItem{TdItem{Name: "name1", Type: IntType}}}, dbfile.TupleDesc())

	emptyPage := make([]byte, PageSize)
	page, err := NewHeapPage(NewHeapPageID(tableID, 1), emptyPage)
	require.NoError(t, err, "new HeapPage and parse the []byte must no error")
	require.NotEqual(t, nil, page)

	emptyPage = make([]byte, PageSize)
	emptyPage[0] = 0x1
	tp := &Tuple{TD: page.TupleDesc(), Fields: []Field{NewIntField(8)}}
	tpBuf, err := tp.MarshalBinary()
	assert.NoErrorf(t, err, "marshal tuple must no error")
	assert.Equal(t, []byte{0x8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, tpBuf)
	copy(emptyPage[page.HeaderSize():page.HeaderSize()+tp.TD.Size()], tpBuf)
	page, err = NewHeapPage(NewHeapPageID(tableID, 1), emptyPage)
	assert.Equal(t, true, page.Bitset().Get(0), "the first byte of head is 0")
	assert.Equal(t, false, page.Bitset().Get(1), "the second bit of head is 0")
	assert.NotEmpty(t, page)
	t.Log(spew.Sdump(page.Tuples[0]))
	assert.Equal(t, "name1(int64(8))", page.TD.String())
	assert.Equal(t, "int(8)", page.Tuples[0].String(), "the first tuple must eq int(8)")
	assert.Equal(t, 1, NumOfNotNilPage(page))
}
