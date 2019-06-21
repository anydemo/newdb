package newdb

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/anydemo/newdb/pkg/bitset"
	"github.com/stretchr/testify/assert"
)

var (
	tableID = "20ccb2e256cc729496851f0c3f4f597324cb20b9"
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

func GeneratePageBytes(tupleNum int) ([]byte, error) {
	emptyPage := make([]byte, PageSize)
	page, err := NewHeapPage(NewHeapPageID(tableID, 1), emptyPage)
	if err != nil {
		return nil, err
	}
	bs := bitset.NewBytes(uint(page.NumOfTuples()))
	for i := 0; i < tupleNum; i++ {
		bs.SetBool(uint(i), true)
	}
	var n int
	n = copy(emptyPage, []byte(bs))
	for i := 0; i < tupleNum; i++ {
		tp := &Tuple{TD: page.TupleDesc(), Fields: []Field{NewIntField(int64(i))}}
		tpBuf, err := tp.MarshalBinary()
		if err != nil {
			return nil, err
		}
		n += copy(emptyPage[n:], tpBuf)
	}
	return emptyPage, err
}

func Test_GeneratePageBytes(t *testing.T) {
	buf, err := GeneratePageBytes(4)
	assert.NoError(t, err)
	assert.Equal(t, byte(0xf), buf[0])
	page, err := NewHeapPage(NewHeapPageID(tableID, 1), buf)
	assert.NoError(t, err)
	for i, tuple := range page.Tuples[:4] {
		assert.Equal(t, fmt.Sprintf("int(%v)", i), tuple.String())
	}
	assert.Equal(t, 4, NumOfNotNilPage(page))
	assert.Nil(t, page.Tuples[4], "only generate 4 tuple, but != 4")
}
