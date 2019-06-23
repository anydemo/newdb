package newdb

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCatalog_MarshalUnmarshalSchema(t *testing.T) {
	// catalog := NewCatalog()
	var schema = []CatalogSchema{
		{Filename: "a", TD: []CatalogTDSchema{
			{Name: "name1", Type: "int"},
		}},
	}

	var catalogName = filepath.Join(os.TempDir(), "database.cat")
	defer os.Remove(catalogName)

	buf, err := json.Marshal(schema)
	assert.NoError(t, err, "marshal schema must no err")
	assert.Equal(t, "[{\"filename\":\"a\",\"td\":[{\"name\":\"name1\",\"type\":\"int\"}]}]", string(buf))
	err = ioutil.WriteFile(catalogName, buf, os.ModePerm)
	assert.NoError(t, err, "write file must no err")

	var result []CatalogSchema
	f, err := os.Open(catalogName)
	assert.NoError(t, err, "open file must no error")
	buf, err = ioutil.ReadAll(f)
	assert.NoError(t, err, "readall must no err")
	err = json.Unmarshal(buf, &result)
	assert.NoError(t, err, "unmarshal json must no err")
	assert.Equal(t, schema, result, "marshal and unmarshal should equal")
}

func TestCatalog_LoadSchema(t *testing.T) {
	var schema = strings.NewReader("[{\"filename\":\"data/a.db\",\"td\":[{\"name\":\"name1\",\"type\":\"int\"}]}]")
	var catalog = NewCatalog()
	tableIDs, err := catalog.LoadSchema(schema)
	require.NoError(t, err)
	assert.Len(t, catalog.TableID2DBFile, 1)
	assert.Equal(t, singleFieldTableID, tableIDs[0])
	assert.Equal(t, &TupleDesc{TdItems: []TdItem{TdItem{Type: IntType, Name: "name1"}}}, DB.C().GetTableByID(tableIDs[0]).TupleDesc())
}

func TestBufferPool_GetPage(t *testing.T) {
	txID := NewTxID()
	tableID, err := RandDBFile(2)
	require.NoError(t, err)
	dbFile := DB.C().GetTableByID(tableID)
	tuple := &Tuple{
		TD:     dbFile.TupleDesc(),
		Fields: []Field{NewIntField(1), NewIntField(3)},
	}
	err = DB.B().InsertTuple(txID, tableID, tuple)
	require.NoError(t, err)
	pid := NewHeapPageID(tableID, 0)
	t.Logf("pid.ID(%v)", pid.ID())
	page, err := DB.B().GetPage(txID, pid, PermReadOnly)
	require.NoError(t, err)
	assert.NotNil(t, page)
	assert.Equal(t, pid.ID(), page.PageID().ID())
	heapPage := page.(*HeapPage)
	assert.Equal(t, tuple, heapPage.Tuples[0])
}
