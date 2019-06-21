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
	assert.Equal(t, "[{\"Filename\":\"a\",\"TD\":[{\"Name\":\"name1\",\"Type\":\"int\"}]}]", string(buf))
	err = ioutil.WriteFile(catalogName, buf, os.ModePerm)
	assert.NoError(t, err, "write file must no err")

	var result []CatalogSchema
	f, err := os.Open(catalogName)
	assert.NoError(t, err, "open file must no error")
	buf, err = ioutil.ReadAll(f)
	assert.NoError(t, err, "readall must no err")
	json.Unmarshal(buf, &result)
	assert.Equal(t, schema, result, "marshal and unmarshal should equal")
}

func TestCatalog_LoadSchema(t *testing.T) {
	var schema = strings.NewReader("[{\"Filename\":\"data/a.db\",\"TD\":[{\"Name\":\"name1\",\"Type\":\"int\"}]}]")
	var catalog = NewCatalog()
	err := catalog.LoadSchema(schema)
	require.NoError(t, err)
	require.Len(t, catalog.TableID2DBFile, 1)
}
