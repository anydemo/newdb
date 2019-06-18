package newdb

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

var (
	// DB singleton db
	DB = NewDatabase()

	dbL = logrus.New().WithField("name", "db")
)

// Database singleton struct
type Database struct {
	Catalog *Catalog
}

// NewDatabase return new
func NewDatabase() *Database {
	catalog := NewCatalog()
	return &Database{
		Catalog: catalog,
	}
}

// Catalog The Catalog keeps track of all available tables in the database and their associated schemas.
type Catalog struct {
	TableID2DBFile map[string]DBFile
}

// NewCatalog new Catalog
func NewCatalog() *Catalog {
	return &Catalog{
		TableID2DBFile: make(map[string]DBFile),
	}
}

// AddTable add DBFile/Table
func (c Catalog) AddTable(file DBFile, name string) {
	c.TableID2DBFile[name] = file
}

// GetTable get DBFile/Table
func (c Catalog) GetTable(name string) DBFile {
	return c.TableID2DBFile[name]
}

// CatalogTDSchema for CatalogSchema
type CatalogTDSchema struct {
	Name string
	Type string
}

// CatalogSchema for load Catalog from file
type CatalogSchema struct {
	Filename string
	TD       []CatalogTDSchema
}

// LoadSchema load Catalog from file
func (c *Catalog) LoadSchema(r io.Reader) error {
	var schema []CatalogSchema
	fBuf, err := ioutil.ReadAll(r)
	if err != nil {
		dbL.WithError(err).Error("read file err")
		return err
	}
	err = json.Unmarshal(fBuf, &schema)
	if err != nil {
		dbL.WithError(err).Error("unmarshal err")
	}
	for _, cs := range schema {
		f, err := os.Open(cs.Filename)
		if err != nil {
			dbL.WithError(err).Error("open file error")
		}
		var td = &TupleDesc{}
		for _, oneTDItem := range cs.TD {
			one := TdItem{}
			one.Name = oneTDItem.Name
			switch oneTDItem.Type {
			case "int":
				one.Type = IntType
			default:
				err := xerrors.Errorf("unknown type %v", oneTDItem.Type)
				dbL.WithError(err).Error("err in Load schema from reader")
				return err
			}
			td.TdItems = append(td.TdItems, one)
		}
		heapFile := NewHeapFile(f, td)
		c.AddTable(heapFile, heapFile.ID())
	}
	return err
}
