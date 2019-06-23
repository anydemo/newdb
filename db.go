package newdb

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
)

var (
	// DB singleton db
	DB = NewDatabase()

	log = logrus.New()
	dbL = log.WithField("name", "db")

	// PageSize the os dependency pagesize
	PageSize = os.Getpagesize()
)

// Database singleton struct
type Database struct {
	Catalog    *Catalog
	BufferPool *BufferPool
}

// C get Catalog
func (db *Database) C() *Catalog {
	return db.Catalog
}

// B get BufferPool
func (db *Database) B() *BufferPool {
	return db.BufferPool
}

// NewDatabase return new
func NewDatabase() *Database {
	catalog := NewCatalog()
	return &Database{
		Catalog:    catalog,
		BufferPool: NewBufferPool(),
	}
}

// Catalog The Catalog keeps track of all available tables in the database and their associated schemas.
type Catalog struct {
	TableID2DBFile map[string]DBFile
	Name2ID        map[string]string
}

// NewCatalog new Catalog
func NewCatalog() *Catalog {
	return &Catalog{
		TableID2DBFile: make(map[string]DBFile),
		Name2ID:        make(map[string]string),
	}
}

// AddTable add DBFile/Table
func (c Catalog) AddTable(file DBFile, name string) {
	id := file.ID()
	c.TableID2DBFile[id] = file
	c.Name2ID[name] = id
}

// GetTableByID get DBFile/Table by tableID
func (c Catalog) GetTableByID(tableID string) DBFile {
	return c.TableID2DBFile[tableID]
}

// GetTableByName get DBFile/Table by name
func (c Catalog) GetTableByName(name string) DBFile {
	return c.TableID2DBFile[c.Name2ID[name]]
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
		f, err := os.OpenFile(cs.Filename, os.O_RDWR, 0666)
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
				err := fmt.Errorf("unknown type %v", oneTDItem.Type)
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

// BufferPool BufferPool manages the reading and writing of pages into memory from
// disk. Access methods call into it to retrieve pages, and it fetches
// pages from the appropriate location.
// <p>
// The BufferPool is also responsible for locking;  when a transaction fetches
// a page, BufferPool checks that the transaction has the appropriate
// locks to read/write the page.
//
//@Threadsafe, all fields are final
type BufferPool struct {
	pageSize int
}

// NewBufferPool return BufferPool
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pageSize: PageSize,
	}
}

// PageSize get the os dependencied page size
func (bp BufferPool) PageSize() int {
	return bp.pageSize
}
