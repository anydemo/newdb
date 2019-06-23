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

	// DefaultPageSize the os dependency pagesize
	DefaultPageSize = os.Getpagesize()
	// DefaultPageNum default page num
	DefaultPageNum = 50
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
		BufferPool: NewBufferPool(-1),
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
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
}

// CatalogSchema for load Catalog from file
type CatalogSchema struct {
	Filename  string            `json:"filename,omitempty"`
	TD        []CatalogTDSchema `json:"td,omitempty"`
	TableName string            `json:"table_name,omitempty"`
}

// LoadSchema load Catalog from file, and return slice of TableID
func (c *Catalog) LoadSchema(r io.Reader) (ret []string, err error) {
	var schema []CatalogSchema
	fBuf, err := ioutil.ReadAll(r)
	if err != nil {
		dbL.WithError(err).Error("read file err")
		return
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
				return nil, err
			}
			td.TdItems = append(td.TdItems, one)
		}

		heapFile := NewHeapFile(f, td)
		heapFileID := heapFile.ID()
		tableName := heapFileID
		if cs.TableName != "" {
			tableName = heapFileID
		}
		c.AddTable(heapFile, tableName)

		ret = append(ret, heapFileID)
	}
	return
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
	maxSize  int
	pageSize int
	// PageID2Page k is PageID.ID()
	PageID2Page map[string]Page
}

// NewBufferPool return BufferPool
func NewBufferPool(size int) *BufferPool {
	if size == -1 {
		size = DefaultPageNum
	}
	return &BufferPool{
		maxSize:     size,
		pageSize:    DefaultPageSize,
		PageID2Page: make(map[string]Page),
	}
}

// PageSize get the os dependencied page size
func (bp BufferPool) PageSize() int {
	return bp.pageSize
}

// GetPage Retrieve the specified page with the associated permissions.
// Will acquire a lock and may block if that lock is held by another
// transaction.
// <p>
// The retrieved page should be looked up in the buffer pool.  If it
// is present, it should be returned.  If it is not present, it should
// be added to the buffer pool and returned.  If there is insufficient
// space in the buffer pool, a page should be evicted and the new page
// should be added in its place.
func (bp *BufferPool) GetPage(tx *TxID, pid PageID, perm Permission) (ret Page, err error) {
	pidKey := pid.ID()
	if _, exists := bp.PageID2Page[pidKey]; !exists {
		if len(bp.PageID2Page) >= bp.maxSize {
			err = bp.evictPage()
			if err != nil {
				return
			}
		}
		ret, err = DB.C().GetTableByID(pid.TableID()).ReadPage(pid)
		if err != nil {
			return nil, err
		}
		bp.PageID2Page[pidKey] = ret
	}
	return bp.PageID2Page[pidKey], nil
}

func (bp *BufferPool) evictPage() error {
	return nil
}

// InsertTuple insert tuple to page
func (bp *BufferPool) InsertTuple(txID *TxID, tableID string, tuple *Tuple) error {
	hf := DB.C().GetTableByID(tableID)
	dirtyPages, err := hf.InsertTuple(txID, tuple)
	if err != nil {
		return err
	}
	for _, dirty := range dirtyPages {
		dirty.MarkDirty(txID)
		pid := dirty.PageID().ID()
		if _, exists := bp.PageID2Page[pid]; !exists {
			_, err = bp.GetPage(txID, dirty.PageID(), PermReadWrite)
			if err != nil {
				return err
			}
		}
		bp.PageID2Page[pid] = dirty
	}
	return nil
}
