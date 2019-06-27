package newdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/anydemo/newdb/pkg/bitset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	singleFieldTableID = "20ccb2e256cc729496851f0c3f4f597324cb20b9"
	testLog            = log.WithField("name", "test")
)

func init() {
	tmpfile := "data/a.db"
	_, err := os.Create(tmpfile)
	if err != nil {
		log.WithError(err).WithField("name", "page_test_init").Errorf("create file %v", tmpfile)
	}
	var schema = strings.NewReader(fmt.Sprintf("[{\"filename\":\"%v\",\"td\":[{\"name\":\"name1\",\"type\":\"int\"}]}]", tmpfile))
	_, err = DB.C().LoadSchema(schema)
	if err != nil {
		testLog.WithError(err).Error("init test utils, LoadSchema return err")
		panic("has err with loadSchema")
	}
	testLog.Info("init test database")
}

// NumOfNotNilPage the num of exist pages
func NumOfNotNilPage(page *HeapPage) (ret int) {
	for _, p := range page.Tuples {
		if p != nil {
			ret++
		}
	}
	return
}

// GeneratePageBytes
func GeneratePageBytes(tupleNum int) ([]byte, error) {
	emptyPage := make([]byte, DB.B().PageSize())
	page, err := NewHeapPage(NewHeapPageID(singleFieldTableID, 1), emptyPage)
	if err != nil {
		return nil, err
	}
	if tupleNum > page.NumOfTuples() {
		return nil, fmt.Errorf("to large tuple num, get %v, max: %v", tupleNum, page.NumOfTuples())
	}
	bs := bitset.NewBytes(uint(page.NumOfTuples()))
	for i := 0; i < tupleNum; i++ {
		bs.SetBool(uint(i), true)
	}
	var n = copy(emptyPage, []byte(bs))
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
	page, err := NewHeapPage(NewHeapPageID(singleFieldTableID, 1), buf)
	assert.NoError(t, err)
	for i, tuple := range page.Tuples[:4] {
		assert.Equal(t, fmt.Sprintf("int(%v)", i), tuple.String())
	}
	assert.Equal(t, 4, NumOfNotNilPage(page))
	assert.Nil(t, page.Tuples[4], "only generate 4 tuple, but != 4")
}

func RandDBFile(fieldNum int) (ret string, err error) {
	tmpfile := fmt.Sprintf("data/tmp-%v.data", RandString(10))
	_, err = os.Create(tmpfile)
	if err != nil {
		log.WithError(err).WithField("name", "page_test_init").Errorf("create file %v", tmpfile)
	}
	var fields []string
	for i := 0; i < fieldNum; i++ {
		fields = append(fields, fmt.Sprintf("{\"name\":\"f%v\",\"type\":\"int\"}", i))
	}
	var schemaString = fmt.Sprintf("[{\"filename\":\"%v\",\"td\":[%v]}]", tmpfile, strings.Join(fields, ","))
	err = ioutil.WriteFile(tmpfile+".schema.json", []byte(schemaString), 0644)
	if err != nil {
		return "", err
	}
	var schema = strings.NewReader(schemaString)
	tableIDS, err := DB.C().LoadSchema(schema)
	if err != nil {
		testLog.WithError(err).Error("init test utils, LoadSchema return err")
		panic("has err with loadSchema")
	}
	ret = tableIDS[0]
	return
}

func TestRandDBFile(t *testing.T) {
	tableID, err := RandDBFile(3)
	require.NoError(t, err)
	DBFile := DB.C().GetTableByID(tableID)
	assert.Equal(t, tableID, DBFile.ID())
	assert.Equal(t, &TupleDesc{TdItems: []TdItem{TdItem{Type: IntType, Name: "f0"}, TdItem{Type: IntType, Name: "f1"}, TdItem{Type: IntType, Name: "f2"}}}, DBFile.TupleDesc())
}

// GetTupleDesc get td with n field, type IntType, name ${name}{0, n-1}
func GetTupleDesc(n int, name string) *TupleDesc {
	return NewTupleDesc(GetTypes(n), GetStrings(n, name))
}

func GetFields(n int) (ret []Field) {
	for i := 0; i < n; i++ {
		ret = append(ret, NewIntField(int64(i)))
	}
	return
}

// GetTypes ret IntType, IntType ...
func GetTypes(n int) (ret []*Type) {
	for i := 0; i < n; i++ {
		ret = append(ret, IntType)
	}
	return
}

// GetStrings return ${prefix}0, ${prefix}1 ...
func GetStrings(n int, prefix string) (ret []string) {
	for i := 0; i < n; i++ {
		ret = append(ret, fmt.Sprintf("%v%v", prefix, i))
	}
	return
}

func TestGetTupleDesc(t *testing.T) {
	td := GetTupleDesc(2, "demo")
	assert.Equal(t, TdItem(TdItem{Type: IntType, Name: "demo0"}), td.TdItems[0])
	assert.Equal(t, TdItem(TdItem{Type: IntType, Name: "demo1"}), td.TdItems[1])
}

var _ OpIterator = (*MockScan)(nil)

type MockScan struct {
	Cur, Low, High, Width int
	Err                   error
}

func NewMockScan(low, high, width int) *MockScan {
	return &MockScan{
		Cur:   low,
		Low:   low,
		High:  high,
		Width: width,
	}
}
func (m MockScan) Error() error {
	return m.Err
}
func (m *MockScan) Open() error {
	m.Cur = m.Low
	return nil
}

func (m *MockScan) Close() {}

func (m *MockScan) HasNext() bool {
	return m.Cur < m.High
}

func (m *MockScan) Next() *Tuple {
	if m.Cur >= m.High {
		m.Err = fmt.Errorf("no such element")
		return nil
	}
	tup := &Tuple{
		TD: GetTupleDesc(m.Width, "scan"),
	}
	for i := 0; i < m.Width; i++ {
		tup.Fields = append(tup.Fields, NewIntField(int64(m.Cur)))
	}
	m.Cur++
	return tup
}

func (m *MockScan) Rewind() error {
	m.Close()
	return m.Open()
}

func (m *MockScan) TupleDesc() *TupleDesc {
	return GetTupleDesc(m.Width, "scan")
}

func TestNewMockScan(t *testing.T) {
	scan := NewMockScan(0, 3, 2)
	err := scan.Open()
	assert.NoError(t, err)
	for scan.HasNext() {
		ele := scan.Next()
		assert.NoError(t, err)
		assert.NotNil(t, ele)
	}
}
