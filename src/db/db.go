package ghostdb

import (
	"common"
	"db/index"
	"db/mem_cache"
	"fmt"
	"iowrapper"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
)

const SHARED_COUNT = 32

type DB struct {
	BasePath   string
	DiskFiles  map[uint32]*os.File
	WriteCache *mem_cache.MemWriteCache
	BtreeIndex []*index.BTree
}

func NewDB(basePath, table, dataBase, columnName string) *Table {
	return &Table{
		basePath:   basePath,
		table:      table,
		dataBase:   dataBase,
		columnName: columnName,
	}
}
func (table Table) GetTablePath() string {
	return fmt.Sprintf("%v/%v/%v/%v", table.basePath, table.dataBase, table.table, table.columnName)
}

func (table Table) CreateTable() {
	path := table.GetTablePath()
	os.MkdirAll(filepath.Base(path), os.ModePerm)
	if iowrapper.PathExist(path) {
		return
	}
	common.Check(iowrapper.CreateSparseFile(path, 4096*1000000))
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	common.Check(err)
	metaPage := NewMetaPage(INITROOTNULL, MAXPAGENUMBER/8)
	bs := metaPage.ToBytes()
	mapregion, err := mmap.MapRegion(f, METAPAGEMAXLENGTH, mmap.RDWR, 0, 0)
	copy(mapregion, bs)
	mapregion.Flush()
	mapregion.Unmap()
	f.Close()
}
