package mem_cache

import (
	"common"
	"container/list"
	"db/index"
	"encoding/binary"
	"iowrapper"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/directio"
	cmap "github.com/orcaman/concurrent-map"

	"github.com/edsrzf/mmap-go"
)

const (
	MMAP_COUNT      = 4
	MMAP_SIZE       = 256 * 1024 * 1024
	MMAPFILE_PREFIX = "mmap"
	CONCURRENT_SIZE = 8
)

//MetaValue is position
type MetaValue struct {
	Offset uint64
	Length uint32
}

func EncodeKey(key string, version uint64) []byte {
	length := len(key)
	concatenated := make([]byte, length+8)
	copy(concatenated, key)
	binary.LittleEndian.PutUint64(concatenated[length:], version)
	return concatenated
}

func EncodeMetaValue(meta_value *MetaValue) []byte {
	concatenated := make([]byte, unsafe.Sizeof(meta_value))
	binary.LittleEndian.PutUint64(concatenated[0:8], meta_value.Offset)
	binary.LittleEndian.PutUint32(concatenated[8:12], meta_value.Length)
	return concatenated
}

func EncodeKeyAndValue(key string, value []byte) []byte {
	concatenated := make([]byte, 8+len(key)+len(value))
	binary.LittleEndian.PutUint16(concatenated[0:2], uint16(len(key)))
	binary.LittleEndian.PutUint32(concatenated[0:2], uint32(len(value)))
	copy(concatenated[4:], key)
	copy(concatenated[4+len(key):], value)
	crc := common.Crc16(concatenated[:len(concatenated)-2])
	binary.LittleEndian.PutUint16(concatenated[len(concatenated)-2:], crc)
	return concatenated
}

//MemCache is cache struct
type mmapCache struct {
	curOffset uint64
	index     cmap.ConcurrentMap
	memStore  mmap.MMap
	rwMutex   sync.RWMutex
}

//
func NewMmapCache(file_path string) (*mmapCache, error) {
	var mp mmap.MMap
	var mc *mmapCache
	mmapFile, err := iowrapper.CreateMMapFile(file_path, MMAP_SIZE)
	defer mmapFile.Close()
	common.Check(err)
	if err == nil {
		mp, err = mmap.MapRegion(mmapFile, MMAP_SIZE, mmap.RDWR, 0, 0)
		common.Check(err)
	}
	if err == nil {
		mc = &mmapCache{
			curOffset: 0,
			index:     cmap.New(),
			memStore:  mp,
		}
	}
	return mc, err
}

//Get is to get a key and value
func (m *mmapCache) Get(key string) ([]byte, bool) {
	result, ok := m.index.Get(key)
	var value []byte
	if ok {
		meta_value := result.(MetaValue)
		value = make([]byte, meta_value.Length)
		copy(value, m.memStore[meta_value.Offset:meta_value.Offset+uint64(meta_value.Length)])

	}
	return value, ok
}

//
func (m *mmapCache) ClearMap() {
	m.index = cmap.New()
	m.curOffset = 0
}

//
type WriteMemCache struct {
	CurrentMmap    atomic.Value
	CurMemCache    *mmapCache
	MutableMmaps   chan *list.Element
	ImmutableMmaps chan *list.Element
	RingListMmaps  *list.List
	SegmentBtree   *index.SegmentBtree
	DiskFiles      map[uint32]*os.File
	NextFno        uint32
	BasePath       string
}

//NewMemCache create skiplist for mem cache use
func NewWriteMemCache(path string) *WriteMemCache {
	mutable_ch := make(chan *list.Element, MMAP_COUNT)
	immutable_ch := make(chan *list.Element, MMAP_COUNT)
	ringList := list.New()
	for i := 0; i < MMAP_COUNT; i++ {
		file_path := path + MMAPFILE_PREFIX + strconv.Itoa(i)
		mc, err := NewMmapCache(file_path)
		if err == nil {
			e := ringList.PushBack(mc)
			mutable_ch <- e
		}
	}
	var cmmap atomic.Value
	element := <-mutable_ch
	mc := element.Value.(*mmapCache)
	cmmap.Store(element)
	wmem_cache := &WriteMemCache{
		CurrentMmap:    cmmap,
		CurMemCache:    mc,
		MutableMmaps:   mutable_ch,
		ImmutableMmaps: immutable_ch,
		RingListMmaps:  ringList,
	}
	return wmem_cache
}

//
func (m *WriteMemCache) ImmutableMmapCache() {
	tmp := <-m.MutableMmaps
	value := m.CurrentMmap.Load()
	m.ImmutableMmaps <- value.(*list.Element)
	m.CurMemCache = tmp.Value.(*mmapCache)
	m.CurrentMmap.Store(tmp)
}

//Put is to insert a key and value
func (m *WriteMemCache) Put(key string, value []byte) {
	var end_flag = []byte{0xff, 0xff, 0xff, 0xff}
	mc := m.CurMemCache
	cur_offset := mc.curOffset
	value_length := len(value)
	if (cur_offset + uint64(value_length)) > MMAP_SIZE {
		m.ImmutableMmapCache()
		cur_offset = mc.curOffset
	}
	copy(mc.memStore[cur_offset:len(value)], value)
	if (cur_offset + uint64(value_length) + 4) < MMAP_SIZE {
		copy(mc.memStore[cur_offset+uint64(value_length):], end_flag)
	}
	mc.curOffset = cur_offset + uint64(len(value))
	metaValue := MetaValue{
		Offset: cur_offset,
		Length: uint32(len(value)),
	}
	mc.index.Set(key, metaValue)
}

//Get is to get a key and value
func (m *WriteMemCache) Get(key string) ([]byte, bool) {
	var value []byte
	ok := false
	tmp := m.CurrentMmap.Load()
	cur_element := tmp.(*list.Element)
	for e := cur_element; e != nil; e = e.Prev() {
		cache := e.Value.(*mmapCache)
		value, ok = cache.Get(key)
		if ok {
			return value, ok
		}
	}
	tmp = m.CurrentMmap.Load()
	cur_element = tmp.(*list.Element)
	for e := m.RingListMmaps.Back(); e != cur_element; e = e.Prev() {
		cache := e.Value.(*mmapCache)
		value, ok = cache.Get(key)
		if ok {
			break
		}
	}
	return value, ok
}

//Delete is to delete  a key and value for collect the garbedge
func (m *WriteMemCache) FreeWriteMemCache() {
	e := <-m.ImmutableMmaps
	mc := e.Value.(*mmapCache)
	keys := mc.index.Keys()
	sort.Strings(keys)
	blocks := directio.AlignedBlock(MMAP_SIZE)
	btreeNodes := make([]index.BtreeNodeItem, 0)
	pos := uint32(0)
	fno := m.NextFno
	for _, k := range keys {
		var btreeNode index.BtreeNodeItem
		i, _ := mc.index.Get(k)
		meta_value := i.(MetaValue)
		copy(blocks[pos:pos+meta_value.Length], mc.memStore[meta_value.Offset:meta_value.Offset+uint64(meta_value.Length)])
		btreeNode.Key = k
		btreeNode.MetaVal.Offset = pos
		btreeNode.MetaVal.Length = meta_value.Length
		btreeNode.MetaVal.Fno = fno
		pos = pos + meta_value.Length
		btreeNodes = append(btreeNodes, btreeNode)
	}
	path := m.BasePath + strconv.Itoa(int(fno)) + ".data"
	out, _ := iowrapper.DirectWrite(path, blocks)
	m.DiskFiles[fno] = out
	chunks := splitArray(btreeNodes, len(btreeNodes)/CONCURRENT_SIZE+1)
	m.MultiInsert(chunks)
	mc.ClearMap()
}

//split array
func splitArray(buf []index.BtreeNodeItem, lim int) [][]index.BtreeNodeItem {
	var chunk []index.BtreeNodeItem
	chunks := make([][]index.BtreeNodeItem, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:len(buf)])
	}
	return chunks
}

//multhread insert btree
func (m *WriteMemCache) MultiInsert(items [][]index.BtreeNodeItem) {
	num := len(items)
	wg := &sync.WaitGroup{}
	wg.Add(num)
	for _, it := range items {
		pt := it
		go func() {
			for _, node := range pt {
				m.SegmentBtree.ReplaceOrInsert(&node)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
