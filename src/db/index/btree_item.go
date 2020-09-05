package index

import (
	"common"
	"encoding/binary"
	"unsafe"
	logger "until/xlog4go"
)

type MetaValue struct {
	Fno    uint32
	Offset uint32
	Length uint32
}

type BtreeNodeItem struct {
	Key     string
	MetaVal MetaValue
	KeyType byte
}

func NewBtreeNodeItem(key string, meta_value MetaValue, keyType byte) *BtreeNodeItem {
	if len(key) > MAXKEYSIZE {
		logger.Warn("the max Key size is 128 byte")
		return nil
	}
	return &BtreeNodeItem{
		Key:     key,
		MetaVal: meta_value,
		KeyType: keyType,
	}
}

func (item MetaValue) ToBytes(bytes []byte) uint32 {
	iStart, iEnd := 0, 0
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bytes[iStart:iEnd], item.Fno)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bytes[iStart:iEnd], item.Offset)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bytes[iStart:iEnd], item.Length)
	return uint32(iEnd)
}

func BytesToMetaValue(barr []byte) MetaValue {
	iStart, iEnd := uint32(0), uint32(0)
	iEnd = iStart + common.INT32_LEN
	fno := binary.LittleEndian.Uint32(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	offset := binary.LittleEndian.Uint32(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	length := binary.LittleEndian.Uint32(barr[iStart:iEnd])
	return MetaValue{
		Fno:    fno,
		Offset: offset,
		Length: length,
	}
}

func (item BtreeNodeItem) Size() uint16 {
	return uint16(unsafe.Sizeof(item))
}

func (item BtreeNodeItem) KeyLength() uint16 {
	return uint16(len(item.Key))
}

func (item BtreeNodeItem) ToBytes(bytes []byte) int32 {
	length := item.KeyLength()
	_assert(len(bytes) >= int(item.Size()), "the BtreeNodeItem to bytes's bytes is too small")
	iStart, iEnd := 0, 0
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bytes[iStart:iEnd], length)
	keyLen := len(item.Key)
	iStart = iEnd
	iEnd = iStart + keyLen
	copy(bytes[iStart:iEnd], item.Key)
	iStart = iEnd
	meta_val_length := item.MetaVal.ToBytes(bytes[iStart:])
	iEnd = iStart + int(meta_val_length)
	iStart = iEnd
	iEnd = iStart + common.BYTE_LEN
	bytes[iStart] = item.KeyType
	crc := common.Crc16(bytes[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bytes[iStart:iEnd], crc)
	return int32(iEnd)
}

func BytesToBtreeNodeItems(barr []byte, count uint16) []*BtreeNodeItem {
	items := make([]*BtreeNodeItem, count, count)
	iStart, iEnd := uint32(0), uint32(0)
	sentiel := uint32(0)
	for i := uint16(0); i < count; i++ {
		b := new(BtreeNodeItem)
		iStart = iEnd
		iEnd = iStart + common.INT16_LEN
		length := binary.LittleEndian.Uint16(barr[iStart:iEnd])
		iStart = iEnd
		iEnd = iStart + uint32(length)
		b.Key = string(barr[iStart:iEnd])
		iStart = iEnd
		iEnd = iStart + uint32(unsafe.Sizeof(b.MetaVal))
		b.MetaVal = BytesToMetaValue(barr[iStart:iEnd])
		iStart = iEnd
		iEnd = iStart + common.BYTE_LEN
		b.KeyType = barr[iStart]
		crc_0 := common.Crc16(barr[sentiel:iEnd])
		iStart = iEnd
		iEnd = iStart + common.INT16_LEN
		crc_1 := binary.LittleEndian.Uint16(barr[iStart:iEnd])
		_assert(crc_0 == crc_1, "the BtreeNodeItems crc is failed")
		items[i] = b
		sentiel = iEnd
	}
	return items
}

func BatchBtreeNodeItemToBytes(items []*BtreeNodeItem) []byte {
	bytes := make([]byte, BLOCKSIZE, BLOCKSIZE)
	iStart, length := int32(0), int32(0)
	for _, item := range items {
		iStart = iStart + length
		length = item.ToBytes(bytes[iStart:])
	}
	return bytes[0 : iStart+length]
}
