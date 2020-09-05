package index

const SHARD_COUNT = 32

type SegmentBtree []*BTree

// Creates a new concurrent SegmentBtree.
func NewSegmentBtree(btree_path string) SegmentBtree {
	m := make(SegmentBtree, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		m[i] = BuildBTreeFromPage(btree_path)
	}
	return m
}

//
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// GetShard returns shard under given key
func (m SegmentBtree) GetShard(key string) *BTree {
	return m[uint(fnv32(key))%uint(SHARD_COUNT)]
}

func (m SegmentBtree) ReplaceOrInsert(item Item) Item {
	bItem := item.(*BtreeNodeItem)
	tr := m.GetShard(bItem.Key)
	return tr.ReplaceOrInsert(item)
}
