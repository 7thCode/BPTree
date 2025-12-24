package bnode

import (
	"encoding/binary"
	"sort"
)

// LeafNode provides operations on a leaf node's raw byte slice.
// The layout is:
//   - Header: 16 bytes
//   - Entries: [Key1: 8, Key2: 8, Value: 8] × KeyCount starting at offset 16
//
// For MaxLeafKeys=170, each entry is 24 bytes, total data = 4080 bytes.
type LeafNode struct {
	data []byte
}

// NewLeafNode creates a new leaf node wrapper around raw bytes.
// If init is true, initializes the node as empty.
func NewLeafNode(data []byte, init bool) *LeafNode {
	n := &LeafNode{data: data}
	if init {
		data[0] = byte(NodeTypeLeaf)
		SetKeyCount(data, 0)
		setNextLeaf(data, 0)
	}
	return n
}

// Type returns the node type.
func (n *LeafNode) Type() NodeType {
	return GetNodeType(n.data)
}

// KeyCount returns the number of keys in this node.
func (n *LeafNode) KeyCount() int {
	return int(GetKeyCount(n.data))
}

// IsFull returns true if the node cannot accept more keys.
func (n *LeafNode) IsFull() bool {
	return n.KeyCount() >= MaxLeafKeys
}

// NextLeaf returns the page ID of the next leaf node.
func (n *LeafNode) NextLeaf() uint64 {
	return getNextLeaf(n.data)
}

// SetNextLeaf sets the next leaf page ID.
func (n *LeafNode) SetNextLeaf(pageID uint64) {
	setNextLeaf(n.data, pageID)
}

// entryOffset returns the byte offset for entry at index i.
// Each entry is 24 bytes (Key1: 8 + Key2: 8 + Value: 8).
func (n *LeafNode) entryOffset(i int) int {
	return HeaderSize + i*24
}

// GetKey1 returns the key1 at index i.
func (n *LeafNode) GetKey1(i int) uint64 {
	off := n.entryOffset(i)
	return binary.BigEndian.Uint64(n.data[off : off+8])
}

// GetKey2 returns the key2 at index i.
func (n *LeafNode) GetKey2(i int) uint64 {
	off := n.entryOffset(i) + 8
	return binary.BigEndian.Uint64(n.data[off : off+8])
}

// setKey1 sets the key1 at index i.
func (n *LeafNode) setKey1(i int, key1 uint64) {
	off := n.entryOffset(i)
	binary.BigEndian.PutUint64(n.data[off:off+8], key1)
}

// setKey2 sets the key2 at index i.
func (n *LeafNode) setKey2(i int, key2 uint64) {
	off := n.entryOffset(i) + 8
	binary.BigEndian.PutUint64(n.data[off:off+8], key2)
}

// getValue returns the value at index i.
func (n *LeafNode) getValue(i int) uint64 {
	off := n.entryOffset(i) + 16
	return binary.BigEndian.Uint64(n.data[off : off+8])
}

// setValue sets the value at index i.
func (n *LeafNode) setValue(i int, value uint64) {
	off := n.entryOffset(i) + 16
	binary.BigEndian.PutUint64(n.data[off:off+8], value)
}

// compareKeys compares two composite keys.
// Returns -1 if (a1,a2) < (b1,b2), 0 if equal, 1 if greater.
func compareKeys(a1, a2, b1, b2 uint64) int {
	if a1 < b1 {
		return -1
	} else if a1 > b1 {
		return 1
	}
	// a1 == b1, compare a2 and b2
	if a2 < b2 {
		return -1
	} else if a2 > b2 {
		return 1
	}
	return 0
}

// Search finds the index of the given composite key using binary search.
// Returns (index, found). If not found, index is where it should be inserted.
func (n *LeafNode) Search(key1, key2 uint64) (int, bool) {
	count := n.KeyCount()
	idx := sort.Search(count, func(i int) bool {
		return compareKeys(n.GetKey1(i), n.GetKey2(i), key1, key2) >= 0
	})
	if idx < count && n.GetKey1(idx) == key1 && n.GetKey2(idx) == key2 {
		return idx, true
	}
	return idx, false
}

// Get retrieves a value by composite key.
func (n *LeafNode) Get(key1, key2 uint64) (uint64, bool) {
	idx, found := n.Search(key1, key2)
	if !found {
		return 0, false
	}
	return n.getValue(idx), true
}

// Put inserts or updates a key-value pair with composite key.
// Returns true if a new key was inserted, false if updated.
// Panics if the node is full and key doesn't exist.
func (n *LeafNode) Put(key1, key2, value uint64) bool {
	idx, found := n.Search(key1, key2)

	if found {
		// Update existing key
		n.setValue(idx, value)
		return false
	}

	// Insert new key
	count := n.KeyCount()
	if count >= MaxLeafKeys {
		panic("leaf node is full")
	}

	// Shift entries to make room
	for i := count; i > idx; i-- {
		n.setKey1(i, n.GetKey1(i-1))
		n.setKey2(i, n.GetKey2(i-1))
		n.setValue(i, n.getValue(i-1))
	}

	n.setKey1(idx, key1)
	n.setKey2(idx, key2)
	n.setValue(idx, value)
	SetKeyCount(n.data, uint16(count+1))

	return true
}

// Delete removes a composite key from the node.
// Returns true if the key was found and removed.
func (n *LeafNode) Delete(key1, key2 uint64) bool {
	idx, found := n.Search(key1, key2)
	if !found {
		return false
	}

	count := n.KeyCount()

	// Shift entries to fill the gap
	for i := idx; i < count-1; i++ {
		n.setKey1(i, n.GetKey1(i+1))
		n.setKey2(i, n.GetKey2(i+1))
		n.setValue(i, n.getValue(i+1))
	}

	SetKeyCount(n.data, uint16(count-1))
	return true
}

// Split splits the node into two, returning the middle key1 and new node.
// The new node contains the upper half of keys.
// Caller is responsible for providing the new node's data buffer.
func (n *LeafNode) Split(newData []byte) (uint64, *LeafNode) {
	count := n.KeyCount()
	mid := count / 2

	// Create new node
	newNode := NewLeafNode(newData, true)

	// Copy upper half to new node
	for i := mid; i < count; i++ {
		newNode.setKey1(i-mid, n.GetKey1(i))
		newNode.setKey2(i-mid, n.GetKey2(i))
		newNode.setValue(i-mid, n.getValue(i))
	}
	SetKeyCount(newData, uint16(count-mid))

	// Update original node count
	SetKeyCount(n.data, uint16(mid))

	// Link leaves
	newNode.SetNextLeaf(n.NextLeaf())

	// Return the first key1 of the new node (used for separator)
	return newNode.GetKey1(0), newNode
}

// Range returns all key-value pairs where (start1,start2) <= (key1,key2) <= (end1,end2).
func (n *LeafNode) Range(start1, start2, end1, end2 uint64) []KVPair {
	var results []KVPair
	count := n.KeyCount()

	// Find starting position
	startIdx, _ := n.Search(start1, start2)

	for i := startIdx; i < count; i++ {
		key1 := n.GetKey1(i)
		key2 := n.GetKey2(i)
		if compareKeys(key1, key2, end1, end2) > 0 {
			break
		}
		results = append(results, KVPair{
			Key1:  key1,
			Key2:  key2,
			Value: n.getValue(i),
		})
	}

	return results
}

// GetKey1At returns the key1 at the given index.
func (n *LeafNode) GetKey1At(idx int) uint64 {
	return n.GetKey1(idx)
}

// GetKey2At returns the key2 at the given index.
func (n *LeafNode) GetKey2At(idx int) uint64 {
	return n.GetKey2(idx)
}

// GetValueAt returns the value at the given index.
func (n *LeafNode) GetValueAt(idx int) uint64 {
	return n.getValue(idx)
}

// IsUnderflow returns true if the node has fewer than minimum keys.
// Root nodes are exempt from minimum key requirements.
func (n *LeafNode) IsUnderflow() bool {
	return n.KeyCount() < MinLeafKeys
}

// CanLendTo returns true if this node can lend a key to a sibling.
func (n *LeafNode) CanLendTo() bool {
	return n.KeyCount() > MinLeafKeys
}

// BorrowFromRight borrows the first key from the right sibling.
// Returns the new separator key1 for the parent.
func (n *LeafNode) BorrowFromRight(right *LeafNode) uint64 {
	// Get the first entry from right sibling
	key1 := right.GetKey1(0)
	key2 := right.GetKey2(0)
	value := right.getValue(0)

	// Append to this node
	count := n.KeyCount()
	n.setKey1(count, key1)
	n.setKey2(count, key2)
	n.setValue(count, value)
	SetKeyCount(n.data, uint16(count+1))

	// Remove from right sibling
	right.Delete(key1, key2)

	// Return the new separator (first key1 of right sibling after borrow)
	return right.GetKey1(0)
}

// BorrowFromLeft borrows the last key from the left sibling.
// Returns the new separator key1 for the parent.
func (n *LeafNode) BorrowFromLeft(left *LeafNode) uint64 {
	leftCount := left.KeyCount()
	key1 := left.GetKey1(leftCount - 1)
	key2 := left.GetKey2(leftCount - 1)
	value := left.getValue(leftCount - 1)

	// Shift all entries in this node to make room at position 0
	count := n.KeyCount()
	for i := count; i > 0; i-- {
		n.setKey1(i, n.GetKey1(i-1))
		n.setKey2(i, n.GetKey2(i-1))
		n.setValue(i, n.getValue(i-1))
	}

	// Insert borrowed entry at position 0
	n.setKey1(0, key1)
	n.setKey2(0, key2)
	n.setValue(0, value)
	SetKeyCount(n.data, uint16(count+1))

	// Remove from left sibling
	SetKeyCount(left.data, uint16(leftCount-1))

	// Return the new separator (first key1 of this node)
	return n.GetKey1(0)
}

// MergeWith merges the right sibling into this node.
// After merge, the right sibling should be freed.
func (n *LeafNode) MergeWith(right *LeafNode) {
	count := n.KeyCount()
	rightCount := right.KeyCount()

	// Copy all entries from right to this node
	for i := 0; i < rightCount; i++ {
		n.setKey1(count+i, right.GetKey1(i))
		n.setKey2(count+i, right.GetKey2(i))
		n.setValue(count+i, right.getValue(i))
	}

	SetKeyCount(n.data, uint16(count+rightCount))

	// Update next leaf pointer
	n.SetNextLeaf(right.NextLeaf())
}
