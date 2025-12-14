package node

import (
	"encoding/binary"
	"sort"
)

// LeafNode provides operations on a leaf node's raw byte slice.
// The layout is:
//   - Header: 16 bytes
//   - Keys: [uint64 × KeyCount] starting at offset 16
//   - Values: [uint64 × KeyCount] starting after keys
//
// For MaxLeafKeys=255, keys occupy bytes 16-2055, values 2056-4095.
type LeafNode struct {
	data []byte
}

// NewLeafNode creates a new leaf node wrapper around raw bytes.
// If init is true, initializes the node as empty.
func NewLeafNode(data []byte, init bool) *LeafNode {
	n := &LeafNode{data: data}
	if init {
		data[0] = byte(NodeTypeLeaf)
		setKeyCount(data, 0)
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

// keyOffset returns the byte offset for key at index i.
func (n *LeafNode) keyOffset(i int) int {
	return HeaderSize + i*8
}

// valueOffset returns the byte offset for value at index i.
func (n *LeafNode) valueOffset(i int) int {
	// Values start after all possible keys
	return HeaderSize + MaxLeafKeys*8 + i*8
}

// getKey returns the key at index i.
func (n *LeafNode) getKey(i int) uint64 {
	off := n.keyOffset(i)
	return binary.LittleEndian.Uint64(n.data[off : off+8])
}

// setKey sets the key at index i.
func (n *LeafNode) setKey(i int, key uint64) {
	off := n.keyOffset(i)
	binary.LittleEndian.PutUint64(n.data[off:off+8], key)
}

// getValue returns the value at index i.
func (n *LeafNode) getValue(i int) uint64 {
	off := n.valueOffset(i)
	return binary.LittleEndian.Uint64(n.data[off : off+8])
}

// setValue sets the value at index i.
func (n *LeafNode) setValue(i int, value uint64) {
	off := n.valueOffset(i)
	binary.LittleEndian.PutUint64(n.data[off:off+8], value)
}

// Search finds the index of the given key using binary search.
// Returns (index, found). If not found, index is where it should be inserted.
func (n *LeafNode) Search(key uint64) (int, bool) {
	count := n.KeyCount()
	idx := sort.Search(count, func(i int) bool {
		return n.getKey(i) >= key
	})
	if idx < count && n.getKey(idx) == key {
		return idx, true
	}
	return idx, false
}

// Get retrieves a value by key.
func (n *LeafNode) Get(key uint64) (uint64, bool) {
	idx, found := n.Search(key)
	if !found {
		return 0, false
	}
	return n.getValue(idx), true
}

// Put inserts or updates a key-value pair.
// Returns true if a new key was inserted, false if updated.
// Panics if the node is full and key doesn't exist.
func (n *LeafNode) Put(key, value uint64) bool {
	idx, found := n.Search(key)

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

	// Shift keys and values to make room
	for i := count; i > idx; i-- {
		n.setKey(i, n.getKey(i-1))
		n.setValue(i, n.getValue(i-1))
	}

	n.setKey(idx, key)
	n.setValue(idx, value)
	setKeyCount(n.data, uint16(count+1))

	return true
}

// Delete removes a key from the node.
// Returns true if the key was found and removed.
func (n *LeafNode) Delete(key uint64) bool {
	idx, found := n.Search(key)
	if !found {
		return false
	}

	count := n.KeyCount()

	// Shift keys and values to fill the gap
	for i := idx; i < count-1; i++ {
		n.setKey(i, n.getKey(i+1))
		n.setValue(i, n.getValue(i+1))
	}

	setKeyCount(n.data, uint16(count-1))
	return true
}

// Split splits the node into two, returning the middle key and new node.
// The new node contains the upper half of keys.
// Caller is responsible for providing the new node's data buffer.
func (n *LeafNode) Split(newData []byte) (uint64, *LeafNode) {
	count := n.KeyCount()
	mid := count / 2

	// Create new node
	newNode := NewLeafNode(newData, true)

	// Copy upper half to new node
	for i := mid; i < count; i++ {
		newNode.setKey(i-mid, n.getKey(i))
		newNode.setValue(i-mid, n.getValue(i))
	}
	setKeyCount(newData, uint16(count-mid))

	// Update original node count
	setKeyCount(n.data, uint16(mid))

	// Link leaves
	newNode.SetNextLeaf(n.NextLeaf())

	// Return the first key of the new node
	return newNode.getKey(0), newNode
}

// Range returns all key-value pairs where start <= key <= end.
func (n *LeafNode) Range(start, end uint64) []KVPair {
	var results []KVPair
	count := n.KeyCount()

	// Find starting position
	startIdx, _ := n.Search(start)

	for i := startIdx; i < count; i++ {
		key := n.getKey(i)
		if key > end {
			break
		}
		results = append(results, KVPair{
			Key:   key,
			Value: n.getValue(i),
		})
	}

	return results
}

// GetKeyAt returns the key at the given index.
func (n *LeafNode) GetKeyAt(idx int) uint64 {
	return n.getKey(idx)
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
// Returns the new separator key for the parent.
func (n *LeafNode) BorrowFromRight(right *LeafNode) uint64 {
	// Get the first key-value from right sibling
	key := right.getKey(0)
	value := right.getValue(0)

	// Append to this node
	count := n.KeyCount()
	n.setKey(count, key)
	n.setValue(count, value)
	setKeyCount(n.data, uint16(count+1))

	// Remove from right sibling
	right.Delete(key)

	// Return the new separator (first key of right sibling after borrow)
	return right.getKey(0)
}

// BorrowFromLeft borrows the last key from the left sibling.
// Returns the new separator key for the parent.
func (n *LeafNode) BorrowFromLeft(left *LeafNode) uint64 {
	leftCount := left.KeyCount()
	key := left.getKey(leftCount - 1)
	value := left.getValue(leftCount - 1)

	// Shift all keys in this node to make room at position 0
	count := n.KeyCount()
	for i := count; i > 0; i-- {
		n.setKey(i, n.getKey(i-1))
		n.setValue(i, n.getValue(i-1))
	}

	// Insert borrowed key at position 0
	n.setKey(0, key)
	n.setValue(0, value)
	setKeyCount(n.data, uint16(count+1))

	// Remove from left sibling
	setKeyCount(left.data, uint16(leftCount-1))

	// Return the new separator (first key of this node)
	return n.getKey(0)
}

// MergeWith merges the right sibling into this node.
// After merge, the right sibling should be freed.
func (n *LeafNode) MergeWith(right *LeafNode) {
	count := n.KeyCount()
	rightCount := right.KeyCount()

	// Copy all keys from right to this node
	for i := 0; i < rightCount; i++ {
		n.setKey(count+i, right.getKey(i))
		n.setValue(count+i, right.getValue(i))
	}

	setKeyCount(n.data, uint16(count+rightCount))

	// Update next leaf pointer
	n.SetNextLeaf(right.NextLeaf())
}
