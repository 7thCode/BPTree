package bnode

import (
	"encoding/binary"
	"sort"
)

// InternalNode provides operations on an internal (branch) node's raw byte slice.
// The layout is:
//   - Header: 16 bytes
//   - Children: [(N+1) × uint64] starting at offset 16
//   - Keys: [N × uint64] starting after children
//
// For MaxInternalKeys=254:
//   - Children (255): bytes 16-2055 (255 * 8 = 2040 bytes)
//   - Keys (254): bytes 2056-4087 (254 * 8 = 2032 bytes)
type InternalNode struct {
	data []byte
}

// NewInternalNode creates a new internal node wrapper around raw bytes.
// If init is true, initializes the node as empty.
func NewInternalNode(data []byte, init bool) *InternalNode {
	n := &InternalNode{data: data}
	if init {
		data[0] = byte(NodeTypeInternal)
		setKeyCount(data, 0)
	}
	return n
}

// Type returns the node type.
func (n *InternalNode) Type() NodeType {
	return GetNodeType(n.data)
}

// KeyCount returns the number of keys in this node.
func (n *InternalNode) KeyCount() int {
	return int(GetKeyCount(n.data))
}

// IsFull returns true if the node cannot accept more keys.
func (n *InternalNode) IsFull() bool {
	return n.KeyCount() >= MaxInternalKeys
}

// childOffset returns the byte offset for child pointer at index i.
// There are (KeyCount + 1) children.
func (n *InternalNode) childOffset(i int) int {
	return HeaderSize + i*8
}

// keyOffset returns the byte offset for key at index i.
func (n *InternalNode) keyOffset(i int) int {
	// Keys start after all possible children (255 children max)
	return HeaderSize + (MaxInternalKeys+1)*8 + i*8
}

// GetChild returns the child page ID at index i.
func (n *InternalNode) GetChild(i int) uint64 {
	off := n.childOffset(i)
	return binary.LittleEndian.Uint64(n.data[off : off+8])
}

// setChild sets the child page ID at index i.
func (n *InternalNode) setChild(i int, pageID uint64) {
	off := n.childOffset(i)
	binary.LittleEndian.PutUint64(n.data[off:off+8], pageID)
}

// getKey returns the key at index i.
func (n *InternalNode) getKey(i int) uint64 {
	off := n.keyOffset(i)
	return binary.LittleEndian.Uint64(n.data[off : off+8])
}

// setKey sets the key at index i.
func (n *InternalNode) setKey(i int, key uint64) {
	off := n.keyOffset(i)
	binary.LittleEndian.PutUint64(n.data[off:off+8], key)
}

// Search finds the child index for the given key.
// Returns the index of the child pointer to follow.
func (n *InternalNode) Search(key uint64) int {
	count := n.KeyCount()

	// Find the first key greater than the search key
	idx := sort.Search(count, func(i int) bool {
		return n.getKey(i) > key
	})

	return idx
}

// GetChildForKey returns the child page ID that should contain the given key.
func (n *InternalNode) GetChildForKey(key uint64) uint64 {
	idx := n.Search(key)
	return n.GetChild(idx)
}

// Insert inserts a key with its right child pointer.
// The left child should already be in place.
// Returns true if inserted successfully.
func (n *InternalNode) Insert(key uint64, rightChild uint64) bool {
	count := n.KeyCount()
	if count >= MaxInternalKeys {
		return false
	}

	// Find insertion point
	idx := sort.Search(count, func(i int) bool {
		return n.getKey(i) > key
	})

	// Shift keys and children to make room
	for i := count; i > idx; i-- {
		n.setKey(i, n.getKey(i-1))
		n.setChild(i+1, n.GetChild(i))
	}

	n.setKey(idx, key)
	n.setChild(idx+1, rightChild)
	setKeyCount(n.data, uint16(count+1))

	return true
}

// InitRoot initializes an internal node as a root with one key and two children.
func (n *InternalNode) InitRoot(leftChild, rightChild uint64, key uint64) {
	n.data[0] = byte(NodeTypeInternal)
	setKeyCount(n.data, 1)
	n.setChild(0, leftChild)
	n.setChild(1, rightChild)
	n.setKey(0, key)
}

// Split splits the node into two, returning the middle key and new node.
// The middle key should be promoted to the parent.
// Caller is responsible for providing the new node's data buffer.
func (n *InternalNode) Split(newData []byte) (uint64, *InternalNode) {
	count := n.KeyCount()
	mid := count / 2

	// Create new node
	newNode := NewInternalNode(newData, true)

	// The middle key will be promoted
	midKey := n.getKey(mid)

	// Copy upper half to new node (keys after mid, and corresponding children)
	newKeyCount := count - mid - 1
	for i := 0; i < newKeyCount; i++ {
		newNode.setKey(i, n.getKey(mid+1+i))
	}
	for i := 0; i <= newKeyCount; i++ {
		newNode.setChild(i, n.GetChild(mid+1+i))
	}
	setKeyCount(newData, uint16(newKeyCount))

	// Update original node count (keys before mid)
	setKeyCount(n.data, uint16(mid))

	return midKey, newNode
}

// GetKeyAt returns the key at the given index.
func (n *InternalNode) GetKeyAt(idx int) uint64 {
	return n.getKey(idx)
}

// SetKeyAt sets the key at the given index.
func (n *InternalNode) SetKeyAt(idx int, key uint64) {
	n.setKey(idx, key)
}

// SetChild sets the child page ID at index i (public accessor).
func (n *InternalNode) SetChild(i int, pageID uint64) {
	n.setChild(i, pageID)
}

// IsUnderflow returns true if the node has fewer than minimum keys.
func (n *InternalNode) IsUnderflow() bool {
	return n.KeyCount() < MinInternalKeys
}

// CanLendTo returns true if this node can lend a key to a sibling.
func (n *InternalNode) CanLendTo() bool {
	return n.KeyCount() > MinInternalKeys
}

// DeleteKeyAt removes the key and its right child at the given index.
func (n *InternalNode) DeleteKeyAt(idx int) {
	count := n.KeyCount()

	// Shift keys and children to fill the gap
	for i := idx; i < count-1; i++ {
		n.setKey(i, n.getKey(i+1))
		n.setChild(i+1, n.GetChild(i+2))
	}

	setKeyCount(n.data, uint16(count-1))
}

// BorrowFromRight borrows the first key from the right sibling.
// parentKey is the current separator in parent between this and right.
// Returns the new separator key for the parent.
func (n *InternalNode) BorrowFromRight(right *InternalNode, parentKey uint64) uint64 {
	count := n.KeyCount()

	// Add parent key to end of this node
	n.setKey(count, parentKey)
	// Add right's first child as our new last child
	n.setChild(count+1, right.GetChild(0))
	setKeyCount(n.data, uint16(count+1))

	// New parent key is right's first key
	newParentKey := right.getKey(0)

	// Remove first key and child from right
	rightCount := right.KeyCount()
	for i := 0; i < rightCount-1; i++ {
		right.setKey(i, right.getKey(i+1))
	}
	for i := 0; i < rightCount; i++ {
		right.setChild(i, right.GetChild(i+1))
	}
	setKeyCount(right.data, uint16(rightCount-1))

	return newParentKey
}

// BorrowFromLeft borrows the last key from the left sibling.
// parentKey is the current separator in parent between left and this.
// Returns the new separator key for the parent.
func (n *InternalNode) BorrowFromLeft(left *InternalNode, parentKey uint64) uint64 {
	count := n.KeyCount()
	leftCount := left.KeyCount()

	// Shift all keys and children in this node to make room at position 0
	for i := count; i > 0; i-- {
		n.setKey(i, n.getKey(i-1))
	}
	for i := count + 1; i > 0; i-- {
		n.setChild(i, n.GetChild(i-1))
	}

	// Insert parent key at position 0
	n.setKey(0, parentKey)
	// Insert left's last child as our new first child
	n.setChild(0, left.GetChild(leftCount))
	setKeyCount(n.data, uint16(count+1))

	// New parent key is left's last key
	newParentKey := left.getKey(leftCount - 1)

	// Remove last key from left (child is moved to this node)
	setKeyCount(left.data, uint16(leftCount-1))

	return newParentKey
}

// MergeWith merges the right sibling into this node using parentKey as separator.
// After merge, the right sibling should be freed.
func (n *InternalNode) MergeWith(right *InternalNode, parentKey uint64) {
	count := n.KeyCount()
	rightCount := right.KeyCount()

	// Add parent key
	n.setKey(count, parentKey)

	// Copy all keys from right
	for i := 0; i < rightCount; i++ {
		n.setKey(count+1+i, right.getKey(i))
	}

	// Copy all children from right
	for i := 0; i <= rightCount; i++ {
		n.setChild(count+1+i, right.GetChild(i))
	}

	setKeyCount(n.data, uint16(count+1+rightCount))
}
