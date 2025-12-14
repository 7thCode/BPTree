// Package node provides B+Tree node operations.
package node

import (
	"encoding/binary"
)

const (
	// HeaderSize is the size of the node header in bytes.
	HeaderSize = 16

	// UsableSize is the space available for keys/values in a page.
	UsableSize = 4096 - HeaderSize // 4080 bytes

	// MaxLeafKeys is the maximum number of keys in a leaf node.
	// Each key-value pair is 16 bytes (8 + 8).
	MaxLeafKeys = UsableSize / 16 // 255

	// MinLeafKeys is the minimum number of keys in a leaf node (except root).
	// Should be at least ceil(MaxLeafKeys/2) - 1 for B+Tree invariant.
	MinLeafKeys = MaxLeafKeys / 2 // 127

	// MaxInternalKeys is the maximum number of keys in an internal node.
	// Each key is 8 bytes, plus we need (N+1) child pointers at 8 bytes each.
	// So: 8*N + 8*(N+1) = 16N + 8 <= 4080 => N <= 254
	MaxInternalKeys = 254

	// MinInternalKeys is the minimum number of keys in an internal node (except root).
	MinInternalKeys = MaxInternalKeys / 2 // 127
)

// NodeType indicates the type of node.
type NodeType uint8

const (
	// NodeTypeInternal represents an internal (branch) node.
	NodeTypeInternal NodeType = 0
	// NodeTypeLeaf represents a leaf node.
	NodeTypeLeaf NodeType = 1
)

// KVPair represents a key-value pair.
type KVPair struct {
	Key   uint64
	Value uint64
}

// Header layout:
// Byte 0: NodeType (1 byte)
// Byte 1-2: KeyCount (2 bytes, little endian)
// Byte 3-10: NextLeaf for leaf, unused for internal (8 bytes)
// Byte 11-15: Reserved (5 bytes)

// GetNodeType returns the type of the node from raw bytes.
func GetNodeType(data []byte) NodeType {
	return NodeType(data[0])
}

// GetKeyCount returns the number of keys in the node.
func GetKeyCount(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data[1:3])
}

// setKeyCount sets the number of keys in the node.
func setKeyCount(data []byte, count uint16) {
	binary.LittleEndian.PutUint16(data[1:3], count)
}

// getNextLeaf returns the next leaf pointer (only valid for leaf nodes).
func getNextLeaf(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[3:11])
}

// setNextLeaf sets the next leaf pointer.
func setNextLeaf(data []byte, next uint64) {
	binary.LittleEndian.PutUint64(data[3:11], next)
}
