// Package pager manages page-based storage using memory-mapped files.
package bpager

import (
	"encoding/binary"
)

const (
	// PageSize is the size of each page in bytes.
	// 4096 bytes is the standard OS page size and optimal for I/O.
	PageSize = 4096

	// MetaPageID is the page ID for the metadata page.
	MetaPageID PageID = 0

	// Magic number to identify BPTree files
	Magic uint32 = 0x42505452 // "BPTR"

	// Version of the file format (2 = multi-root support)
	Version uint32 = 2

	// MaxRoots is the maximum number of root trees supported
	MaxRoots = 500
)

// PageID is the identifier for a page.
type PageID = uint64

// RootID is the identifier for a root tree.
type RootID = uint64

// MetaPage represents the file header and metadata.
// Stored at page 0.
type MetaPage struct {
	Reserved  [8]byte          // Reserved for future use
	Magic     uint32           // File format magic number
	Version   uint32           // File format version
	RootCount uint64           // Number of active roots
	PageCount uint64           // Total number of allocated pages
	FreeList  PageID           // Head of free page list (0 if none)
	RootTable [MaxRoots]PageID // rootID → root page mapping
}

// MetaPageSize is the serialized size of MetaPage header (before RootTable).
const MetaPageHeaderSize = 8 + 4 + 4 + 8 + 8 + 8 // 40 bytes

// Serialize writes the meta page to a byte slice.
func (m *MetaPage) Serialize(buf []byte) {
	// Bytes 0-7 are reserved
	binary.BigEndian.PutUint32(buf[8:12], m.Magic)
	binary.BigEndian.PutUint32(buf[12:16], m.Version)
	binary.BigEndian.PutUint64(buf[16:24], m.RootCount)
	binary.BigEndian.PutUint64(buf[24:32], m.PageCount)
	binary.BigEndian.PutUint64(buf[32:40], m.FreeList)

	// Serialize RootTable
	offset := 40
	for i := 0; i < MaxRoots && offset+8 <= PageSize; i++ {
		binary.BigEndian.PutUint64(buf[offset:offset+8], m.RootTable[i])
		offset += 8
	}
}

// Deserialize reads the meta page from a byte slice.
func (m *MetaPage) Deserialize(buf []byte) {
	// Bytes 0-7 are reserved
	m.Magic = binary.BigEndian.Uint32(buf[8:12])
	m.Version = binary.BigEndian.Uint32(buf[12:16])
	m.RootCount = binary.BigEndian.Uint64(buf[16:24])
	m.PageCount = binary.BigEndian.Uint64(buf[24:32])
	m.FreeList = binary.BigEndian.Uint64(buf[32:40])

	// Deserialize RootTable
	offset := 40
	for i := 0; i < MaxRoots && offset+8 <= PageSize; i++ {
		m.RootTable[i] = binary.BigEndian.Uint64(buf[offset : offset+8])
		offset += 8
	}
}

// GetRootPage returns the root page for a given rootID.
// Returns 0 if the rootID is invalid or not set.
func (m *MetaPage) GetRootPage(rootID RootID) PageID {
	if rootID >= MaxRoots {
		return 0
	}
	return m.RootTable[rootID]
}

// SetRootPage sets the root page for a given rootID.
// Returns false if rootID is invalid.
func (m *MetaPage) SetRootPage(rootID RootID, pageID PageID) bool {
	if rootID >= MaxRoots {
		return false
	}
	m.RootTable[rootID] = pageID
	return true
}
