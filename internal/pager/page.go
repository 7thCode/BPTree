// Package pager manages page-based storage using memory-mapped files.
package pager

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

	// Version of the file format
	Version uint32 = 1
)

// PageID is the identifier for a page.
type PageID = uint64

// MetaPage represents the file header and metadata.
// Stored at page 0.
type MetaPage struct {
	Magic     uint32 // File format magic number
	Version   uint32 // File format version
	RootPage  PageID // Root node page ID (0 if empty tree)
	PageCount uint64 // Total number of allocated pages
	FreeList  PageID // Head of free page list (0 if none)
}

// MetaPageSize is the serialized size of MetaPage.
const MetaPageSize = 4 + 4 + 8 + 8 + 8 // 32 bytes

// Serialize writes the meta page to a byte slice.
func (m *MetaPage) Serialize(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], m.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], m.Version)
	binary.LittleEndian.PutUint64(buf[8:16], m.RootPage)
	binary.LittleEndian.PutUint64(buf[16:24], m.PageCount)
	binary.LittleEndian.PutUint64(buf[24:32], m.FreeList)
}

// Deserialize reads the meta page from a byte slice.
func (m *MetaPage) Deserialize(buf []byte) {
	m.Magic = binary.LittleEndian.Uint32(buf[0:4])
	m.Version = binary.LittleEndian.Uint32(buf[4:8])
	m.RootPage = binary.LittleEndian.Uint64(buf[8:16])
	m.PageCount = binary.LittleEndian.Uint64(buf[16:24])
	m.FreeList = binary.LittleEndian.Uint64(buf[24:32])
}
