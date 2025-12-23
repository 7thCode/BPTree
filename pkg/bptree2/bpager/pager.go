package bpager

import (
	"encoding/binary"
	"fmt"
	"sync"

	"bptree2/bmmap"
)

const (
	// InitialFileSize is the initial size of the database file (1MB).
	InitialFileSize = 1024 * 1024

	// GrowthFactor determines how much to grow the file when expanding.
	GrowthFactor = 2

	// ReservedMarker is a special value marking a root slot as reserved but empty.
	ReservedMarker PageID = ^PageID(0)
)

// Pager manages page-based I/O using memory-mapped files.
type Pager struct {
	mmap *bmmap.MMap
	meta *MetaPage
	mu   sync.RWMutex // Protects meta and page allocation
}

// Open opens or creates a database file.
func Open(path string) (*Pager, error) {
	// Open the mmap file
	m, err := bmmap.Open(path, InitialFileSize)
	if err != nil {
		return nil, fmt.Errorf("failed to open mmap: %w", err)
	}

	p := &Pager{
		mmap: m,
		meta: &MetaPage{},
	}

	// Read or initialize metadata
	if err := p.loadOrInitMeta(); err != nil {
		m.Close()
		return nil, err
	}

	return p, nil
}

// loadOrInitMeta loads existing metadata or initializes a new file.
func (p *Pager) loadOrInitMeta() error {
	data := p.mmap.Slice(0, PageSize)
	if data == nil {
		return fmt.Errorf("failed to read meta page")
	}

	p.meta.Deserialize(data)

	// Check if this is a new file
	if p.meta.Magic == 0 {
		// Initialize new file
		p.meta.Magic = Magic
		p.meta.Version = Version
		p.meta.RootCount = 0
		p.meta.PageCount = 1 // Meta page is page 0
		p.meta.FreeList = 0
		// RootTable is already zeroed
		p.writeMeta()
	} else if p.meta.Magic != Magic {
		return fmt.Errorf("invalid file format: bad magic number")
	} else if p.meta.Version != Version {
		return fmt.Errorf("unsupported version: %d (expected %d)", p.meta.Version, Version)
	}

	return nil
}

// writeMeta writes the metadata to the meta page.
func (p *Pager) writeMeta() {
	data := p.mmap.Slice(0, PageSize)
	p.meta.Serialize(data)
}

// Close closes the pager and underlying file.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.mmap.Close()
}

// GetPage returns a byte slice for the given page ID.
// The returned slice is only valid until Close or Grow is called.
func (p *Pager) GetPage(id PageID) []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()

	offset := int64(id) * PageSize
	return p.mmap.Slice(offset, PageSize)
}

// AllocatePage allocates a new page and returns its ID.
func (p *Pager) AllocatePage() (PageID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check free list first
	if p.meta.FreeList != 0 {
		pageID := p.meta.FreeList

		// Get the next free page from the freed page's header
		data := p.mmap.Slice(int64(pageID)*PageSize, PageSize)
		nextFree := binary.BigEndian.Uint64(data[0:8])

		p.meta.FreeList = nextFree
		p.writeMeta()

		// Clear the page
		for i := range data {
			data[i] = 0
		}

		return pageID, nil
	}

	// Calculate required size for new page
	newPageID := PageID(p.meta.PageCount)
	requiredSize := int64(newPageID+1) * PageSize

	// Grow file if necessary
	if requiredSize > p.mmap.Size() {
		newSize := p.mmap.Size() * GrowthFactor
		for newSize < requiredSize {
			newSize *= GrowthFactor
		}
		if err := p.mmap.Grow(newSize); err != nil {
			return 0, fmt.Errorf("failed to grow file: %w", err)
		}
	}

	p.meta.PageCount++
	p.writeMeta()

	return newPageID, nil
}

// GetRootPage returns the root page ID for a given rootID.
// Returns 0 if the rootID is invalid or the tree doesn't exist.
func (p *Pager) GetRootPage(rootID RootID) PageID {
	p.mu.RLock()
	defer p.mu.RUnlock()
	page := p.meta.GetRootPage(rootID)
	// Reserved marker means empty tree
	if page == ReservedMarker {
		return 0
	}
	return page
}

// SetRootPage sets the root page ID for a given rootID.
func (p *Pager) SetRootPage(rootID RootID, pageID PageID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.meta.SetRootPage(rootID, pageID) {
		return fmt.Errorf("invalid rootID: %d (max: %d)", rootID, MaxRoots-1)
	}
	p.writeMeta()
	return nil
}

// CreateRoot creates a new root and returns its ID.
// Returns error if maximum roots reached.
func (p *Pager) CreateRoot() (RootID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Find first available slot (0 means unused)
	for i := RootID(0); i < MaxRoots; i++ {
		if p.meta.RootTable[i] == 0 {
			// Mark as reserved (not free, but empty tree)
			p.meta.RootTable[i] = ReservedMarker
			p.meta.RootCount++
			p.writeMeta()
			return i, nil
		}
	}

	return 0, fmt.Errorf("maximum roots reached: %d", MaxRoots)
}

// DeleteRoot deletes a root tree.
// Note: This only removes the root reference, does not free pages.
func (p *Pager) DeleteRoot(rootID RootID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if rootID >= MaxRoots {
		return fmt.Errorf("invalid rootID: %d", rootID)
	}

	if p.meta.RootTable[rootID] != 0 {
		p.meta.RootTable[rootID] = 0
		if p.meta.RootCount > 0 {
			p.meta.RootCount--
		}
		p.writeMeta()
	}

	return nil
}

// RootCount returns the number of active roots.
func (p *Pager) RootCount() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.meta.RootCount
}

// PageCount returns the total number of allocated pages.
func (p *Pager) PageCount() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.meta.PageCount
}

// Checkpoint syncs all changes to disk.
func (p *Pager) Checkpoint() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.writeMeta()
	return p.mmap.Sync()
}

// FreePage adds a page to the free list.
func (p *Pager) FreePage(id PageID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Store the current free list head in this page
	data := p.mmap.Slice(int64(id)*PageSize, PageSize)
	if data == nil {
		return fmt.Errorf("failed to get page %d for freeing", id)
	}

	// Clear page and store next free page pointer
	for i := range data {
		data[i] = 0
	}
	binary.BigEndian.PutUint64(data[0:8], p.meta.FreeList)

	// Update free list head
	p.meta.FreeList = id
	p.writeMeta()

	return nil
}
