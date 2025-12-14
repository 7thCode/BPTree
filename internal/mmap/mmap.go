// Package mmap provides memory-mapped file I/O.
package mmap

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// MMap represents a memory-mapped file.
type MMap struct {
	file *os.File
	data []byte
	size int64
}

// Open opens or creates a file and maps it into memory.
// If the file doesn't exist, it will be created with the given size.
// If the file exists but is smaller than size, it will be extended.
func Open(path string, size int64) (*MMap, error) {
	// Open or create file
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get current file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Extend file if necessary
	currentSize := info.Size()
	if currentSize < size {
		if err := file.Truncate(size); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to extend file: %w", err)
		}
		currentSize = size
	}

	// Memory map the file
	data, err := unix.Mmap(int(file.Fd()), 0, int(currentSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap: %w", err)
	}

	return &MMap{
		file: file,
		data: data,
		size: currentSize,
	}, nil
}

// Close unmaps and closes the file.
func (m *MMap) Close() error {
	if m.data != nil {
		if err := unix.Munmap(m.data); err != nil {
			return fmt.Errorf("failed to munmap: %w", err)
		}
		m.data = nil
	}
	if m.file != nil {
		if err := m.file.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}
		m.file = nil
	}
	return nil
}

// Sync flushes changes to disk.
func (m *MMap) Sync() error {
	if m.data == nil {
		return fmt.Errorf("mmap is closed")
	}
	return unix.Msync(m.data, unix.MS_SYNC)
}

// Size returns the current mapped size.
func (m *MMap) Size() int64 {
	return m.size
}

// Data returns the underlying byte slice.
// WARNING: Do not keep references to this slice after Close is called.
func (m *MMap) Data() []byte {
	return m.data
}

// Slice returns a slice of the mapped memory.
// Returns nil if the range is invalid.
func (m *MMap) Slice(offset, length int64) []byte {
	if m.data == nil {
		return nil
	}
	if offset < 0 || length < 0 || offset+length > m.size {
		return nil
	}
	return m.data[offset : offset+length]
}

// Grow extends the file and remaps it.
// This invalidates any previously returned slices.
func (m *MMap) Grow(newSize int64) error {
	if newSize <= m.size {
		return nil // No need to grow
	}

	// Unmap current mapping
	if err := unix.Munmap(m.data); err != nil {
		return fmt.Errorf("failed to munmap during grow: %w", err)
	}

	// Extend file
	if err := m.file.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to extend file during grow: %w", err)
	}

	// Remap with new size
	data, err := unix.Mmap(int(m.file.Fd()), 0, int(newSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to remap during grow: %w", err)
	}

	m.data = data
	m.size = newSize
	return nil
}
