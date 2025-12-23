package bmmap_test

import (
	"bptree2/bmmap"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenClose(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	m, err := bmmap.Open(path, 4096)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer m.Close()

	if m.Size() != 4096 {
		t.Errorf("expected size 4096, got %d", m.Size())
	}

	// Check file exists
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("file should exist: %v", err)
	}
	if info.Size() != 4096 {
		t.Errorf("file size should be 4096, got %d", info.Size())
	}
}

func TestReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	m, err := bmmap.Open(path, 4096)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data
	data := m.Data()
	copy(data[0:5], []byte("hello"))

	// Sync and close
	if err := m.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify
	m2, err := bmmap.Open(path, 4096)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer m2.Close()

	if string(m2.Data()[0:5]) != "hello" {
		t.Errorf("expected 'hello', got '%s'", string(m2.Data()[0:5]))
	}
}

func TestSlice(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	m, err := bmmap.Open(path, 4096)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer m.Close()

	// Valid slice
	slice := m.Slice(100, 50)
	if slice == nil {
		t.Fatal("Slice should not be nil")
	}
	if len(slice) != 50 {
		t.Errorf("expected length 50, got %d", len(slice))
	}

	// Invalid slices
	if m.Slice(-1, 10) != nil {
		t.Error("negative offset should return nil")
	}
	if m.Slice(4000, 200) != nil {
		t.Error("out of bounds should return nil")
	}
}

func TestGrow(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	m, err := bmmap.Open(path, 4096)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer m.Close()

	// Write some data
	copy(m.Data()[0:5], []byte("hello"))

	// Grow
	if err := m.Grow(8192); err != nil {
		t.Fatalf("Grow failed: %v", err)
	}

	if m.Size() != 8192 {
		t.Errorf("expected size 8192, got %d", m.Size())
	}

	// Verify data is preserved
	if string(m.Data()[0:5]) != "hello" {
		t.Errorf("data should be preserved after grow")
	}

	// Verify file size
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != 8192 {
		t.Errorf("file size should be 8192, got %d", info.Size())
	}
}
