package pager

import (
	"path/filepath"
	"testing"
)

func TestOpenClose(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if p.PageCount() != 1 {
		t.Errorf("expected page count 1, got %d", p.PageCount())
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestAllocatePage(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	// Allocate first page (should be page 1, since 0 is meta)
	id1, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	if id1 != 1 {
		t.Errorf("expected page ID 1, got %d", id1)
	}

	// Allocate second page
	id2, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	if id2 != 2 {
		t.Errorf("expected page ID 2, got %d", id2)
	}

	if p.PageCount() != 3 {
		t.Errorf("expected page count 3, got %d", p.PageCount())
	}
}

func TestGetPage(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	id, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	page := p.GetPage(id)
	if page == nil {
		t.Fatal("GetPage returned nil")
	}
	if len(page) != PageSize {
		t.Errorf("expected page size %d, got %d", PageSize, len(page))
	}

	// Write to page
	copy(page[0:5], []byte("hello"))

	// Checkpoint
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	// Create and write
	p1, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	id, _ := p1.AllocatePage()
	page := p1.GetPage(id)
	copy(page[0:5], []byte("hello"))
	p1.SetRootPage(id)
	p1.Checkpoint()
	p1.Close()

	// Reopen and verify
	p2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer p2.Close()

	if p2.RootPage() != id {
		t.Errorf("root page should be %d, got %d", id, p2.RootPage())
	}

	page2 := p2.GetPage(id)
	if string(page2[0:5]) != "hello" {
		t.Errorf("data should persist, got '%s'", string(page2[0:5]))
	}
}

func TestGrowth(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	p, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer p.Close()

	// Allocate many pages to trigger growth
	// Initial size is 1MB = 256 pages
	for i := 0; i < 300; i++ {
		_, err := p.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage failed at %d: %v", i, err)
		}
	}

	if p.PageCount() != 301 { // 300 allocated + meta page
		t.Errorf("expected page count 301, got %d", p.PageCount())
	}
}
