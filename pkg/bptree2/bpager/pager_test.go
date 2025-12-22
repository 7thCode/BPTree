package bpager_test

import (
	"path/filepath"
	"testing"

	"github.com/oda/bptree2/pkg/bptree2/bpager"
)

func TestOpenClose(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	p, err := bpager.Open(path)
	if err != nil {
		t.Fatalf("bpager.Open failed: %v", err)
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

	p, err := bpager.Open(path)
	if err != nil {
		t.Fatalf("bpager.Open failed: %v", err)
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

	p, err := bpager.Open(path)
	if err != nil {
		t.Fatalf("bpager.Open failed: %v", err)
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
	if len(page) != bpager.PageSize {
		t.Errorf("expected page size %d, got %d", bpager.PageSize, len(page))
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
	p1, err := bpager.Open(path)
	if err != nil {
		t.Fatalf("bpager.Open failed: %v", err)
	}

	id, _ := p1.AllocatePage()
	page := p1.GetPage(id)
	copy(page[0:5], []byte("hello"))

	// Use rootID 0
	var rootID uint64 = 0
	p1.SetRootPage(rootID, id)
	p1.Checkpoint()
	p1.Close()

	// Reopen and verify
	p2, err := bpager.Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer p2.Close()

	if p2.GetRootPage(rootID) != id {
		t.Errorf("root page should be %d, got %d", id, p2.GetRootPage(rootID))
	}

	page2 := p2.GetPage(id)
	if string(page2[0:5]) != "hello" {
		t.Errorf("data should persist, got '%s'", string(page2[0:5]))
	}
}

func TestGrowth(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	p, err := bpager.Open(path)
	if err != nil {
		t.Fatalf("bpager.Open failed: %v", err)
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

func TestMultiRoots(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	p, err := bpager.Open(path)
	if err != nil {
		t.Fatalf("bpager.Open failed: %v", err)
	}
	defer p.Close()

	// Create multiple roots
	root1, err := p.CreateRoot()
	if err != nil {
		t.Fatalf("CreateRoot failed: %v", err)
	}
	root2, err := p.CreateRoot()
	if err != nil {
		t.Fatalf("CreateRoot failed: %v", err)
	}

	// Allocate pages and assign to roots
	page1, _ := p.AllocatePage()
	page2, _ := p.AllocatePage()

	p.SetRootPage(root1, page1)
	p.SetRootPage(root2, page2)

	// Verify
	if p.GetRootPage(root1) != page1 {
		t.Errorf("root1 should point to page %d", page1)
	}
	if p.GetRootPage(root2) != page2 {
		t.Errorf("root2 should point to page %d", page2)
	}

	// Delete root1
	p.DeleteRoot(root1)
	if p.GetRootPage(root1) != 0 {
		t.Error("root1 should be 0 after delete")
	}
	if p.GetRootPage(root2) != page2 {
		t.Error("root2 should not be affected")
	}
}
