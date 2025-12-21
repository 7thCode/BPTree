package bptree

import (
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

func TestBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	// Insert
	if err := tree.Insert(10, 100); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := tree.Insert(5, 50); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := tree.Insert(15, 150); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Find
	val, found := tree.Find(10)
	if !found {
		t.Error("key 10 should be found")
	}
	if val != 100 {
		t.Errorf("expected 100, got %d", val)
	}

	// Find non-existent
	_, found = tree.Find(20)
	if found {
		t.Error("key 20 should not be found")
	}

	// Update
	if err := tree.Insert(10, 200); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	val, _ = tree.Find(10)
	if val != 200 {
		t.Errorf("expected 200 after update, got %d", val)
	}
}

func TestLargeInsert(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	n := 10000

	// Insert n keys
	for i := 0; i < n; i++ {
		if err := tree.Insert(uint64(i), uint64(i*10)); err != nil {
			t.Fatalf("Insert failed at %d: %v", i, err)
		}
	}

	// Verify all keys
	for i := 0; i < n; i++ {
		val, found := tree.Find(uint64(i))
		if !found {
			t.Fatalf("key %d should be found", i)
		}
		if val != uint64(i*10) {
			t.Fatalf("key %d: expected %d, got %d", i, i*10, val)
		}
	}

	// Verify count
	count := tree.Count()
	if count != n {
		t.Errorf("expected count %d, got %d", n, count)
	}
}

func TestRandomInsert(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	// Generate random keys
	n := 5000
	keys := make([]uint64, n)
	for i := range keys {
		keys[i] = rand.Uint64()
	}

	// Insert
	for i, k := range keys {
		if err := tree.Insert(k, uint64(i)); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Verify
	for i, k := range keys {
		val, found := tree.Find(k)
		if !found {
			t.Fatalf("key %d should be found", k)
		}
		if val != uint64(i) {
			t.Fatalf("key %d: expected %d, got %d", k, i, val)
		}
	}
}

func TestRangeScan(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	// Insert 100 keys
	for i := 1; i <= 100; i++ {
		if err := tree.Insert(uint64(i), uint64(i*10)); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// FindRange [30, 50]
	var results []uint64
	err = tree.FindRange(30, 50, func(key, value uint64) bool {
		results = append(results, key)
		return true
	})
	if err != nil {
		t.Fatalf("FindRange failed: %v", err)
	}

	if len(results) != 21 { // 30 to 50 inclusive
		t.Errorf("expected 21 results, got %d", len(results))
	}

	// Verify order
	for i, r := range results {
		expected := uint64(30 + i)
		if r != expected {
			t.Errorf("result %d: expected %d, got %d", i, expected, r)
		}
	}
}

func TestFindRangeEarlyStop(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	for i := 1; i <= 100; i++ {
		tree.Insert(uint64(i), uint64(i))
	}

	count := 0
	tree.FindRange(1, 100, func(key, value uint64) bool {
		count++
		return count < 10 // Stop after 10
	})

	if count != 10 {
		t.Errorf("expected 10 iterations, got %d", count)
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	// Create and populate
	tree1, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 1000; i++ {
		tree1.Insert(uint64(i), uint64(i*10))
	}
	tree1.Checkpoint()
	tree1.Close()

	// Reopen and verify
	tree2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer tree2.Close()

	for i := 0; i < 1000; i++ {
		val, found := tree2.Find(uint64(i))
		if !found {
			t.Fatalf("key %d should be found after reopen", i)
		}
		if val != uint64(i*10) {
			t.Fatalf("key %d: expected %d, got %d", i, i*10, val)
		}
	}
}

func TestDelete(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	// Insert keys
	for i := 1; i <= 10; i++ {
		tree.Insert(uint64(i), uint64(i*10))
	}

	// Delete some keys
	if !tree.Delete(5) {
		t.Error("Delete(5) should return true")
	}
	if tree.Delete(5) {
		t.Error("Delete(5) second time should return false")
	}

	// Verify
	_, found := tree.Find(5)
	if found {
		t.Error("key 5 should not be found after delete")
	}

	// Other keys should still exist
	val, found := tree.Find(4)
	if !found || val != 40 {
		t.Error("key 4 should still exist")
	}
	val, found = tree.Find(6)
	if !found || val != 60 {
		t.Error("key 6 should still exist")
	}
}

func TestLargeDelete(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	n := 1000

	// Insert n keys
	for i := 0; i < n; i++ {
		tree.Insert(uint64(i), uint64(i*10))
	}

	// Delete all keys
	for i := 0; i < n; i++ {
		if !tree.Delete(uint64(i)) {
			t.Fatalf("Delete(%d) should return true", i)
		}
	}

	// Verify tree is empty
	if tree.Count() != 0 {
		t.Errorf("expected empty tree, got count %d", tree.Count())
	}

	// Verify no keys found
	for i := 0; i < n; i++ {
		_, found := tree.Find(uint64(i))
		if found {
			t.Fatalf("key %d should not be found after delete", i)
		}
	}
}

func TestDeleteWithPageReuse(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	n := 500

	// Insert keys
	for i := 0; i < n; i++ {
		tree.Insert(uint64(i), uint64(i*10))
	}

	// Delete half the keys
	for i := 0; i < n/2; i++ {
		tree.Delete(uint64(i))
	}

	// Insert new keys (should reuse freed pages)
	for i := n; i < n+n/2; i++ {
		tree.Insert(uint64(i), uint64(i*10))
	}

	// Verify remaining keys
	for i := n / 2; i < n+n/2; i++ {
		val, found := tree.Find(uint64(i))
		if !found {
			t.Fatalf("key %d should be found", i)
		}
		if val != uint64(i*10) {
			t.Fatalf("key %d: expected %d, got %d", i, i*10, val)
		}
	}
}

func TestDeleteReverseOrder(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	n := 500

	// Insert keys
	for i := 0; i < n; i++ {
		tree.Insert(uint64(i), uint64(i*10))
	}

	// Delete in reverse order
	for i := n - 1; i >= 0; i-- {
		if !tree.Delete(uint64(i)) {
			t.Fatalf("Delete(%d) should return true", i)
		}
	}

	// Verify tree is empty
	if tree.Count() != 0 {
		t.Errorf("expected empty tree, got count %d", tree.Count())
	}
}

func TestConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	// Populate
	n := 1000
	for i := 0; i < n; i++ {
		tree.Insert(uint64(i), uint64(i*10))
	}

	// Concurrent reads
	var wg sync.WaitGroup
	numReaders := 10
	readsPerReader := 1000

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readsPerReader; i++ {
				key := uint64(rand.Intn(n))
				val, found := tree.Find(key)
				if !found {
					t.Errorf("key %d should be found", key)
					return
				}
				if val != key*10 {
					t.Errorf("key %d: expected %d, got %d", key, key*10, val)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestEmptyTree(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	// Find from empty tree
	_, found := tree.Find(1)
	if found {
		t.Error("empty tree should not find anything")
	}

	// Delete from empty tree
	if tree.Delete(1) {
		t.Error("delete from empty tree should return false")
	}

	// FindRange empty tree
	count := 0
	tree.FindRange(0, 100, func(key, value uint64) bool {
		count++
		return true
	})
	if count != 0 {
		t.Error("FindRange of empty tree should return 0 results")
	}

	// Count empty tree
	if tree.Count() != 0 {
		t.Error("empty tree count should be 0")
	}
}

func BenchmarkInsert(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.db")

	tree, _ := Open(path)
	defer tree.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Insert(uint64(i), uint64(i))
	}
}

func BenchmarkFind(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.db")

	tree, _ := Open(path)
	defer tree.Close()

	// Pre-populate
	for i := 0; i < 100000; i++ {
		tree.Insert(uint64(i), uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Find(uint64(i % 100000))
	}
}

func BenchmarkFindRange(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.db")

	tree, _ := Open(path)
	defer tree.Close()

	// Pre-populate
	for i := 0; i < 100000; i++ {
		tree.Insert(uint64(i), uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.FindRange(1000, 2000, func(key, value uint64) bool {
			return true
		})
	}
}
