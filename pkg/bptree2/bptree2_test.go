package bptree2_test

import (
	"bptree2"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

func TestBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	// Create a root
	rootID, err := tree.CreateRoot()
	if err != nil {
		t.Fatalf("CreateRoot failed: %v", err)
	}

	// Insert with composite key (key1, key2)
	if err := tree.Insert(rootID, 10, 100, 1000); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := tree.Insert(rootID, 5, 50, 500); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := tree.Insert(rootID, 15, 150, 1500); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Find with exact composite key match (AND condition)
	val, found := tree.Find(rootID, 10, 100)
	if !found {
		t.Error("key (10, 100) should be found")
	}
	if val != 1000 {
		t.Errorf("expected 1000, got %d", val)
	}

	// Find non-existent key pair - key1 exists but key2 doesn't
	_, found = tree.Find(rootID, 10, 200)
	if found {
		t.Error("key (10, 200) should not be found - AND condition")
	}

	// Find completely non-existent
	_, found = tree.Find(rootID, 20, 200)
	if found {
		t.Error("key (20, 200) should not be found")
	}

	// Update
	if err := tree.Insert(rootID, 10, 100, 2000); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	val, _ = tree.Find(rootID, 10, 100)
	if val != 2000 {
		t.Errorf("expected 2000 after update, got %d", val)
	}
}

func TestLargeInsert(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()
	n := 10000

	// Insert n keys with composite keys
	for i := 0; i < n; i++ {
		if err := tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i*10)); err != nil {
			t.Fatalf("Insert failed at %d: %v", i, err)
		}
	}

	// Verify all keys
	for i := 0; i < n; i++ {
		val, found := tree.Find(rootID, uint64(i), uint64(i*2))
		if !found {
			t.Fatalf("key (%d, %d) should be found", i, i*2)
		}
		if val != uint64(i*10) {
			t.Fatalf("key (%d, %d): expected %d, got %d", i, i*2, i*10, val)
		}
	}

	// Verify count
	count := tree.Count(rootID)
	if count != n {
		t.Errorf("expected count %d, got %d", n, count)
	}
}

func TestMillionInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large test in short mode")
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()
	n := 1000000

	// Insert n keys
	for i := 0; i < n; i++ {
		if err := tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i*10)); err != nil {
			t.Fatalf("Insert failed at %d: %v", i, err)
		}
	}

	// Verify some keys
	for i := 0; i < n; i += 10000 {
		val, found := tree.Find(rootID, uint64(i), uint64(i*2))
		if !found {
			t.Fatalf("key (%d, %d) should be found", i, i*2)
		}
		if val != uint64(i*10) {
			t.Fatalf("key (%d, %d): expected %d, got %d", i, i*2, i*10, val)
		}
	}
}

func TestRandomInsert(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	// Generate random keys
	n := 5000
	keys1 := make([]uint64, n)
	keys2 := make([]uint64, n)
	for i := range keys1 {
		keys1[i] = rand.Uint64()
		keys2[i] = rand.Uint64()
	}

	// Insert
	for i := 0; i < n; i++ {
		if err := tree.Insert(rootID, keys1[i], keys2[i], uint64(i)); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Verify
	for i := 0; i < n; i++ {
		val, found := tree.Find(rootID, keys1[i], keys2[i])
		if !found {
			t.Fatalf("key (%d, %d) should be found", keys1[i], keys2[i])
		}
		if val != uint64(i) {
			t.Fatalf("key (%d, %d): expected %d, got %d", keys1[i], keys2[i], i, val)
		}
	}
}

func TestRangeScan(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	// Insert 100 keys with key2 = key1
	for i := 1; i <= 100; i++ {
		if err := tree.Insert(rootID, uint64(i), uint64(i), uint64(i*10)); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// FindRange [30, 30] to [50, 50]
	var results []uint64
	err = tree.FindRange(rootID, 30, 30, 50, 50, func(key1, key2, value uint64) bool {
		results = append(results, key1)
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

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	for i := 1; i <= 100; i++ {
		tree.Insert(rootID, uint64(i), uint64(i), uint64(i))
	}

	count := 0
	tree.FindRange(rootID, 1, 1, 100, 100, func(key1, key2, value uint64) bool {
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
	tree1, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	rootID, _ := tree1.CreateRoot()
	for i := 0; i < 1000; i++ {
		tree1.Insert(rootID, uint64(i), uint64(i*2), uint64(i*10))
	}
	tree1.Flash()
	tree1.Close()

	// Reopen and verify
	tree2, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer tree2.Close()

	// Use same rootID (0)
	for i := 0; i < 1000; i++ {
		val, found := tree2.Find(rootID, uint64(i), uint64(i*2))
		if !found {
			t.Fatalf("key (%d, %d) should be found after reopen", i, i*2)
		}
		if val != uint64(i*10) {
			t.Fatalf("key (%d, %d): expected %d, got %d", i, i*2, i*10, val)
		}
	}
}

func TestDelete(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	// Insert keys
	for i := 1; i <= 10; i++ {
		tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i*10))
	}

	// Delete some keys
	if !tree.Delete(rootID, 5, 10) {
		t.Error("Delete(5, 10) should return true")
	}
	if tree.Delete(rootID, 5, 10) {
		t.Error("Delete(5, 10) second time should return false")
	}

	// Verify
	_, found := tree.Find(rootID, 5, 10)
	if found {
		t.Error("key (5, 10) should not be found after delete")
	}

	// Other keys should still exist
	val, found := tree.Find(rootID, 4, 8)
	if !found || val != 40 {
		t.Error("key (4, 8) should still exist")
	}
	val, found = tree.Find(rootID, 6, 12)
	if !found || val != 60 {
		t.Error("key (6, 12) should still exist")
	}
}

func TestLargeDelete(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()
	n := 1000

	// Insert n keys
	for i := 0; i < n; i++ {
		tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i*10))
	}

	// Delete all keys
	for i := 0; i < n; i++ {
		if !tree.Delete(rootID, uint64(i), uint64(i*2)) {
			t.Fatalf("Delete(%d, %d) should return true", i, i*2)
		}
	}

	// Verify tree is empty
	if tree.Count(rootID) != 0 {
		t.Errorf("expected empty tree, got count %d", tree.Count(rootID))
	}

	// Verify no keys found
	for i := 0; i < n; i++ {
		_, found := tree.Find(rootID, uint64(i), uint64(i*2))
		if found {
			t.Fatalf("key (%d, %d) should not be found after delete", i, i*2)
		}
	}
}

func TestDeleteWithPageReuse(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()
	n := 500

	// Insert keys
	for i := 0; i < n; i++ {
		tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i*10))
	}

	// Delete half the keys
	for i := 0; i < n/2; i++ {
		tree.Delete(rootID, uint64(i), uint64(i*2))
	}

	// Insert new keys (should reuse freed pages)
	for i := n; i < n+n/2; i++ {
		tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i*10))
	}

	// Verify remaining keys
	for i := n / 2; i < n+n/2; i++ {
		val, found := tree.Find(rootID, uint64(i), uint64(i*2))
		if !found {
			t.Fatalf("key (%d, %d) should be found", i, i*2)
		}
		if val != uint64(i*10) {
			t.Fatalf("key (%d, %d): expected %d, got %d", i, i*2, i*10, val)
		}
	}
}

func TestDeleteReverseOrder(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()
	n := 500

	// Insert keys
	for i := 0; i < n; i++ {
		tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i*10))
	}

	// Delete in reverse order
	for i := n - 1; i >= 0; i-- {
		if !tree.Delete(rootID, uint64(i), uint64(i*2)) {
			t.Fatalf("Delete(%d, %d) should return true", i, i*2)
		}
	}

	// Verify tree is empty
	if tree.Count(rootID) != 0 {
		t.Errorf("expected empty tree, got count %d", tree.Count(rootID))
	}
}

func TestConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	// Populate
	n := 1000
	for i := 0; i < n; i++ {
		tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i*10))
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
				key1 := uint64(rand.Intn(n))
				key2 := key1 * 2
				val, found := tree.Find(rootID, key1, key2)
				if !found {
					t.Errorf("key (%d, %d) should be found", key1, key2)
					return
				}
				if val != key1*10 {
					t.Errorf("key (%d, %d): expected %d, got %d", key1, key2, key1*10, val)
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

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	// Find from empty tree
	_, found := tree.Find(rootID, 1, 2)
	if found {
		t.Error("empty tree should not find anything")
	}

	// Delete from empty tree
	if tree.Delete(rootID, 1, 2) {
		t.Error("delete from empty tree should return false")
	}

	// FindRange empty tree
	count := 0
	tree.FindRange(rootID, 0, 0, 100, 100, func(key1, key2, value uint64) bool {
		count++
		return true
	})
	if count != 0 {
		t.Error("FindRange of empty tree should return 0 results")
	}

	// Count empty tree
	if tree.Count(rootID) != 0 {
		t.Error("empty tree count should be 0")
	}
}

func TestMultipleRoots(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	// Create multiple roots
	root1, _ := tree.CreateRoot()
	root2, _ := tree.CreateRoot()
	root3, _ := tree.CreateRoot()

	// Insert different data into each root
	for i := 0; i < 100; i++ {
		tree.Insert(root1, uint64(i), uint64(i*2), uint64(i*10))
		tree.Insert(root2, uint64(i), uint64(i*2), uint64(i*100))
		tree.Insert(root3, uint64(i), uint64(i*2), uint64(i*1000))
	}

	// Verify each root has independent data
	for i := 0; i < 100; i++ {
		val1, _ := tree.Find(root1, uint64(i), uint64(i*2))
		val2, _ := tree.Find(root2, uint64(i), uint64(i*2))
		val3, _ := tree.Find(root3, uint64(i), uint64(i*2))

		if val1 != uint64(i*10) {
			t.Errorf("root1 key (%d, %d): expected %d, got %d", i, i*2, i*10, val1)
		}
		if val2 != uint64(i*100) {
			t.Errorf("root2 key (%d, %d): expected %d, got %d", i, i*2, i*100, val2)
		}
		if val3 != uint64(i*1000) {
			t.Errorf("root3 key (%d, %d): expected %d, got %d", i, i*2, i*1000, val3)
		}
	}

	// Verify counts are independent
	if tree.Count(root1) != 100 || tree.Count(root2) != 100 || tree.Count(root3) != 100 {
		t.Error("each root should have 100 entries")
	}

	// Delete from one root doesn't affect others
	for i := 0; i < 50; i++ {
		tree.Delete(root1, uint64(i), uint64(i*2))
	}

	if tree.Count(root1) != 50 {
		t.Errorf("root1 should have 50 entries after delete, got %d", tree.Count(root1))
	}
	if tree.Count(root2) != 100 {
		t.Errorf("root2 should still have 100 entries, got %d", tree.Count(root2))
	}
}

func TestMultipleRootsPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	// Create and populate
	tree1, _ := bptree2.Open(path)
	root1, _ := tree1.CreateRoot()
	root2, _ := tree1.CreateRoot()

	for i := 0; i < 100; i++ {
		tree1.Insert(root1, uint64(i), uint64(i*2), uint64(i*10))
		tree1.Insert(root2, uint64(i), uint64(i*2), uint64(i*100))
	}
	tree1.Flash()
	tree1.Close()

	// Reopen and verify
	tree2, _ := bptree2.Open(path)
	defer tree2.Close()

	for i := 0; i < 100; i++ {
		val1, found1 := tree2.Find(root1, uint64(i), uint64(i*2))
		val2, found2 := tree2.Find(root2, uint64(i), uint64(i*2))

		if !found1 || val1 != uint64(i*10) {
			t.Errorf("root1 key (%d, %d): expected %d, got %d (found=%v)", i, i*2, i*10, val1, found1)
		}
		if !found2 || val2 != uint64(i*100) {
			t.Errorf("root2 key (%d, %d): expected %d, got %d (found=%v)", i, i*2, i*100, val2, found2)
		}
	}
}

func TestAndCondition(t *testing.T) {
	// Test that AND condition works correctly
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	tree, err := bptree2.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	// Insert multiple entries with same key1 but different key2
	tree.Insert(rootID, 100, 1, 1001)
	tree.Insert(rootID, 100, 2, 1002)
	tree.Insert(rootID, 100, 3, 1003)

	// Insert entry with different key1
	tree.Insert(rootID, 200, 1, 2001)

	// Test AND condition - must match both key1 AND key2
	val, found := tree.Find(rootID, 100, 1)
	if !found || val != 1001 {
		t.Errorf("expected 1001, got %d (found=%v)", val, found)
	}

	val, found = tree.Find(rootID, 100, 2)
	if !found || val != 1002 {
		t.Errorf("expected 1002, got %d (found=%v)", val, found)
	}

	// Key1 matches but key2 doesn't
	_, found = tree.Find(rootID, 100, 99)
	if found {
		t.Error("should not find (100, 99) - AND condition not satisfied")
	}

	// Key2 matches but key1 doesn't
	_, found = tree.Find(rootID, 999, 1)
	if found {
		t.Error("should not find (999, 1) - AND condition not satisfied")
	}
}

func BenchmarkInsert(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.db")

	tree, _ := bptree2.Open(path)
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i))
	}
}

func BenchmarkFind(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.db")

	tree, _ := bptree2.Open(path)
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	// Pre-populate
	for i := 0; i < 100000; i++ {
		tree.Insert(rootID, uint64(i), uint64(i*2), uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Find(rootID, uint64(i%100000), uint64((i%100000)*2))
	}
}

func BenchmarkFindRange(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.db")

	tree, _ := bptree2.Open(path)
	defer tree.Close()

	rootID, _ := tree.CreateRoot()

	// Pre-populate
	for i := 0; i < 100000; i++ {
		tree.Insert(rootID, uint64(i), uint64(i), uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.FindRange(rootID, 1000, 1000, 2000, 2000, func(key1, key2, value uint64) bool {
			return true
		})
	}
}
