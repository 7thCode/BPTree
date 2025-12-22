// Package bptree provides a B+Tree implementation using memory-mapped files.
//
// The tree stores uint64 keys and uint64 values, optimized for concurrent reads.
// Changes are persisted to disk via checkpoint operations.
//
// Example:
//
//	tree, err := bptree.Open("data.db")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer tree.Close()
//
//	tree.Insert(1, 100)
//	tree.Insert(2, 200)
//
//	val, ok := tree.Find(1)
//	if ok {
//	    fmt.Println(val) // 100
//	}
//
//	tree.Checkpoint() // Sync to disk
package bptree2

import (
	"fmt"
	"sync"

	"github.com/oda/bptree2/pkg/bptree2/bnode"
	"github.com/oda/bptree2/pkg/bptree2/bpager"
)

// BPTree is a B+Tree that stores uint64 keys and values.
type BPTree struct {
	pager *bpager.Pager
	mu    sync.RWMutex // Protects writes, allows concurrent reads
}

// Open opens or creates a B+Tree file.
func Open(path string) (*BPTree, error) {
	p, err := bpager.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open pager: %w", err)
	}

	return &BPTree{
		pager: p,
	}, nil
}

// Checkpoint syncs all changes to disk.
func (t *BPTree) Checkpoint() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pager.Checkpoint()
}

// Count returns the number of key-value pairs in the tree.
// This is an O(n) operation.
func (t *BPTree) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	count := 0
	t.scanInternal(0, ^uint64(0), func(key, value uint64) bool {
		count++
		return true
	})
	return count
}

// Close closes the B+Tree and underlying file.
func (t *BPTree) Close() error {
	return t.pager.Close()
}

// Find retrieves a value by key.
// Returns (value, true) if found, (0, false) otherwise.
func (t *BPTree) Find(key uint64) (uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	rootID := t.pager.RootPage()
	if rootID == 0 {
		return 0, false // Empty tree
	}

	return t.search(rootID, key)
}

// FindRange iterates over all key-value pairs where start <= key <= end.
// The callback function is called for each pair. Return false to stop iteration.
func (t *BPTree) FindRange(start, end uint64, fn func(key, value uint64) bool) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.scanInternal(start, end, fn)
}

// Insert inserts or updates a key-value pair.
func (t *BPTree) Insert(key, value uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	rootID := t.pager.RootPage()

	// Empty tree - create first leaf
	if rootID == 0 {
		newPageID, err := t.pager.AllocatePage()
		if err != nil {
			return fmt.Errorf("failed to allocate root: %w", err)
		}
		data := t.pager.GetPage(newPageID)
		leaf := bnode.NewLeafNode(data, true)
		leaf.Put(key, value)
		t.pager.SetRootPage(newPageID)
		return nil
	}

	// Insert into existing tree
	splitKey, newChildID, err := t.insert(rootID, key, value)
	if err != nil {
		return err
	}

	// Root was split - create new root
	if newChildID != 0 {
		newRootID, err := t.pager.AllocatePage()
		if err != nil {
			return fmt.Errorf("failed to allocate new root: %w", err)
		}
		data := t.pager.GetPage(newRootID)
		newRoot := bnode.NewInternalNode(data, true)
		newRoot.InitRoot(rootID, newChildID, splitKey)
		t.pager.SetRootPage(newRootID)
	}

	return nil
}

// Delete removes a key from the tree.
// Returns true if the key was found and removed.
func (t *BPTree) Delete(key uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	rootID := t.pager.RootPage()
	if rootID == 0 {
		return false
	}

	deleted, _ := t.deleteRecursive(rootID, key)

	// Check if root needs to shrink
	if deleted {
		rootData := t.pager.GetPage(rootID)
		rootType := bnode.GetNodeType(rootData)

		if rootType == bnode.NodeTypeInternal {
			internal := bnode.NewInternalNode(rootData, false)
			if internal.KeyCount() == 0 {
				// Root has no keys, promote only child to root
				newRootID := internal.GetChild(0)
				t.pager.SetRootPage(newRootID)
				t.pager.FreePage(rootID)
			}
		} else {
			// Root is leaf
			leaf := bnode.NewLeafNode(rootData, false)
			if leaf.KeyCount() == 0 {
				// Tree is now empty
				t.pager.SetRootPage(0)
				t.pager.FreePage(rootID)
			}
		}
	}

	return deleted
}

// search recursively searches for a key starting from the given page.
func (t *BPTree) search(pageID bpager.PageID, key uint64) (uint64, bool) {
	data := t.pager.GetPage(pageID)
	if data == nil {
		return 0, false
	}

	nodeType := bnode.GetNodeType(data)

	if nodeType == bnode.NodeTypeLeaf {
		leaf := bnode.NewLeafNode(data, false)
		return leaf.Get(key)
	}

	// Internal node - find child to search
	internal := bnode.NewInternalNode(data, false)
	childID := internal.GetChildForKey(key)
	return t.search(childID, key)
}

// insert recursively inserts a key-value pair.
// Returns (splitKey, newPageID, error). If newPageID is non-zero, a split occurred.
func (t *BPTree) insert(pageID bpager.PageID, key, value uint64) (uint64, bpager.PageID, error) {
	data := t.pager.GetPage(pageID)
	if data == nil {
		return 0, 0, fmt.Errorf("failed to get page %d", pageID)
	}

	nodeType := bnode.GetNodeType(data)

	if nodeType == bnode.NodeTypeLeaf {
		return t.insertLeaf(pageID, key, value)
	}

	return t.insertInternal(pageID, key, value)
}

// insertLeaf inserts into a leaf node.
// Note: pageID is used instead of data slice because AllocatePage may trigger
// mmap remap, invalidating any previously obtained slices.
func (t *BPTree) insertLeaf(pageID bpager.PageID, key, value uint64) (uint64, bpager.PageID, error) {
	data := t.pager.GetPage(pageID)
	leaf := bnode.NewLeafNode(data, false)

	// If node has room, just insert
	if !leaf.IsFull() {
		leaf.Put(key, value)
		return 0, 0, nil
	}

	// Check if it's an update (existing key)
	if _, found := leaf.Get(key); found {
		leaf.Put(key, value)
		return 0, 0, nil
	}

	// Need to split - AllocatePage may remap mmap, invalidating data slice
	newPageID, err := t.pager.AllocatePage()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate page: %w", err)
	}

	// Re-fetch data after potential mmap remap
	data = t.pager.GetPage(pageID)
	leaf = bnode.NewLeafNode(data, false)

	newData := t.pager.GetPage(newPageID)
	splitKey, newLeaf := leaf.Split(newData)

	// Insert the new key into appropriate node
	if key < splitKey {
		leaf.Put(key, value)
	} else {
		newLeaf.Put(key, value)
	}

	// Update leaf links
	newLeaf.SetNextLeaf(leaf.NextLeaf())
	leaf.SetNextLeaf(newPageID)

	return splitKey, newPageID, nil
}

// insertInternal handles insertion through an internal node.
// Note: pageID is used instead of data slice because AllocatePage may trigger
// mmap remap, invalidating any previously obtained slices.
func (t *BPTree) insertInternal(pageID bpager.PageID, key, value uint64) (uint64, bpager.PageID, error) {
	data := t.pager.GetPage(pageID)
	internal := bnode.NewInternalNode(data, false)
	childID := internal.GetChildForKey(key)

	// Recursively insert into child - this may trigger mmap remap
	splitKey, newChildID, err := t.insert(childID, key, value)
	if err != nil {
		return 0, 0, err
	}

	// No split in child
	if newChildID == 0 {
		return 0, 0, nil
	}

	// Re-fetch data after potential mmap remap during child insert
	data = t.pager.GetPage(pageID)
	internal = bnode.NewInternalNode(data, false)

	// Child was split, need to insert new key into this node
	if !internal.IsFull() {
		internal.Insert(splitKey, newChildID)
		return 0, 0, nil
	}

	// This node is full, need to split - AllocatePage may remap mmap
	newPageID, err := t.pager.AllocatePage()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate page: %w", err)
	}

	// Re-fetch data after potential mmap remap
	data = t.pager.GetPage(pageID)
	internal = bnode.NewInternalNode(data, false)

	newData := t.pager.GetPage(newPageID)
	midKey, _ := internal.Split(newData)

	// Insert the new key into appropriate node
	// Reload nodes after split
	internal = bnode.NewInternalNode(data, false)
	newInternal := bnode.NewInternalNode(newData, false)

	if splitKey < midKey {
		internal.Insert(splitKey, newChildID)
	} else {
		newInternal.Insert(splitKey, newChildID)
	}

	return midKey, newPageID, nil
}

// deleteRecursive recursively deletes a key, handling underflow.
// Returns (deleted, underflow) where underflow indicates this node needs rebalancing.
func (t *BPTree) deleteRecursive(pageID bpager.PageID, key uint64) (bool, bool) {
	data := t.pager.GetPage(pageID)
	if data == nil {
		return false, false
	}

	bnodeType := bnode.GetNodeType(data)

	if bnodeType == bnode.NodeTypeLeaf {
		leaf := bnode.NewLeafNode(data, false)
		deleted := leaf.Delete(key)
		return deleted, deleted && leaf.IsUnderflow()
	}

	// Internal node - find child and recurse
	internal := bnode.NewInternalNode(data, false)
	childIdx := internal.Search(key)
	childID := internal.GetChild(childIdx)

	deleted, childUnderflow := t.deleteRecursive(childID, key)
	if !deleted {
		return false, false
	}

	if !childUnderflow {
		return true, false
	}

	// Handle child underflow
	t.handleUnderflow(internal, childIdx, data)

	return true, internal.IsUnderflow()
}

// handleUnderflow handles an underflowing child by borrowing or merging.
func (t *BPTree) handleUnderflow(parent *bnode.InternalNode, childIdx int, parentData []byte) {
	childID := parent.GetChild(childIdx)
	childData := t.pager.GetPage(childID)
	childType := bnode.GetNodeType(childData)

	// Try to borrow from left sibling
	if childIdx > 0 {
		leftSibID := parent.GetChild(childIdx - 1)
		leftSibData := t.pager.GetPage(leftSibID)

		if childType == bnode.NodeTypeLeaf {
			leftSib := bnode.NewLeafNode(leftSibData, false)
			if leftSib.CanLendTo() {
				child := bnode.NewLeafNode(childData, false)
				newSeparator := child.BorrowFromLeft(leftSib)
				parent.SetKeyAt(childIdx-1, newSeparator)
				return
			}
		} else {
			leftSib := bnode.NewInternalNode(leftSibData, false)
			if leftSib.CanLendTo() {
				child := bnode.NewInternalNode(childData, false)
				parentKey := parent.GetKeyAt(childIdx - 1)
				newSeparator := child.BorrowFromLeft(leftSib, parentKey)
				parent.SetKeyAt(childIdx-1, newSeparator)
				return
			}
		}
	}

	// Try to borrow from right sibling
	if childIdx < parent.KeyCount() {
		rightSibID := parent.GetChild(childIdx + 1)
		rightSibData := t.pager.GetPage(rightSibID)

		if childType == bnode.NodeTypeLeaf {
			rightSib := bnode.NewLeafNode(rightSibData, false)
			if rightSib.CanLendTo() {
				child := bnode.NewLeafNode(childData, false)
				newSeparator := child.BorrowFromRight(rightSib)
				parent.SetKeyAt(childIdx, newSeparator)
				return
			}
		} else {
			rightSib := bnode.NewInternalNode(rightSibData, false)
			if rightSib.CanLendTo() {
				child := bnode.NewInternalNode(childData, false)
				parentKey := parent.GetKeyAt(childIdx)
				newSeparator := child.BorrowFromRight(rightSib, parentKey)
				parent.SetKeyAt(childIdx, newSeparator)
				return
			}
		}
	}

	// Must merge - prefer merging with left sibling
	if childIdx > 0 {
		leftSibID := parent.GetChild(childIdx - 1)
		leftSibData := t.pager.GetPage(leftSibID)

		if childType == bnode.NodeTypeLeaf {
			leftSib := bnode.NewLeafNode(leftSibData, false)
			child := bnode.NewLeafNode(childData, false)
			leftSib.MergeWith(child)
		} else {
			leftSib := bnode.NewInternalNode(leftSibData, false)
			child := bnode.NewInternalNode(childData, false)
			parentKey := parent.GetKeyAt(childIdx - 1)
			leftSib.MergeWith(child, parentKey)
		}

		// Remove the separator and child pointer from parent
		parent.DeleteKeyAt(childIdx - 1)
		t.pager.FreePage(childID)
	} else {
		// Merge with right sibling
		rightSibID := parent.GetChild(childIdx + 1)
		rightSibData := t.pager.GetPage(rightSibID)

		if childType == bnode.NodeTypeLeaf {
			child := bnode.NewLeafNode(childData, false)
			rightSib := bnode.NewLeafNode(rightSibData, false)
			child.MergeWith(rightSib)
		} else {
			child := bnode.NewInternalNode(childData, false)
			rightSib := bnode.NewInternalNode(rightSibData, false)
			parentKey := parent.GetKeyAt(childIdx)
			child.MergeWith(rightSib, parentKey)
		}

		// Remove the separator and right child pointer from parent
		parent.DeleteKeyAt(childIdx)
		t.pager.FreePage(rightSibID)
	}
}

// scanInternal is the internal scan implementation without locking.
// Caller must hold at least a read lock.
func (t *BPTree) scanInternal(start, end uint64, fn func(key, value uint64) bool) error {
	rootID := t.pager.RootPage()
	if rootID == 0 {
		return nil // Empty tree
	}

	// Find the leaf containing start key
	leafID := t.findLeaf(rootID, start)
	if leafID == 0 {
		return nil
	}

	// Iterate through leaves
	for leafID != 0 {
		data := t.pager.GetPage(leafID)
		if data == nil {
			return fmt.Errorf("failed to get page %d", leafID)
		}

		leaf := bnode.NewLeafNode(data, false)
		pairs := leaf.Range(start, end)

		for _, pair := range pairs {
			if !fn(pair.Key, pair.Value) {
				return nil // User requested stop
			}
		}

		// Check if we've passed the end
		if leaf.KeyCount() > 0 && leaf.GetKeyAt(leaf.KeyCount()-1) >= end {
			break
		}

		leafID = leaf.NextLeaf()
	}

	return nil
}

// findLeaf finds the leaf page that would contain the given key.
func (t *BPTree) findLeaf(pageID bpager.PageID, key uint64) bpager.PageID {
	data := t.pager.GetPage(pageID)
	if data == nil {
		return 0
	}

	nodeType := bnode.GetNodeType(data)

	if nodeType == bnode.NodeTypeLeaf {
		return pageID
	}

	internal := bnode.NewInternalNode(data, false)
	childID := internal.GetChildForKey(key)
	return t.findLeaf(childID, key)
}
