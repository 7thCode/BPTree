package bnode

import (
	"testing"
)

func TestLeafNodeBasic(t *testing.T) {
	data := make([]byte, 4096)
	leaf := NewLeafNode(data, true)

	if leaf.Type() != NodeTypeLeaf {
		t.Error("expected leaf type")
	}
	if leaf.KeyCount() != 0 {
		t.Error("expected 0 keys")
	}
	if leaf.IsFull() {
		t.Error("should not be full")
	}
}

func TestLeafNodePutGet(t *testing.T) {
	data := make([]byte, 4096)
	leaf := NewLeafNode(data, true)

	// Insert keys
	leaf.Put(10, 100)
	leaf.Put(5, 50)
	leaf.Put(15, 150)
	leaf.Put(7, 70)

	if leaf.KeyCount() != 4 {
		t.Errorf("expected 4 keys, got %d", leaf.KeyCount())
	}

	// Get keys
	tests := []struct {
		key      uint64
		expected uint64
		found    bool
	}{
		{5, 50, true},
		{7, 70, true},
		{10, 100, true},
		{15, 150, true},
		{1, 0, false},
		{20, 0, false},
	}

	for _, tt := range tests {
		val, found := leaf.Get(tt.key)
		if found != tt.found {
			t.Errorf("key %d: expected found=%v, got %v", tt.key, tt.found, found)
		}
		if found && val != tt.expected {
			t.Errorf("key %d: expected value %d, got %d", tt.key, tt.expected, val)
		}
	}
}

func TestLeafNodeUpdate(t *testing.T) {
	data := make([]byte, 4096)
	leaf := NewLeafNode(data, true)

	inserted := leaf.Put(10, 100)
	if !inserted {
		t.Error("expected insert")
	}

	inserted = leaf.Put(10, 200)
	if inserted {
		t.Error("expected update, not insert")
	}

	val, _ := leaf.Get(10)
	if val != 200 {
		t.Errorf("expected 200, got %d", val)
	}

	if leaf.KeyCount() != 1 {
		t.Errorf("expected 1 key, got %d", leaf.KeyCount())
	}
}

func TestLeafNodeDelete(t *testing.T) {
	data := make([]byte, 4096)
	leaf := NewLeafNode(data, true)

	leaf.Put(10, 100)
	leaf.Put(5, 50)
	leaf.Put(15, 150)

	deleted := leaf.Delete(10)
	if !deleted {
		t.Error("expected delete to succeed")
	}

	_, found := leaf.Get(10)
	if found {
		t.Error("key should not exist after delete")
	}

	if leaf.KeyCount() != 2 {
		t.Errorf("expected 2 keys, got %d", leaf.KeyCount())
	}

	// Keys should still be sorted
	if leaf.getKey(0) != 5 || leaf.getKey(1) != 15 {
		t.Error("keys not in order after delete")
	}
}

func TestLeafNodeRange(t *testing.T) {
	data := make([]byte, 4096)
	leaf := NewLeafNode(data, true)

	for i := uint64(1); i <= 10; i++ {
		leaf.Put(i*10, i*100)
	}

	results := leaf.Range(30, 70)
	if len(results) != 5 { // 30, 40, 50, 60, 70
		t.Errorf("expected 5 results, got %d", len(results))
	}

	expected := []uint64{30, 40, 50, 60, 70}
	for i, r := range results {
		if r.Key != expected[i] {
			t.Errorf("result %d: expected key %d, got %d", i, expected[i], r.Key)
		}
	}
}

func TestLeafNodeSplit(t *testing.T) {
	data1 := make([]byte, 4096)
	data2 := make([]byte, 4096)
	leaf := NewLeafNode(data1, true)

	// Insert 10 keys
	for i := uint64(1); i <= 10; i++ {
		leaf.Put(i, i*10)
	}

	midKey, newNode := leaf.Split(data2)

	// Check split
	if leaf.KeyCount()+newNode.KeyCount() != 10 {
		t.Errorf("total keys should be 10, got %d + %d",
			leaf.KeyCount(), newNode.KeyCount())
	}

	// Mid key should be the first key of new node
	if midKey != newNode.getKey(0) {
		t.Errorf("midKey %d != first key of new node %d", midKey, newNode.getKey(0))
	}

	// All keys in original should be less than midKey
	for i := 0; i < leaf.KeyCount(); i++ {
		if leaf.getKey(i) >= midKey {
			t.Errorf("original node key %d >= midKey %d", leaf.getKey(i), midKey)
		}
	}

	// All keys in new node should be >= midKey
	for i := 0; i < newNode.KeyCount(); i++ {
		if newNode.getKey(i) < midKey {
			t.Errorf("new node key %d < midKey %d", newNode.getKey(i), midKey)
		}
	}
}

func TestInternalNodeBasic(t *testing.T) {
	data := make([]byte, 4096)
	node := NewInternalNode(data, true)

	if node.Type() != NodeTypeInternal {
		t.Error("expected internal type")
	}
	if node.KeyCount() != 0 {
		t.Error("expected 0 keys")
	}
}

func TestInternalNodeInitRoot(t *testing.T) {
	data := make([]byte, 4096)
	node := NewInternalNode(data, true)

	node.InitRoot(1, 2, 100)

	if node.KeyCount() != 1 {
		t.Errorf("expected 1 key, got %d", node.KeyCount())
	}
	if node.GetChild(0) != 1 {
		t.Error("left child should be 1")
	}
	if node.GetChild(1) != 2 {
		t.Error("right child should be 2")
	}
	if node.getKey(0) != 100 {
		t.Error("key should be 100")
	}
}

func TestInternalNodeSearch(t *testing.T) {
	data := make([]byte, 4096)
	node := NewInternalNode(data, true)

	// Build a node with keys [20, 40, 60]
	// Children: [C0, C1, C2, C3]
	// C0 for keys < 20
	// C1 for keys >= 20 and < 40
	// C2 for keys >= 40 and < 60
	// C3 for keys >= 60

	node.setChild(0, 10) // page 10
	node.setKey(0, 20)
	node.setChild(1, 11) // page 11
	node.setKey(1, 40)
	node.setChild(2, 12) // page 12
	node.setKey(2, 60)
	node.setChild(3, 13) // page 13
	setKeyCount(data, 3)

	tests := []struct {
		key           uint64
		expectedChild uint64
	}{
		{5, 10},  // < 20 -> C0
		{20, 11}, // >= 20, < 40 -> C1
		{30, 11}, // >= 20, < 40 -> C1
		{40, 12}, // >= 40, < 60 -> C2
		{50, 12}, // >= 40, < 60 -> C2
		{60, 13}, // >= 60 -> C3
		{100, 13},
	}

	for _, tt := range tests {
		child := node.GetChildForKey(tt.key)
		if child != tt.expectedChild {
			t.Errorf("key %d: expected child %d, got %d",
				tt.key, tt.expectedChild, child)
		}
	}
}

func TestInternalNodeInsert(t *testing.T) {
	data := make([]byte, 4096)
	node := NewInternalNode(data, true)

	node.InitRoot(1, 2, 50)

	// Insert more keys
	node.Insert(30, 3) // key 30, right child 3
	node.Insert(70, 4) // key 70, right child 4

	if node.KeyCount() != 3 {
		t.Errorf("expected 3 keys, got %d", node.KeyCount())
	}

	// Check order: keys should be [30, 50, 70]
	if node.getKey(0) != 30 || node.getKey(1) != 50 || node.getKey(2) != 70 {
		t.Errorf("keys out of order: %d, %d, %d",
			node.getKey(0), node.getKey(1), node.getKey(2))
	}

	// Check children: [1, 3, 2, 4]
	if node.GetChild(0) != 1 || node.GetChild(1) != 3 ||
		node.GetChild(2) != 2 || node.GetChild(3) != 4 {
		t.Error("children out of order")
	}
}

func TestInternalNodeSplit(t *testing.T) {
	data1 := make([]byte, 4096)
	data2 := make([]byte, 4096)
	node := NewInternalNode(data1, true)

	// Build a node with many keys
	node.setChild(0, 100)
	for i := 0; i < 10; i++ {
		node.setKey(i, uint64((i+1)*10))
		node.setChild(i+1, uint64(101+i))
	}
	setKeyCount(data1, 10)

	midKey, newNode := node.Split(data2)

	// Total keys should be 9 (mid key is promoted, not in either node)
	if node.KeyCount()+newNode.KeyCount() != 9 {
		t.Errorf("expected 9 total keys, got %d + %d",
			node.KeyCount(), newNode.KeyCount())
	}

	// All keys in original should be less than midKey
	for i := 0; i < node.KeyCount(); i++ {
		if node.getKey(i) >= midKey {
			t.Errorf("original node key %d >= midKey %d", node.getKey(i), midKey)
		}
	}

	// All keys in new node should be greater than midKey
	for i := 0; i < newNode.KeyCount(); i++ {
		if newNode.getKey(i) <= midKey {
			t.Errorf("new node key %d <= midKey %d", newNode.getKey(i), midKey)
		}
	}
}
