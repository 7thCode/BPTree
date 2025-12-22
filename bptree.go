// Package bptree B+Tree実装
package bptree

import (
	"cache"
	"config"
	"container/list"
	"util"
)

/*
	leaf []uint64
	[ Attr()type,size ]
	[     Prev ID     ]
	[     Next ID     ]
	[     Entries     ]
		[ target Value]
		[  key Value  ]
			.
			.
*/

/*
	node []uint64
	[ Attr()type,size ]
	[     Prev ID     ]
	[     Next ID     ]
	[     Entries     ]
		 [  less ID   ]
		 [ key Value  ]
		 [ less ID or greater ID ]
		 [ key Value  ]
			.
*/

const EachEntry int32 = 2

// EntryCount 1023
const EntryCount int32 = 128 * 2 // must EVEN.
const EntrySize = (EntryCount * EachEntry) + 1
const HeaderSize int32 = EntryOffset // attr, prev, next, parent
const BPNodeSize = HeaderSize + EntrySize

// const LeafEntrySize = EntrySize + EachEntry
// ExpandEntrySize
const ExpandEntrySize = EntrySize + EachEntry

// Leaf/Node Offset
const (
	AttrOffset   = 0
	PrevOffset   = 1
	NextOffset   = 2
	ParentOffset = 3
	EntryOffset  = 4
)

// each Entry Offset
const (
	LesserOffset  = 0 // for Node
	ValueOffset   = 0 // for Leaf
	KeyOffset     = 1
	GreaterOffset = 2
)

const StartGap = 1

const NotFound = util.NotFound

type NodeType uint32

const (
	None NodeType = 0x0000
	Node NodeType = 0x0001
	Leaf NodeType = 0x0002
)

// sizeToEntryCount サイズからエントリー数に
func sizeToEntryCount(size int32) int32 {
	return size / EachEntry
}

// attrToType | type | size |
func attrToType(attr uint64) NodeType {
	return NodeType(attr >> 32)
}

// typeToAttr attrにNodeTypeをセット
func typeToAttr(attr uint64, t NodeType) uint64 {
	return (uint64(t) << 32) | (attr & 0x00000000ffffffff)
}

// attrToSize attrからSizeを抽出
func attrToSize(attr uint64) int32 {
	return int32(attr & 0x00000000ffffffff)
}

// sizeToAttr attrにsizeをセット
func sizeToAttr(attr uint64, s uint32) uint64 {
	return uint64(s&0x00000000ffffffff) | (attr & 0xffffffff00000000)
}

// divideLeafEntries エントリーを一つ追加したleafを分割
func divideLeafEntries(entries *[ExpandEntrySize]uint64) ([]uint64, uint64, []uint64) {
	center := (EntrySize - 1) >> 1
	centerKey := (center >> 1) << 1 // to even

	return entries[:center], entries[centerKey+1], entries[center : EntrySize+EachEntry-1]
}

// divideNodeEntries エントリーを一つ追加したnodeを分割
func divideNodeEntries(entries *[ExpandEntrySize]uint64) ([]uint64, uint64, []uint64) {
	center := (EntrySize - 1) >> 1
	centerKey := (center >> 1) << 1 // to even

	return entries[:center+1], entries[centerKey+1], entries[center:EntrySize]
}

// clearEntries エントリーのデータを初期化
func (tree *BPtree) clearEntries(ID util.ID) {
	tree.setSize(ID, 0)
	for index := int32(0); index < EntrySize; index++ {
		tree.setEntry(ID, index, 0)
	}
}

// build ノード作成
func (tree *BPtree) build(ID util.ID, _type NodeType, data *[]uint64) {
	tree.setType(ID, _type)
	tree.setSize(ID, int32(len(*data)))
	for index := int32(0); index < EntrySize; index++ {
		if index < int32(len(*data)) {
			tree.setEntry(ID, index, (*data)[index])
		} else {
			if index == EntryCount*EachEntry { // dirty trick
				tree.forwardSeekPoint(ID, index)
			} else {
				tree.setEntry(ID, index, 0)
			}
		}
	}
}

// transformLeafToNode LeafをNodeに
func (tree *BPtree) transformLeafToNode(ID util.ID) {
	if tree._type(ID) == Leaf {
		tree.setType(ID, Node)
		tree.clearEntries(ID)
	}
}

// attr get attr
func (tree *BPtree) attr(ID util.ID) uint64 {
	return tree.get(int64(ID) + AttrOffset)
}

// setAttr set attr
func (tree *BPtree) setAttr(ID util.ID, value uint64) {
	tree.set(int64(ID+AttrOffset), value)
}

// _type get type
func (tree *BPtree) _type(ID util.ID) NodeType {
	return attrToType(tree.attr(ID))
}

// setType set type
func (tree *BPtree) setType(ID util.ID, value NodeType) {
	tree.setAttr(ID, typeToAttr(tree.attr(ID), value))
}

// size get size
func (tree *BPtree) size(ID util.ID) int32 {
	return attrToSize(tree.attr(ID))
}

// setSize set size
func (tree *BPtree) setSize(ID util.ID, value int32) {
	tree.setAttr(ID, sizeToAttr(tree.attr(ID), uint32(value)))
}

// prev get prev
func (tree *BPtree) prev(ID util.ID) util.ID {
	return tree.get(int64(ID + PrevOffset))
}

// setPrev set prev
func (tree *BPtree) setPrev(ID util.ID, value util.ID) {
	tree.set(int64(ID+PrevOffset), value)
}

// Next get
func (tree *BPtree) next(ID util.ID) util.ID {
	return tree.get(int64(ID + NextOffset))
}

// setNext set next
func (tree *BPtree) setNext(ID util.ID, value util.ID) {
	tree.set(int64(ID+NextOffset), value)
}

// setParent set parent
func (tree *BPtree) setParent(ID util.ID, value util.ID) {
	tree.set(int64(ID+ParentOffset), value)
}

// lesser get lesser hand
func (tree *BPtree) lesser(ID util.ID, offset int32) util.ID {
	return tree.getEntry(ID, (offset*EachEntry)+LesserOffset)
}

// greater get greater hand
func (tree *BPtree) greater(ID util.ID, offset int32) util.ID {
	return tree.getEntry(ID, (offset*EachEntry)+GreaterOffset)
}

// key at offset
func (tree *BPtree) key(ID util.ID, offset int32) util.Key {
	return tree.getEntry(ID, (offset*EachEntry)+KeyOffset)
}

// value get value
func (tree *BPtree) value(ID util.ID, offset int32) util.Value {
	return tree.getEntry(ID, (offset*EachEntry)+ValueOffset)
}

// setValue set value
func (tree *BPtree) setValue(ID util.ID, offset int32, value util.Value) {
	tree.setEntry(ID, offset*EachEntry, value)
}

// getEntry get entry
func (tree *BPtree) getEntry(ID util.ID, offset int32) util.Value {
	return tree.get(int64(ID) + EntryOffset + int64(offset))
}

// setEntry set entry
func (tree *BPtree) setEntry(ID util.ID, offset int32, value util.Value) {
	tree.set(int64(ID)+EntryOffset+int64(offset), value)
}

// setEntry set entry
func (tree *BPtree) forwardSeekPoint(ID util.ID, offset int32) {
	tree.data.ForwardSeekPoint(int64(ID) + EntryOffset + int64(offset))
}

// isFullLeaf
func (tree *BPtree) isFullLeaf(ID util.ID) bool {
	return tree.size(ID) >= EntryCount*EachEntry
}

// isFullNode
func (tree *BPtree) isFullNode(ID util.ID) bool {
	return tree.size(ID) >= (EntryCount-1)*EachEntry
}

// findAndSet Keyがみつかるか、終わりまでコピー
func (tree *BPtree) findAndSet(ID util.ID, key util.Key, result *[ExpandEntrySize]uint64) (offset int32) {
	for offset = 0; offset < EntrySize; offset++ { // position
		result[offset] = tree.getEntry(ID, offset)
		if offset%EachEntry == KeyOffset {
			if result[offset] > key || result[offset] == 0 {
				break
			}
		}
	}

	return
}

// expandLeafEntry Leafにエントリーを一つ追加
func (tree *BPtree) expandLeafEntry(ID util.ID, key util.Key, value util.Value, result *[ExpandEntrySize]uint64) {
	var keysOffset = tree.findAndSet(ID, key, result)

	// 最後から隙間を開けて詰める
	for backward := EntrySize + 1; backward > keysOffset; backward-- { // make room for entry
		result[backward] = tree.getEntry(ID, backward-EachEntry)
	}

	// 値を設定
	result[keysOffset-1] = value
	result[keysOffset] = key
}

// expandNodeEntry Nodeにエントリーを一つ追加
func (tree *BPtree) expandNodeEntry(ID util.ID, key util.Key, value util.Value, result *[ExpandEntrySize]uint64) {
	var keysOffset = tree.findAndSet(ID, key, result)

	// 最後から隙間を開けて詰める
	for backward := EntrySize; backward > keysOffset; backward-- { // make room for entry
		result[backward] = tree.getEntry(ID, backward-EachEntry)
	}

	// 値を設定
	result[keysOffset] = key
	result[keysOffset+1] = value
}

// insertLeafEntry leafにentry追加
func (tree *BPtree) insertLeafEntry(ID util.ID, key util.Key, value util.Value) {
	size := tree.size(ID)
	var keysOffset int32
	for keysOffset = KeyOffset; keysOffset <= size; keysOffset += EachEntry {
		targetKey := tree.getEntry(ID, keysOffset)
		if targetKey > key || targetKey == 0 {
			break
		}
	}

	// 最後からkeysOffsetまでEntryサイズ隙間を開けて詰める (BottleNeck?)
	for backward := EntrySize - 1; backward > keysOffset; backward-- {
		entry := tree.getEntry(ID, backward-EachEntry)
		if entry != util.UnDef {
			tree.setEntry(ID, backward, entry)
		}
	}

	// entryを上書き
	tree.setEntry(ID, keysOffset-1, value)
	tree.setEntry(ID, keysOffset, key)

	tree.setSize(ID, size+EachEntry) // add an Entry
}

// insertNodeEntry nodeにentry追加
func (tree *BPtree) insertNodeEntry(ID util.ID, key util.Key, left util.ID, right util.ID) {
	size := tree.size(ID)
	var keysOffset int32
	for keysOffset = KeyOffset; keysOffset <= size; keysOffset += EachEntry {
		targetKey := tree.getEntry(ID, keysOffset)
		if targetKey > key || targetKey == 0 {
			break
		}
	}

	for backward := EntrySize - 1; backward > keysOffset; backward-- {
		entry := tree.getEntry(ID, backward-EachEntry)
		if entry != util.UnDef {
			tree.setEntry(ID, backward, entry)
		}
	}

	// entryを上書き
	tree.setEntry(ID, keysOffset-1, left)
	tree.setEntry(ID, keysOffset, key)
	tree.setEntry(ID, keysOffset+1, right)

	tree.setSize(ID, size+EachEntry) // add an Entry
}

// insertNode  中間ノード操作
func (tree *BPtree) insertNode(thisID util.ID, key util.Key, left util.ID, right util.ID) {
	if !tree.isFullNode(thisID) { // 保持するサブツリー数がFullでない場合
		tree.insertNodeEntry(thisID, key, left, right) // 単純追加
	} else {
		if tree.ancestor.Len() == 0 { // root
			// 中間ノードを作成しless, greaterに分離したサブツリーをポイントしてルートをその中間ノードに設定
			var entries [ExpandEntrySize]uint64 // Entry分割バッファ
			tree.expandNodeEntry(thisID, key, right, &entries)
			lesserEntries, pivot, greaterEntries := divideNodeEntries(&entries)

			lessNodeID := tree.alloc() // 小さい方のノード作成
			tree.build(lessNodeID, Node, &lesserEntries)

			greaterNodeID := tree.alloc() // 大きい方のノード作成
			tree.build(greaterNodeID, Node, &greaterEntries)

			// 自分を中間ノードにしてノードを追加
			tree.clearEntries(thisID)
			tree.insertNodeEntry(thisID, pivot, lessNodeID, greaterNodeID)
		} else {
			// 親の中間ノードに新しく作成した中間ノードのポインタとその境界値を挿入
			// Entry分割
			var entries [ExpandEntrySize]uint64 // Entry分割バッファ
			tree.expandNodeEntry(thisID, key, right, &entries)
			lesserEntries, pivotEntry, greaterEntries := divideNodeEntries(&entries)

			greaterNodeID := tree.alloc() // 大きい方のノード作成
			tree.build(greaterNodeID, Node, &greaterEntries)

			tree.build(thisID, Node, &lesserEntries)

			// すでにある親の中間ノードに自分を追加
			// 祖先を遡る
			parentNodeID := tree.ancestor.Back().Value.(util.ID)
			tree.ancestor.Remove(tree.ancestor.Back())
			tree.setParent(thisID, parentNodeID)
			tree.insertNode(parentNodeID, pivotEntry, thisID, greaterNodeID)
		}
	}
}

// insertLeaf  リーフノード操作
func (tree *BPtree) insertLeaf(thisID util.ID, key util.Key, value util.Value) {
	if !tree.isFullLeaf(thisID) { // 保持するエントリー数がFullでない場合
		tree.setType(thisID, Leaf)               // 必ず最初の挿入
		tree.insertLeafEntry(thisID, key, value) // 単純追加
	} else {
		if tree.ancestor.Len() == 0 { // root
			// 新しい中間ノードを作成し,less, greaterに分離した葉をポイントしてルートをその中間ノードに変更
			// 必ず最初の分割
			var entries [ExpandEntrySize]uint64 // Entry分割バッファ
			tree.expandLeafEntry(thisID, key, value, &entries)
			lesserEntries, pivot, greaterEntries := divideLeafEntries(&entries)

			lessLeafID := tree.alloc() + StartGap // 小さい方のリーフを作成　必ず最初にStartGap
			tree.build(lessLeafID, Leaf, &lesserEntries)
			tree.setPrev(lessLeafID, tree.prev(thisID))

			greaterLeafID := tree.alloc() // 大きい方のリーフを作成
			tree.build(greaterLeafID, Leaf, &greaterEntries)
			tree.setPrev(greaterLeafID, lessLeafID) // greater always comes before less
			tree.setNext(lessLeafID, greaterLeafID) // Less is always followed by greater.
			tree.transformLeafToNode(thisID)        // LeafからNodeへ

			tree.insertNodeEntry(thisID, pivot, lessLeafID, greaterLeafID)
		} else {
			// 親の中間ノードに新しい葉のエントリを追加

			// Entry分割
			var entries [ExpandEntrySize]uint64 // Entry分割バッファ
			tree.expandLeafEntry(thisID, key, value, &entries)
			lesserEntries, pivot, greaterEntries := divideLeafEntries(&entries)

			tree.build(thisID, Leaf, &lesserEntries)

			greaterLeafID := tree.alloc() // 大きい方のリーフを作成
			tree.build(greaterLeafID, Leaf, &greaterEntries)
			tree.setPrev(greaterLeafID, thisID) // greater always comes before less
			tree.setNext(greaterLeafID, tree.next(thisID))

			tree.setNext(thisID, greaterLeafID) // Less is always followed by greater.

			// すでにある親の中間ノードに追加
			// 祖先を遡る
			parentNodeID := tree.ancestor.Back().Value.(util.ID)
			tree.ancestor.Remove(tree.ancestor.Back())
			tree.setParent(thisID, parentNodeID)
			tree.insertNode(parentNodeID, pivot, thisID, greaterLeafID)
		}
	}
}

// BPtree B+Tree 根っこ
/*
https://miro.com/app/board/uXjVK-gPPcQ=/
*/
type BPtree struct {
	data     *cache.LRU
	ancestor *list.List
}

// NewBPTree B+Tree
func NewBPTree(config *config.CacheConfig, rootConfig *config.RootConfig) (result *BPtree) {
	result = new(BPtree)
	result.data = cache.NewLRU(config, rootConfig)

	return
}

// Create B+Treeクリエイト
func (tree *BPtree) Create(mode util.CacheMode) {
	tree.data.Create(mode)
	tree.data.Alloc(0x00000000000000000)
}

func (tree *BPtree) Valid() (result bool) {
	if tree != nil {
		if tree.data != nil {
			result = tree.data.Valid()
		}
	}
	return result
}

func (tree *BPtree) Invalid() (result bool) {
	return !tree.Valid()
}

// Open B+Treeオープン
func (tree *BPtree) Open(openMode util.OpenMode, cacheMode util.CacheMode) (valid bool, lookupTable int64, current int64) {
	return tree.data.Open(openMode, cacheMode)
}

// Flash 保存
func (tree *BPtree) Flash() {
	if tree.Valid() {
		tree.data.Flash()
	}
}

// Close 閉じる
func (tree *BPtree) Close() {
	if tree.Valid() {
		tree.data.Close()
	}
}

// AppendRoot ルートを追加
func (tree *BPtree) AppendRoot() (newRoot util.ID) {
	if tree.data != nil {
		newRoot = tree.alloc() + StartGap
		tree.clearEntries(newRoot)
	}

	return newRoot
}

// Cache　キャッシュオブジェクト参照
func (tree *BPtree) Cache() *cache.LRU {
	return tree.data
}

// Find 検索
func (tree *BPtree) Find(rootID util.ID, findKey util.Key) (nodeID util.ID, value util.Value, result bool) {
	if util.CheckID(rootID) {
		if tree.data != nil {
			if findKey != util.UnDef {
				nodeID, _, value = tree.find(rootID, findKey)
			}
			result = true
		}
	} else {
		result = false
	}

	return
}

// FindRange 範囲検索
func (tree *BPtree) FindRange(rootID util.ID, from util.Key, to util.Key, callback func(key, result util.Value) bool) (cont bool) {
	if tree.data != nil {
		if from > to {
			from, to = util.Swap(from, to)
		}

		cont = true // continue?

		startLeafID, _, _ := tree.find(rootID, from)
		endLeafID, _, _ := tree.find(rootID, to)

		targetLeafID := startLeafID
		entryCount := sizeToEntryCount(tree.size(targetLeafID))
		for entryOffset := int32(0); entryOffset < entryCount; entryOffset++ {
			key := tree.key(targetLeafID, entryOffset)
			if key >= from && key <= to {
				cont = callback(key, tree.value(targetLeafID, entryOffset))
				if cont == false {
					break
				}
			}
		}

		if cont == true { // 継続なし
			if startLeafID != endLeafID { // startとendが同一leafなら、そのleafのみ検索で良い
				for {
					targetLeafID = tree.next(targetLeafID)
					if targetLeafID != 0 {
						entryCount = sizeToEntryCount(tree.size(targetLeafID))
						for entryOffset := int32(0); entryOffset < entryCount; entryOffset++ {
							key := tree.key(targetLeafID, entryOffset)
							if key >= from && key <= to {
								cont = callback(key, tree.value(targetLeafID, entryOffset))
								if cont == false {
									break
								}
							}
						}
						if targetLeafID == endLeafID {
							break
						}
					} else {
						break
					}
				}
			}
		}
	}

	return
}

type BPcursor struct {
	tree          *BPtree
	startID       util.ID
	startOffset   int32
	endID         util.ID
	endOffset     int32
	currentID     util.ID
	currentOffset int32
}

// NewCursor カーソルインターフェイス（未テスト)
func (tree *BPtree) NewCursor(rootID util.ID, from util.Key, to util.Key) (cursor *BPcursor) {
	if tree.data != nil {
		if from > to {
			from, to = util.Swap(from, to)
		}

		cursor = new(BPcursor)
		cursor.tree = tree
		cursor.startID, cursor.startOffset, _ = tree.find(rootID, from)
		cursor.endID, cursor.endOffset, _ = tree.find(rootID, to)
		cursor.currentID, cursor.currentOffset = cursor.startID, cursor.startOffset
	}

	return
}

// Next カーソルインターフェイス（未テスト)
func (cursor *BPcursor) Next() (cont bool, result util.ID) {
	if cursor.currentID != 0 {
		entryCount := sizeToEntryCount(cursor.tree.size(cursor.currentID))
		result = cursor.tree.value(cursor.currentID, cursor.currentOffset)
		cont = true
		if cursor.currentID == cursor.endID {
			if cursor.currentOffset <= cursor.endOffset {
				cursor.currentOffset++
			} else {
				cont = false
			}
		} else {
			if cursor.currentOffset < entryCount {
				cursor.currentOffset++
			} else {
				cursor.currentID = cursor.tree.next(cursor.currentID)
				cursor.currentOffset = 0
			}
		}
	}

	return
}

// Insert キー・値挿入
func (tree *BPtree) Insert(rootID util.ID, insertKey util.Key, insertValue util.Value) (result bool) {
	return tree.insert(rootID, insertKey, insertValue)
}

// Update　キーに対応する「値」のアップデート
func (tree *BPtree) Update(rootID util.ID, findKey util.Key, callback func(found bool, target util.Value) util.Value) (result bool) {
	if findKey != util.UnDef { // キー値0は除外
		result = tree.update(rootID, findKey, callback)
	}

	return
}

// local

// Last 現状のサイズ
func (tree *BPtree) Last() (result util.ID) {
	return util.ID(tree.data.Size())
}

func (tree *BPtree) alloc() (result util.ID) {
	return util.ID(tree.data.Size())
}

// get
func (tree *BPtree) get(offset int64) (result uint64) {
	return tree.data.ReadUint64(offset, true)
}

// set
func (tree *BPtree) set(offset int64, value uint64) {
	tree.data.WriteUint64(offset, value)
}

// insert キー・値挿入
func (tree *BPtree) insert(rootID util.ID, insertKey util.Key, insertValue util.Value) (result bool) {
	var value util.Value
	var foundNodeID util.ID
	tree.ancestor = list.New()
	foundNodeID, _, value = tree.findWithRoute(rootID, insertKey)
	if value == NotFound {
		tree.insertLeaf(foundNodeID, insertKey, insertValue)
		result = true
	} else {
		result = false
	}

	return
}

// update
func (tree *BPtree) update(rootID util.ID, insertKey util.Key, callback func(found bool, target util.Value) util.Value) (result bool) {
	nodeID, nodeOffset, nodeValue := tree.find(rootID, insertKey)
	if nodeValue != NotFound { // found then
		tree.setValue(nodeID, nodeOffset, callback(true, tree.getEntry(nodeID, nodeOffset*EachEntry)))
		result = true
	}

	return result
}

// BinarySearch 一つのノードからエントリを検索
func (tree *BPtree) BinarySearch(ID util.ID, key util.Key) util.Offset {
	var compare func(*BPtree, util.Key, util.Offset, util.Offset) util.Offset
	compare = func(tree *BPtree, searchKey util.Key, start util.Offset, end util.Offset) util.Offset {
		result := NotFound
		_range := end - start
		if _range > 1 {
			pivot := (_range >> 1) + start
			_key := tree.key(ID, int32(pivot)) // data[pivot]
			if _key == searchKey {
				result = pivot
			} else if _key > searchKey {
				result = compare(tree, searchKey, start, pivot)
			} else {
				result = compare(tree, searchKey, pivot, end)
			}
		} else {
			pivot := (_range >> 1) + start
			if key == searchKey {
				result = pivot
			}
		}
		return result
	}

	entryCount := sizeToEntryCount(tree.size(ID))
	return compare(tree, key, 0, util.Offset(entryCount))
}

// findAtNode 一つのノードからエントリを検索
func (tree *BPtree) findAtNode(ID util.ID, findKey util.Key) (resultNode util.ID, resultOffset int32, resultValue util.Value) {
	resultOffset = int32(tree.BinarySearch(ID, findKey))
	targetKey := tree.key(ID, resultOffset)
	if targetKey != util.UnDef {
		if util.LessThan(targetKey, findKey) {
			resultNode = tree.lesser(ID, resultOffset)
		} else {
			resultNode = tree.greater(ID, resultOffset)
		}
	}
	return resultNode, resultOffset, NotFound
}

// findAtLeaf 一つのリーフからエントリを検索
func (tree *BPtree) findAtLeaf(ID util.ID, findKey util.Key) (resultNode util.ID, resultOffset int32, resultValue util.Value) {
	resultValue = NotFound
	resultOffset = int32(tree.BinarySearch(ID, findKey))
	targetKey := tree.key(ID, resultOffset)
	if targetKey != util.UnDef {
		if util.Equal(targetKey, findKey) {
			resultValue = tree.value(ID, resultOffset)
		}
	}
	return ID, resultOffset, resultValue
}

// find 検索
func (tree *BPtree) find(rootID util.ID, findKey util.Key) (nodeID util.ID, nodeOffset int32, nodeValue util.Value) {
	switch tree._type(rootID) {
	case None:
		nodeID, nodeOffset, nodeValue = rootID, 0, NotFound
	case Node:
		nodeID, nodeOffset, nodeValue = tree.findAtNode(rootID, findKey)
	case Leaf:
		nodeID, nodeOffset, nodeValue = tree.findAtLeaf(rootID, findKey)
	}

	if (nodeValue == NotFound) && (tree._type(rootID) == Node) {
		nodeID, nodeOffset, nodeValue = tree.find(nodeID, findKey)
	}
	return nodeID, nodeOffset, nodeValue
}

// findWithRoute 祖先を収集しながら検索する。（構築時にのみ使用）
func (tree *BPtree) findWithRoute(rootID util.ID, findKey util.Key) (nodeID util.ID, nodeOffset int32, nodeValue util.Value) {
	switch tree._type(rootID) {
	case None:
		nodeID, nodeOffset, nodeValue = rootID, 0, NotFound
	case Node:
		nodeID, nodeOffset, nodeValue = tree.findAtNode(rootID, findKey)
	case Leaf:
		nodeID, nodeOffset, nodeValue = tree.findAtLeaf(rootID, findKey)
	}

	if (nodeValue == NotFound) && (tree._type(rootID) == Node) {
		tree.ancestor.PushBack(rootID)
		nodeID, nodeOffset, nodeValue = tree.findWithRoute(nodeID, findKey)
	}
	return nodeID, nodeOffset, nodeValue
}

// ----------------------------  -------------------------

// deleteLeafEntry leafからentry削除
func (tree *BPtree) deleteLeafEntry(ID util.ID, index int32) {
	size := tree.size(ID)
	if size == 0 {
		return
	}

	start := index * EachEntry
	for i := start; i < size-EachEntry; i++ {
		tree.setEntry(ID, i, tree.getEntry(ID, i+EachEntry))
	}

	for i := size - EachEntry; i < size; i++ {
		tree.setEntry(ID, i, 0)
	}

	tree.setSize(ID, size-EachEntry)
}

// delete キー・値削除（簡易実装、再バランスは未対応）
func (tree *BPtree) delete(rootID util.ID, deleteKey util.Key) (result bool) {
	if util.CheckID(rootID) && tree.data != nil {
		nodeID, nodeOffset, nodeValue := tree.find(rootID, deleteKey)
		if nodeValue != NotFound {
			tree.deleteLeafEntry(nodeID, nodeOffset)
			result = true
		}
	}

	return
}

// Delete キーに対応するエントリー削除
func (tree *BPtree) Delete(rootID util.ID, deleteKey util.Key) (result bool) {
	if deleteKey != util.UnDef {
		result = tree.delete(rootID, deleteKey)
	}

	return
}
