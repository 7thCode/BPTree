# BPTree

MMAP-based B+Tree library for Go with uint64 keys and values.

## Features

- **Memory-mapped I/O** for efficient page access
- **Concurrent reads** supported
- **Flash-based persistence** for durability
- **Range scans** with callback API

## Installation

```bash
go get github.com/oda/bptree
```

## Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/oda/bptree"
)

func main() {
    // Open or create a B+Tree file
    tree, err := bptree.Open("data.db")
    if err != nil {
        log.Fatal(err)
    }
    defer tree.Close()

    // Insert key-value pairs
    tree.Put(1, 100)
    tree.Put(2, 200)
    tree.Put(3, 300)

    // Get a value
    val, ok := tree.Get(2)
    if ok {
        fmt.Printf("Key 2 = %d\n", val) // Key 2 = 200
    }

    // Range scan
    tree.Scan(1, 3, func(key, value uint64) bool {
        fmt.Printf("Key: %d, Value: %d\n", key, value)
        return true // continue iteration
    })

    // Persist to disk
    tree.Flash()

    // Delete a key
    tree.Delete(2)
}
```

## API

| Method                               | Description                  |
| ------------------------------------ | ---------------------------- |
| `Open(path string) (*BPTree, error)` | Open or create a B+Tree file |
| `Close() error`                      | Close the tree               |
| `Get(key uint64) (uint64, bool)`     | Get value by key             |
| `Put(key, value uint64) error`       | Insert or update             |
| `Delete(key uint64) bool`            | Delete a key                 |
| `Scan(start, end uint64, fn) error`  | Range scan with callback     |
| `Flash() error`                 | Sync changes to disk         |
| `Count() int`                        | Count all entries (O(n))     |

## Architecture

```
┌─────────────────┐
│  Public API     │  bptree.go
├─────────────────┤
│  Node Layer     │  internal/node/
├─────────────────┤
│  Pager Layer    │  internal/pager/
├─────────────────┤
│  MMAP Layer     │  internal/mmap/
└─────────────────┘
```

## Benchmarks

Run benchmarks:

```bash
go test -bench=. -benchmem
```

## License

MIT
