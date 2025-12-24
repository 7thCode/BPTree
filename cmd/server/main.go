// Package main provides an HTTP API server for the BPTree library.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"bptree2"
)

// Default rootID for single-tree mode (backward compatibility)
const defaultRootID bptree2.RootID = 0

// Server holds the BPTree instance and provides HTTP handlers.
type Server struct {
	tree   *bptree2.BPTree
	path   string
	rootID bptree2.RootID
	mu     sync.RWMutex
}

// Response is a generic JSON response.
type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// StatusResponse contains database status information.
type StatusResponse struct {
	Connected bool   `json:"connected"`
	Path      string `json:"path,omitempty"`
	Count     int    `json:"count,omitempty"`
}

// KeyValue represents a key-value pair with composite key.
type KeyValue struct {
	Key1  uint64 `json:"key1"`
	Key2  uint64 `json:"key2"`
	Value uint64 `json:"value"`
}

// InsertRequest is the request body for INSERT operations.
type InsertRequest struct {
	Key1  uint64 `json:"key1"`
	Key2  uint64 `json:"key2"`
	Value uint64 `json:"value"`
}

// OpenRequest is the request body for opening a database.
type OpenRequest struct {
	Path string `json:"path"`
}

// FindRangeResult contains the results of a range search operation.
type FindRangeResult struct {
	Items []KeyValue `json:"items"`
	Count int        `json:"count"`
}

// BenchmarkRequest is the request body for benchmark operations.
type BenchmarkRequest struct {
	Count     int    `json:"count"`     // Number of operations
	Key1Range uint64 `json:"key1Range"` // Max key1 value for random generation
	Key2Range uint64 `json:"key2Range"` // Max key2 value for random generation
}

// BenchmarkResult contains benchmark timing results.
type BenchmarkResult struct {
	InsertCount     int     `json:"insertCount"`
	InsertTotalMs   float64 `json:"insertTotalMs"`
	InsertAvgUs     float64 `json:"insertAvgUs"`
	InsertOpsPerSec float64 `json:"insertOpsPerSec"`
	SearchCount     int     `json:"searchCount"`
	SearchTotalMs   float64 `json:"searchTotalMs"`
	SearchAvgUs     float64 `json:"searchAvgUs"`
	SearchOpsPerSec float64 `json:"searchOpsPerSec"`
	SearchHitRate   float64 `json:"searchHitRate"`
	FinalCount      int     `json:"finalCount"`
}

var server = &Server{}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Setup CORS middleware
	corsHandler := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			h(w, r)
		}
	}

	// Register handlers
	http.HandleFunc("/api/status", corsHandler(server.handleStatus))
	http.HandleFunc("/api/open", corsHandler(server.handleOpen))
	http.HandleFunc("/api/close", corsHandler(server.handleClose))
	http.HandleFunc("/api/find", corsHandler(server.handleFind))
	http.HandleFunc("/api/insert", corsHandler(server.handleInsert))
	http.HandleFunc("/api/delete", corsHandler(server.handleDelete))
	http.HandleFunc("/api/findrange", corsHandler(server.handleFindRange))
	http.HandleFunc("/api/flash", corsHandler(server.handleFlash))
	http.HandleFunc("/api/count", corsHandler(server.handleCount))
	http.HandleFunc("/api/benchmark", corsHandler(server.handleBenchmark))

	// Legacy endpoints for backward compatibility
	http.HandleFunc("/api/get", corsHandler(server.handleFind))
	http.HandleFunc("/api/put", corsHandler(server.handleInsert))
	http.HandleFunc("/api/scan", corsHandler(server.handleFindRange))

	log.Printf("BPTree API server starting on port %s...\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func writeJSON(w http.ResponseWriter, status int, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status := StatusResponse{
		Connected: s.tree != nil,
		Path:      s.path,
	}

	if s.tree != nil {
		status.Count = s.tree.Count(s.rootID)
	}

	writeJSON(w, http.StatusOK, Response{Success: true, Data: status})
}

func (s *Server) handleOpen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
		return
	}

	var req OpenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid request body"})
		return
	}

	if req.Path == "" {
		writeJSON(w, http.StatusBadRequest, Response{Error: "path is required"})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Close existing tree if open
	if s.tree != nil {
		s.tree.Close()
	}

	tree, err := bptree2.Open(req.Path)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, Response{Error: fmt.Sprintf("failed to open database: %v", err)})
		return
	}

	// Try to use default root, create if it doesn't exist
	rootID := defaultRootID
	if tree.RootCount() == 0 {
		// New database, create first root
		newRootID, err := tree.CreateRoot()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, Response{Error: fmt.Sprintf("failed to create root: %v", err)})
			return
		}
		rootID = newRootID
	}

	s.tree = tree
	s.path = req.Path
	s.rootID = rootID

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data: StatusResponse{
			Connected: true,
			Path:      req.Path,
			Count:     tree.Count(rootID),
		},
	})
}

func (s *Server) handleClose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tree == nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "no database open"})
		return
	}

	if err := s.tree.Close(); err != nil {
		writeJSON(w, http.StatusInternalServerError, Response{Error: fmt.Sprintf("failed to close: %v", err)})
		return
	}

	s.tree = nil
	s.path = ""

	writeJSON(w, http.StatusOK, Response{Success: true})
}

func (s *Server) handleFind(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
		return
	}

	key1Str := r.URL.Query().Get("key1")
	key2Str := r.URL.Query().Get("key2")
	if key1Str == "" || key2Str == "" {
		writeJSON(w, http.StatusBadRequest, Response{Error: "key1 and key2 are required"})
		return
	}

	key1, err := strconv.ParseUint(key1Str, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid key1 format"})
		return
	}

	key2, err := strconv.ParseUint(key2Str, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid key2 format"})
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.tree == nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "no database open"})
		return
	}

	val, found := s.tree.Find(s.rootID, key1, key2)
	if !found {
		writeJSON(w, http.StatusNotFound, Response{Error: "key not found"})
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    KeyValue{Key1: key1, Key2: key2, Value: val},
	})
}

func (s *Server) handleInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
		return
	}

	var req InsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid request body"})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tree == nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "no database open"})
		return
	}

	if err := s.tree.Insert(s.rootID, req.Key1, req.Key2, req.Value); err != nil {
		writeJSON(w, http.StatusInternalServerError, Response{Error: fmt.Sprintf("insert failed: %v", err)})
		return
	}

	// Auto-flash to ensure data is persisted
	if err := s.tree.Flash(); err != nil {
		writeJSON(w, http.StatusInternalServerError, Response{Error: fmt.Sprintf("flash failed: %v", err)})
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    KeyValue{Key1: req.Key1, Key2: req.Key2, Value: req.Value},
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
		return
	}

	key1Str := r.URL.Query().Get("key1")
	key2Str := r.URL.Query().Get("key2")
	if key1Str == "" || key2Str == "" {
		writeJSON(w, http.StatusBadRequest, Response{Error: "key1 and key2 are required"})
		return
	}

	key1, err := strconv.ParseUint(key1Str, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid key1 format"})
		return
	}

	key2, err := strconv.ParseUint(key2Str, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid key2 format"})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tree == nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "no database open"})
		return
	}

	deleted := s.tree.Delete(s.rootID, key1, key2)

	// Auto-flash to ensure data is persisted
	if deleted {
		if err := s.tree.Flash(); err != nil {
			writeJSON(w, http.StatusInternalServerError, Response{Error: fmt.Sprintf("flash failed: %v", err)})
			return
		}
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    map[string]bool{"deleted": deleted},
	})
}

func (s *Server) handleFindRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
		return
	}

	start1Str := r.URL.Query().Get("start1")
	start2Str := r.URL.Query().Get("start2")
	end1Str := r.URL.Query().Get("end1")
	end2Str := r.URL.Query().Get("end2")

	if start1Str == "" || start2Str == "" || end1Str == "" || end2Str == "" {
		writeJSON(w, http.StatusBadRequest, Response{Error: "start1, start2, end1, and end2 are required"})
		return
	}

	start1, err := strconv.ParseUint(start1Str, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid start1 format"})
		return
	}

	start2, err := strconv.ParseUint(start2Str, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid start2 format"})
		return
	}

	end1, err := strconv.ParseUint(end1Str, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid end1 format"})
		return
	}

	end2, err := strconv.ParseUint(end2Str, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid end2 format"})
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.tree == nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "no database open"})
		return
	}

	var items []KeyValue
	err = s.tree.FindRange(s.rootID, start1, start2, end1, end2, func(key1, key2, value uint64) bool {
		items = append(items, KeyValue{Key1: key1, Key2: key2, Value: value})
		return true
	})

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, Response{Error: fmt.Sprintf("findrange failed: %v", err)})
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    FindRangeResult{Items: items, Count: len(items)},
	})
}

func (s *Server) handleFlash(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tree == nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "no database open"})
		return
	}

	if err := s.tree.Flash(); err != nil {
		writeJSON(w, http.StatusInternalServerError, Response{Error: fmt.Sprintf("flash failed: %v", err)})
		return
	}

	writeJSON(w, http.StatusOK, Response{Success: true})
}

func (s *Server) handleCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.tree == nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "no database open"})
		return
	}

	count := s.tree.Count(s.rootID)
	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    map[string]int{"count": count},
	})
}

func (s *Server) handleBenchmark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "method not allowed"})
		return
	}

	var req BenchmarkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "invalid request body"})
		return
	}

	if req.Count <= 0 {
		req.Count = 10000
	}
	if req.Key1Range == 0 {
		req.Key1Range = 1000000
	}
	if req.Key2Range == 0 {
		req.Key2Range = 1000000
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tree == nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: "no database open"})
		return
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Generate random keys for testing (key1 and key2 with separate ranges)
	keys1 := make([]uint64, req.Count)
	keys2 := make([]uint64, req.Count)
	for i := range keys1 {
		keys1[i] = rng.Uint64() % req.Key1Range
		keys2[i] = rng.Uint64() % req.Key2Range
	}

	// Benchmark Insert
	insertStart := time.Now()
	for i := 0; i < len(keys1); i++ {
		if err := s.tree.Insert(s.rootID, keys1[i], keys2[i], uint64(i)); err != nil {
			writeJSON(w, http.StatusInternalServerError, Response{Error: fmt.Sprintf("insert failed at %d: %v", i, err)})
			return
		}
	}
	insertDuration := time.Since(insertStart)

	// Benchmark Search
	hits := 0
	searchStart := time.Now()
	for i := 0; i < len(keys1); i++ {
		if _, found := s.tree.Find(s.rootID, keys1[i], keys2[i]); found {
			hits++
		}
	}
	searchDuration := time.Since(searchStart)

	// Calculate metrics
	insertTotalMs := float64(insertDuration.Microseconds()) / 1000.0
	searchTotalMs := float64(searchDuration.Microseconds()) / 1000.0

	result := BenchmarkResult{
		InsertCount:     req.Count,
		InsertTotalMs:   insertTotalMs,
		InsertAvgUs:     float64(insertDuration.Microseconds()) / float64(req.Count),
		InsertOpsPerSec: float64(req.Count) / insertDuration.Seconds(),
		SearchCount:     req.Count,
		SearchTotalMs:   searchTotalMs,
		SearchAvgUs:     float64(searchDuration.Microseconds()) / float64(req.Count),
		SearchOpsPerSec: float64(req.Count) / searchDuration.Seconds(),
		SearchHitRate:   float64(hits) / float64(req.Count) * 100,
		FinalCount:      s.tree.Count(s.rootID),
	}

	writeJSON(w, http.StatusOK, Response{Success: true, Data: result})
}
