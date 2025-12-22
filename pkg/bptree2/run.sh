#!/bin/bash

# BPTree2 Startup Script
# This script runs tests and verifies the BPTree implementation

set -e  # Exit on error

cd "$(dirname "$0")"

echo "🔧 Running go mod tidy..."
go mod tidy

echo ""
echo "🧪 Running tests..."
go test ./... -v

echo ""
echo "✅ All tests passed!"
echo ""
echo "📊 Test coverage:"
go test ./... -cover

echo ""
echo "✨ BPTree2 is ready to use!"
