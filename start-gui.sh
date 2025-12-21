#!/bin/bash

# BPTree GUI Startup Script

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_DIR="$SCRIPT_DIR/cmd/server"
ELECTRON_DIR="$SCRIPT_DIR/gui/electron"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}🌲 BPTree GUI${NC}"
echo "================================"

# Check if Go server binary exists or build it
echo -e "${YELLOW}Building Go server...${NC}"
cd "$SERVER_DIR"
go build -o bptree-server .

# Start the Go server in background
echo -e "${GREEN}Starting API server on port 8080...${NC}"
./bptree-server &
SERVER_PID=$!

# Wait for server to be ready
sleep 1

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Shutting down...${NC}"
    kill $SERVER_PID 2>/dev/null || true
    exit 0
}

trap cleanup SIGINT SIGTERM

# Install Electron dependencies if needed
cd "$ELECTRON_DIR"
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}Installing Electron dependencies...${NC}"
    npm install
fi

# Start Electron app
echo -e "${GREEN}Starting Electron app...${NC}"
npm start

# When Electron exits, cleanup
cleanup
