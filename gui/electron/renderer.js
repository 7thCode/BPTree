// BPTree GUI - Renderer Process

const API_BASE = 'http://localhost:8080/api';

// DOM Elements
const elements = {
    // Status
    connectionStatus: document.getElementById('connectionStatus'),
    entryCount: document.getElementById('entryCount'),
    dbPathDisplay: document.getElementById('dbPathDisplay'),
    
    // Database
    dbPath: document.getElementById('dbPath'),
    btnOpen: document.getElementById('btnOpen'),
    btnClose: document.getElementById('btnClose'),
    
    // Operations
    putKey: document.getElementById('putKey'),
    putValue: document.getElementById('putValue'),
    btnPut: document.getElementById('btnPut'),
    
    getKey: document.getElementById('getKey'),
    btnGet: document.getElementById('btnGet'),
    
    deleteKey: document.getElementById('deleteKey'),
    btnDelete: document.getElementById('btnDelete'),
    
    scanStart: document.getElementById('scanStart'),
    scanEnd: document.getElementById('scanEnd'),
    btnScan: document.getElementById('btnScan'),
    
    // Actions
    btnCheckpoint: document.getElementById('btnCheckpoint'),
    btnClear: document.getElementById('btnClear'),
    
    // Benchmark
    benchCount: document.getElementById('benchCount'),
    benchKeyRange: document.getElementById('benchKeyRange'),
    btnBenchmark: document.getElementById('btnBenchmark'),
    
    // Results
    results: document.getElementById('results')
};

// State
let isConnected = false;

// API Functions
async function apiRequest(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_BASE}${endpoint}`, {
            ...options,
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            }
        });
        const data = await response.json();
        return data;
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// Logging
function log(type, message, data = null) {
    // Remove welcome message if present
    const welcome = elements.results.querySelector('.welcome-message');
    if (welcome) {
        welcome.remove();
    }
    
    const entry = document.createElement('div');
    entry.className = `log-entry ${type}`;
    
    const timestamp = new Date().toLocaleTimeString('ja-JP');
    
    let html = `
        <div class="timestamp">${timestamp}</div>
        <div class="message">${message}</div>
    `;
    
    if (data !== null) {
        html += `<div class="data"><pre>${formatData(data)}</pre></div>`;
    }
    
    entry.innerHTML = html;
    elements.results.appendChild(entry);
    elements.results.scrollTop = elements.results.scrollHeight;
}

function formatData(data) {
    if (typeof data === 'object') {
        return JSON.stringify(data, null, 2);
    }
    return String(data);
}

// UI State Management
function updateConnectionState(connected, path = '', count = 0) {
    isConnected = connected;
    
    // Update status indicator
    const dot = elements.connectionStatus.querySelector('.status-dot');
    const text = elements.connectionStatus.querySelector('.status-text');
    
    dot.className = `status-dot ${connected ? 'connected' : 'disconnected'}`;
    text.textContent = connected ? 'Connected' : 'Disconnected';
    
    // Update footer
    elements.entryCount.textContent = `Entries: ${connected ? count : '-'}`;
    elements.dbPathDisplay.textContent = `Path: ${connected ? path : 'Not connected'}`;
    
    // Update buttons
    elements.btnOpen.disabled = connected;
    elements.btnClose.disabled = !connected;
    elements.btnPut.disabled = !connected;
    elements.btnGet.disabled = !connected;
    elements.btnDelete.disabled = !connected;
    elements.btnScan.disabled = !connected;
    elements.btnCheckpoint.disabled = !connected;
    elements.btnBenchmark.disabled = !connected;
}

async function refreshStatus() {
    const result = await apiRequest('/status');
    if (result.success && result.data) {
        updateConnectionState(result.data.connected, result.data.path, result.data.count);
    }
}

// Event Handlers
async function handleOpen() {
    const path = elements.dbPath.value.trim();
    if (!path) {
        log('error', 'Please enter a database path');
        return;
    }
    
    log('info', `Opening database: ${path}...`);
    const result = await apiRequest('/open', {
        method: 'POST',
        body: JSON.stringify({ path })
    });
    
    if (result.success) {
        log('success', `Database opened successfully`, result.data);
        updateConnectionState(true, result.data.path, result.data.count);
    } else {
        log('error', `Failed to open database: ${result.error}`);
    }
}

async function handleClose() {
    log('info', 'Closing database...');
    const result = await apiRequest('/close', { method: 'POST' });
    
    if (result.success) {
        log('success', 'Database closed');
        updateConnectionState(false);
    } else {
        log('error', `Failed to close database: ${result.error}`);
    }
}

async function handlePut() {
    const key = elements.putKey.value;
    const value = elements.putValue.value;
    
    if (key === '' || value === '') {
        log('error', 'Please enter both key and value');
        return;
    }
    
    log('info', `Put: key=${key}, value=${value}`);
    const result = await apiRequest('/put', {
        method: 'POST',
        body: JSON.stringify({ key: parseInt(key), value: parseInt(value) })
    });
    
    if (result.success) {
        log('success', 'Put successful', result.data);
        refreshStatus();
    } else {
        log('error', `Put failed: ${result.error}`);
    }
}

async function handleGet() {
    const key = elements.getKey.value;
    
    if (key === '') {
        log('error', 'Please enter a key');
        return;
    }
    
    log('info', `Get: key=${key}`);
    const result = await apiRequest(`/get?key=${key}`);
    
    if (result.success) {
        log('success', `Found: key=${result.data.key}, value=${result.data.value}`, result.data);
    } else {
        log('error', `Get failed: ${result.error}`);
    }
}

async function handleDelete() {
    const key = elements.deleteKey.value;
    
    if (key === '') {
        log('error', 'Please enter a key');
        return;
    }
    
    log('info', `Delete: key=${key}`);
    const result = await apiRequest(`/delete?key=${key}`, { method: 'DELETE' });
    
    if (result.success) {
        if (result.data.deleted) {
            log('success', `Key ${key} deleted`);
        } else {
            log('info', `Key ${key} was not found`);
        }
        refreshStatus();
    } else {
        log('error', `Delete failed: ${result.error}`);
    }
}

async function handleScan() {
    const start = elements.scanStart.value;
    const end = elements.scanEnd.value;
    
    if (start === '' || end === '') {
        log('error', 'Please enter both start and end');
        return;
    }
    
    log('info', `Scan: range=[${start}, ${end}]`);
    const result = await apiRequest(`/scan?start=${start}&end=${end}`);
    
    if (result.success) {
        log('success', `Found ${result.data.count} entries`);
        
        if (result.data.items && result.data.items.length > 0) {
            const entry = document.createElement('div');
            entry.className = 'log-entry info';
            
            let tableHtml = `
                <table class="scan-results">
                    <thead>
                        <tr><th>Key</th><th>Value</th></tr>
                    </thead>
                    <tbody>
            `;
            
            for (const item of result.data.items) {
                tableHtml += `<tr><td>${item.key}</td><td>${item.value}</td></tr>`;
            }
            
            tableHtml += '</tbody></table>';
            entry.innerHTML = tableHtml;
            elements.results.appendChild(entry);
            elements.results.scrollTop = elements.results.scrollHeight;
        }
    } else {
        log('error', `Scan failed: ${result.error}`);
    }
}

async function handleCheckpoint() {
    log('info', 'Saving checkpoint...');
    const result = await apiRequest('/checkpoint', { method: 'POST' });
    
    if (result.success) {
        log('success', 'Checkpoint saved successfully');
    } else {
        log('error', `Checkpoint failed: ${result.error}`);
    }
}

function handleClearLog() {
    elements.results.innerHTML = `
        <div class="welcome-message">
            <p>Log cleared</p>
            <p>Ready for new operations.</p>
        </div>
    `;
}

async function handleBenchmark() {
    const count = parseInt(elements.benchCount.value) || 10000;
    const keyRange = parseInt(elements.benchKeyRange.value) || 1000000;
    
    log('info', `Running benchmark: ${count.toLocaleString()} operations, key range: 0-${keyRange.toLocaleString()}...`);
    elements.btnBenchmark.disabled = true;
    elements.btnBenchmark.textContent = '⏳ Running...';
    
    try {
        const result = await apiRequest('/benchmark', {
            method: 'POST',
            body: JSON.stringify({ count, keyRange })
        });
        
        if (result.success) {
            const d = result.data;
            log('success', '📊 Benchmark completed!');
            
            // Create benchmark result card
            const entry = document.createElement('div');
            entry.className = 'log-entry benchmark-result';
            entry.innerHTML = `
                <div class="benchmark-header">📊 Benchmark Results</div>
                <div class="benchmark-grid">
                    <div class="benchmark-section">
                        <h4>✏️ Insert Performance</h4>
                        <table class="benchmark-table">
                            <tr><td>Count</td><td>${d.insertCount.toLocaleString()}</td></tr>
                            <tr><td>Total Time</td><td>${d.insertTotalMs.toFixed(2)} ms</td></tr>
                            <tr><td>Avg per Op</td><td>${d.insertAvgUs.toFixed(2)} µs</td></tr>
                            <tr class="highlight"><td>Throughput</td><td>${formatOps(d.insertOpsPerSec)}</td></tr>
                        </table>
                    </div>
                    <div class="benchmark-section">
                        <h4>🔍 Search Performance</h4>
                        <table class="benchmark-table">
                            <tr><td>Count</td><td>${d.searchCount.toLocaleString()}</td></tr>
                            <tr><td>Total Time</td><td>${d.searchTotalMs.toFixed(2)} ms</td></tr>
                            <tr><td>Avg per Op</td><td>${d.searchAvgUs.toFixed(2)} µs</td></tr>
                            <tr class="highlight"><td>Throughput</td><td>${formatOps(d.searchOpsPerSec)}</td></tr>
                        </table>
                    </div>
                </div>
                <div class="benchmark-summary">
                    <span>Hit Rate: ${d.searchHitRate.toFixed(1)}%</span>
                    <span>Final Entries: ${d.finalCount.toLocaleString()}</span>
                </div>
            `;
            elements.results.appendChild(entry);
            elements.results.scrollTop = elements.results.scrollHeight;
            refreshStatus();
        } else {
            log('error', `Benchmark failed: ${result.error}`);
        }
    } catch (error) {
        log('error', `Benchmark error: ${error.message}`);
    } finally {
        elements.btnBenchmark.disabled = false;
        elements.btnBenchmark.textContent = '🚀 Run Benchmark';
    }
}

function formatOps(ops) {
    if (ops >= 1000000) {
        return (ops / 1000000).toFixed(2) + ' M ops/sec';
    } else if (ops >= 1000) {
        return (ops / 1000).toFixed(2) + ' K ops/sec';
    }
    return ops.toFixed(2) + ' ops/sec';
}

// Initialize
function init() {
    // Bind event handlers
    elements.btnOpen.addEventListener('click', handleOpen);
    elements.btnClose.addEventListener('click', handleClose);
    elements.btnPut.addEventListener('click', handlePut);
    elements.btnGet.addEventListener('click', handleGet);
    elements.btnDelete.addEventListener('click', handleDelete);
    elements.btnScan.addEventListener('click', handleScan);
    elements.btnCheckpoint.addEventListener('click', handleCheckpoint);
    elements.btnClear.addEventListener('click', handleClearLog);
    elements.btnBenchmark.addEventListener('click', handleBenchmark);
    
    // Enter key handlers
    elements.dbPath.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !isConnected) handleOpen();
    });
    
    elements.putValue.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && isConnected) handlePut();
    });
    
    elements.getKey.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && isConnected) handleGet();
    });
    
    elements.deleteKey.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && isConnected) handleDelete();
    });
    
    elements.scanEnd.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && isConnected) handleScan();
    });
    
    // Check server status
    refreshStatus();
    
    // Periodic status check
    setInterval(refreshStatus, 5000);
}

// Start
document.addEventListener('DOMContentLoaded', init);
