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
    putKey1: document.getElementById('putKey1'),
    putKey2: document.getElementById('putKey2'),
    putValue: document.getElementById('putValue'),
    btnPut: document.getElementById('btnPut'),

    getKey1: document.getElementById('getKey1'),
    getKey2: document.getElementById('getKey2'),
    btnGet: document.getElementById('btnGet'),

    deleteKey1: document.getElementById('deleteKey1'),
    deleteKey2: document.getElementById('deleteKey2'),
    btnDelete: document.getElementById('btnDelete'),

    scanStart1: document.getElementById('scanStart1'),
    scanStart2: document.getElementById('scanStart2'),
    scanEnd1: document.getElementById('scanEnd1'),
    scanEnd2: document.getElementById('scanEnd2'),
    btnScan: document.getElementById('btnScan'),

    // Actions
    btnFlash: document.getElementById('btnFlash'),
    btnClear: document.getElementById('btnClear'),

    // Benchmark
    benchCount: document.getElementById('benchCount'),
    benchKey1Range: document.getElementById('benchKey1Range'),
    benchKey2Range: document.getElementById('benchKey2Range'),
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
    elements.btnFlash.disabled = !connected;
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
    const key1 = elements.putKey1.value;
    const key2 = elements.putKey2.value;
    const value = elements.putValue.value;

    if (key1 === '' || key2 === '' || value === '') {
        log('error', 'Please enter key1, key2, and value');
        return;
    }

    log('info', `Put: key1=${key1}, key2=${key2}, value=${value}`);
    const result = await apiRequest('/put', {
        method: 'POST',
        body: JSON.stringify({ key1: parseInt(key1), key2: parseInt(key2), value: parseInt(value) })
    });

    if (result.success) {
        log('success', 'Put successful', result.data);
        refreshStatus();
    } else {
        log('error', `Put failed: ${result.error}`);
    }
}

async function handleGet() {
    const key1 = elements.getKey1.value;
    const key2 = elements.getKey2.value;

    if (key1 === '' || key2 === '') {
        log('error', 'Please enter key1 and key2');
        return;
    }

    log('info', `Get: key1=${key1}, key2=${key2}`);
    const result = await apiRequest(`/get?key1=${key1}&key2=${key2}`);

    if (result.success) {
        log('success', `Found: key1=${result.data.key1}, key2=${result.data.key2}, value=${result.data.value}`, result.data);
    } else {
        log('error', `Get failed: ${result.error}`);
    }
}

async function handleDelete() {
    const key1 = elements.deleteKey1.value;
    const key2 = elements.deleteKey2.value;

    if (key1 === '' || key2 === '') {
        log('error', 'Please enter key1 and key2');
        return;
    }

    log('info', `Delete: key1=${key1}, key2=${key2}`);
    const result = await apiRequest(`/delete?key1=${key1}&key2=${key2}`, { method: 'DELETE' });

    if (result.success) {
        if (result.data.deleted) {
            log('success', `Key (${key1}, ${key2}) deleted`);
        } else {
            log('info', `Key (${key1}, ${key2}) was not found`);
        }
        refreshStatus();
    } else {
        log('error', `Delete failed: ${result.error}`);
    }
}

async function handleScan() {
    const start1 = elements.scanStart1.value;
    const start2 = elements.scanStart2.value;
    const end1 = elements.scanEnd1.value;
    const end2 = elements.scanEnd2.value;

    if (start1 === '' || start2 === '' || end1 === '' || end2 === '') {
        log('error', 'Please enter start1, start2, end1, and end2');
        return;
    }

    log('info', `Scan: range=[(${start1}, ${start2}), (${end1}, ${end2})]`);
    const result = await apiRequest(`/scan?start1=${start1}&start2=${start2}&end1=${end1}&end2=${end2}`);

    if (result.success) {
        log('success', `Found ${result.data.count} entries`);

        if (result.data.items && result.data.items.length > 0) {
            const entry = document.createElement('div');
            entry.className = 'log-entry info';

            let tableHtml = `
                <table class="scan-results">
                    <thead>
                        <tr><th>Key1</th><th>Key2</th><th>Value</th></tr>
                    </thead>
                    <tbody>
            `;

            for (const item of result.data.items) {
                tableHtml += `<tr><td>${item.key1}</td><td>${item.key2}</td><td>${item.value}</td></tr>`;
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

async function handleFlash() {
    log('info', 'Flashing...');
    const result = await apiRequest('/flash', { method: 'POST' });

    if (result.success) {
        log('success', 'Flash saved successfully');
    } else {
        log('error', `Flash failed: ${result.error}`);
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
    const key1Range = parseInt(elements.benchKey1Range.value) || 1000000;
    const key2Range = parseInt(elements.benchKey2Range.value) || 1000000;

    log('info', `Running benchmark: ${count.toLocaleString()} ops, Key1: 0-${key1Range.toLocaleString()}, Key2: 0-${key2Range.toLocaleString()}...`);
    elements.btnBenchmark.disabled = true;
    elements.btnBenchmark.textContent = '⏳ Running...';

    try {
        const result = await apiRequest('/benchmark', {
            method: 'POST',
            body: JSON.stringify({ count, key1Range, key2Range })
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
    elements.btnFlash.addEventListener('click', handleFlash);
    elements.btnClear.addEventListener('click', handleClearLog);
    elements.btnBenchmark.addEventListener('click', handleBenchmark);

    // Enter key handlers
    elements.dbPath.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !isConnected) handleOpen();
    });

    elements.putValue.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && isConnected) handlePut();
    });

    elements.getKey2.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && isConnected) handleGet();
    });

    elements.deleteKey2.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && isConnected) handleDelete();
    });

    elements.scanEnd2.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && isConnected) handleScan();
    });

    // Check server status
    refreshStatus();

    // Periodic status check
    setInterval(refreshStatus, 5000);
}

// Start
document.addEventListener('DOMContentLoaded', init);
