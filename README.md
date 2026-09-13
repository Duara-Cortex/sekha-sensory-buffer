# Sekha Sensory Text Ring Buffer (`sekha-sensory-buffer`)

[![Go Version](https://img.shields.io/badge/go-1.22+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

A high-throughput, low-latency in-memory circular text ring buffer micro-service designed for **Node 3** (`sekha-node3` &bull; `192.168.8.183`) in the **Sekha Tri-Node Edge Cognitive Cluster**.

In the Sekha cognitive architecture, Node 3 acts as the **Sensory Memory Ingestion Layer** (analogous to human iconic/echoic sensory memory). It ingests high-frequency raw text streams (system logs, prompt inputs, web scrapes, tool telemetry, documents) into a deterministic in-memory ring buffer before cognitive attention gates and noise-suppression filters evaluate them.

---

## Supported Text Formats

The service natively reads, decodes, and chunks the 5 primary text formats with **zero external dependencies**:

| Format | Extension | Ingestion Methods | Processing Strategy |
| :--- | :--- | :--- | :--- |
| **Plain Text** | `.txt`, `.log` | Multipart upload, raw body (`text/plain`), JSON | Chunked by paragraph (`\n\n`), by line (`\n`), or as single document. |
| **Markdown** | `.md` | Multipart upload, raw body (`text/markdown`) | Splits by Markdown headings (`#`, `##`, `###`) preserving heading context per section. |
| **Tabular Data** | `.csv`, `.tsv` | Multipart upload, raw body (`text/csv`) | Parses with `encoding/csv`; formats each row into contextual key-values (`col1: val1 \| col2: val2`). |
| **Structured Feeds**| `.xml`, `.rss` | Multipart upload, raw body (`application/xml`) | Parses with `encoding/xml`; extracts text nodes, repeated entities (`<item>`, `<event>`), and tag paths. |
| **PDF Documents** | `.pdf` | Multipart upload, raw body (`application/pdf`) | FlateDecode stream decompressor (`compress/zlib`) and `BT...ET` operator parser; extracts textual streams without external C/OCR dependencies. |
| **JSON Data** | `.json`, `.jsonl`, `.ndjson` | Direct JSON POST (`application/json`), multipart, ndjson | Supports single objects, batch arrays, item containers, and newline-delimited JSON streams. |

---

## Features

- **Pure Go Standard Library**: Zero external runtime dependencies, minimal attack surface, instant compilation, and low memory overhead (~10–15 MB base RSS).
- **Strict Byte-Budgeted Ring Buffer**: Configurable memory envelope (e.g. 50MB–200MB, default 64MB) to safely operate within the 4GB RAM envelope of Node 3.
- **Deterministic FIFO Eviction**: Automatically evicts oldest unconsumed packets when capacity is saturated.
- **Metadata Tagging**: Every chunk receives a monotonic 64-bit sequence number (`seq`), high-precision UTC timestamp (`timestamp`), and source identifier (`origin`).
- **Flexible Ingestion Formats**:
  - File upload via `multipart/form-data` (`curl -F "file=@doc.pdf"`)
  - Direct HTTP body streaming with auto-format detection
  - Structured single JSON (`{"origin": "agent", "data": "..."}`)
  - Batch JSON (`{"origin": "telemetry", "items": ["line 1", "line 2"]}`)
  - Newline-delimited JSON (`application/x-ndjson`) or raw text streams (`text/plain`)
- **Window Querying**: Retrieve the latest $K$ chunks, only chunks within the last $N$ milliseconds, or chunks since a sequence watermark (`since_seq`).
- **Real-Time Telemetry**: Live fill percentage, rolling ingestion rate (KB/s), total bytes ingested, and eviction counters.
- **Production Daemonization**: Complete `systemd` service unit with automatic restart on failure and boot-time launch.

---

## Micro-Service API Specification

The daemon listens on **port `8081`** by default.

### 1. Ingest Text Streams & Documents (`POST /api/v1/sensory/ingest`)

#### Option A: File Upload (`multipart/form-data`)
Upload any `.txt`, `.pdf`, `.csv`, `.xml`, or `.md` file directly:
```bash
# Upload a PDF paper
curl -X POST http://192.168.8.183:8081/api/v1/sensory/ingest \
  -F "file=@whitepaper.pdf" \
  -F "origin=research-paper"

# Upload a CSV metrics file
curl -X POST http://192.168.8.183:8081/api/v1/sensory/ingest \
  -F "file=@cluster_telemetry.csv"

# Upload Markdown notes
curl -X POST http://192.168.8.183:8081/api/v1/sensory/ingest \
  -F "file=@meeting_notes.md"
```

Response (`201 Created`):
```json
{
  "status": "ingested",
  "format": "pdf",
  "ingested_count": 8,
  "first_seq": 105,
  "last_seq": 112,
  "timestamp": "2026-09-12T10:45:00.123456789Z"
}
```

#### Option B: Direct Body Uploads
```bash
# Markdown
curl -X POST http://192.168.8.183:8081/api/v1/sensory/ingest?origin=specs \
  -H "Content-Type: text/markdown" \
  --data-binary @architecture.md

# CSV (converts rows to key-value sensory chunks)
curl -X POST http://192.168.8.183:8081/api/v1/sensory/ingest?origin=metrics \
  -H "Content-Type: text/csv" \
  --data-binary @sensors.csv

# XML
curl -X POST http://192.168.8.183:8081/api/v1/sensory/ingest?origin=rss \
  -H "Content-Type: application/xml" \
  --data-binary @feed.xml

# Plain Text / Logs
cat syslog.log | curl -X POST "http://192.168.8.183:8081/api/v1/sensory/ingest?origin=syslog&chunk_by=line" \
  -H "Content-Type: text/plain" \
  --data-binary @-
```

#### Option C: Structured JSON
```bash
curl -X POST http://192.168.8.183:8081/api/v1/sensory/ingest \
  -H "Content-Type: application/json" \
  -d '{"origin": "sensor-stream", "data": "Raw telemetry event data: temperature=42.1C"}'
```

---

### 2. Query Sensory Buffer Window (`GET /api/v1/sensory/buffer`)

Query parameters:
- `limit` (int, default `100`, max `10000`): Maximum chunks to return (most recent).
- `since_ms` (int64, optional): Only return chunks ingested within the last $N$ milliseconds.
- `since_seq` (uint64, optional): Only return chunks strictly newer than this sequence number (for watermark tracking).

#### Example: Get latest 10 chunks from the last 5 seconds
```bash
curl "http://192.168.8.183:8081/api/v1/sensory/buffer?limit=10&since_ms=5000"
```

Response (`200 OK`):
```json
{
  "count": 2,
  "chunks": [
    {
      "seq": 101,
      "timestamp": "2026-09-12T10:45:01.000Z",
      "origin": "sensor-stream",
      "data": "Raw telemetry event data...",
      "size_bytes": 138
    },
    {
      "seq": 102,
      "timestamp": "2026-09-12T10:45:01.050Z",
      "origin": "sensor-stream",
      "data": "Raw telemetry event data #2...",
      "size_bytes": 141
    }
  ]
}
```

---

### 3. Sensory Noise Filtering & Salience Gating (`POST /api/v1/sensory/filter`)

Filters incoming text streams or queries the active buffer, suppressing boilerplate noise (>60% reduction) and retaining high-information content for working memory.

#### Option A: Filter Directly From In-Memory Buffer
```bash
curl -X POST "http://192.168.8.183:8081/api/v1/sensory/filter?from_buffer=true&limit=50&threshold=0.45&task=detect+memory+leaks"
```

#### Option B: Filter Arbitrary Payload
```bash
curl -X POST http://192.168.8.183:8081/api/v1/sensory/filter \
  -H "Content-Type: application/json" \
  -d '{
    "task": "investigate thermal throttling",
    "threshold": 0.45,
    "include_discarded": false,
    "items": [
      {"origin": "syslog", "data": "ping 64 bytes from 192.168.8.1: icmp_seq=1 ttl=64 time=0.4 ms"},
      {"origin": "dmesg", "data": "CRITICAL: thermal throttle event on SoC BCM2712 at 82.4C"}
    ]
  }'
```

Response (`200 OK`):
```json
{
  "total_evaluated": 2,
  "salient_count": 1,
  "discarded_count": 1,
  "noise_reduction_ratio": 0.5,
  "threshold": 0.45,
  "task_directive": "investigate thermal throttling",
  "salient_chunks": [
    {
      "chunk": {
        "seq": 2,
        "origin": "dmesg",
        "data": "CRITICAL: thermal throttle event on SoC BCM2712 at 82.4C"
      },
      "metrics": {
        "entropy": 4.15,
        "lexical_density": 0.78,
        "entity_density": 0.85,
        "task_relevance": 0.90,
        "salience_score": 0.835,
        "is_salient": true
      }
    }
  ]
}
```

---

### 4. Buffer Telemetry & Statistics (`GET /api/v1/sensory/stats`)

Returns operational health and load metrics.

```bash
curl http://192.168.8.183:8081/api/v1/sensory/stats
```

Response (`200 OK`):
```json
{
  "capacity_bytes": 67108864,
  "used_bytes": 1245088,
  "fill_percent": 1.85,
  "current_item_count": 8920,
  "total_ingested_count": 150000,
  "total_ingested_bytes": 21500000,
  "dropped_packets": 0,
  "dropped_bytes": 0,
  "ingestion_rate_kbps": 320.45,
  "uptime_seconds": 3600.5
}
```

---

### 4. Health Check (`GET /healthz` or `GET /api/v1/sensory/health`)

```bash
curl http://192.168.8.183:8081/healthz
```

---

## Building and Running

### Prerequisites
- Go 1.22+ (or 1.21+)

### Native Build & Run
```bash
# Build native binaries
make build

# Run tests
make test

# Run server on port 8081 with 64MB buffer
./bin/sekha-sensory-buffer -port 8081 -capacity-mb 64
```

### Cross-Compile for Raspberry Pi 5 (`linux/arm64`)
From any workstation (macOS / x86 / Linux):
```bash
make build-arm64
```
Produces `bin/sekha-sensory-buffer-linux-arm64` and `bin/sekha-benchmark-linux-arm64`.

---

## Deployment on Node 3 (`sekha-node3`)

### Option A: Direct Git / Build on Node 3
```bash
# 1. On Node 3: Install base packages and official Go 1.22 (ARM64)
sudo apt update && sudo apt install -y git make curl tar build-essential
curl -fsSL https://go.dev/dl/go1.22.7.linux-arm64.tar.gz -o /tmp/go1.22.7.linux-arm64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf /tmp/go1.22.7.linux-arm64.tar.gz
rm /tmp/go1.22.7.linux-arm64.tar.gz

# 2. Add Go to PATH using printf (avoids heredoc/tee whitespace issues)
sudo sh -c "printf 'export PATH=\$PATH:/usr/local/go/bin:\$HOME/go/bin\n' > /etc/profile.d/go.sh"
printf 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin\n' >> ~/.bashrc
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
go version

# 3. Clone, build, and install
git clone git@github.com:Duara-Cortex/sekha-sensory-buffer.git
cd sekha-sensory-buffer
make build
sudo systemctl stop sekha-sensory-buffer.service 2>/dev/null || true
sudo cp bin/sekha-sensory-buffer /usr/local/bin/
sudo cp systemd/sekha-sensory-buffer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sekha-sensory-buffer.service
```

### Option B: Deploy Cross-Compiled Static Binary
```bash
# From developer workstation:
make build-arm64
scp bin/sekha-sensory-buffer-linux-arm64 admin@192.168.8.183:/tmp/sekha-sensory-buffer
scp systemd/sekha-sensory-buffer.service admin@192.168.8.183:/tmp/

# On Node 3:
sudo mv /tmp/sekha-sensory-buffer /usr/local/bin/sekha-sensory-buffer
sudo chmod +x /usr/local/bin/sekha-sensory-buffer
sudo mv /tmp/sekha-sensory-buffer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sekha-sensory-buffer.service
```

---

## Load Testing & Benchmarks

### 1. Ingestion Load Test (10,000 lines/sec)
```bash
./bin/sekha-benchmark \
  -url http://192.168.8.183:8081/api/v1/sensory/ingest \
  -stats-url http://192.168.8.183:8081/api/v1/sensory/stats \
  -lines-per-sec 10000 \
  -duration 10s \
  -batch-size 50
```

### 2. Sensory Noise Filter Validation (Task 04)
Evaluates classifier accuracy on synthetic mixed edge workloads (70% noise, 30% signal) and measures latency per chunk:
```bash
./bin/sekha-validate \
  -url http://192.168.8.183:8081/api/v1/sensory/filter \
  -threshold 0.45 \
  -iterations 100
```

---

## License

Apache License 2.0. Copyright (c) 2026 Duara Cortex Limited.
