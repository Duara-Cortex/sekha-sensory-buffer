# Sekha Sensory Layer (`sekha-sensory-buffer`)

**Version 1.0.0**

[![Go Version](https://img.shields.io/badge/go-1.22+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

The **Node 3** (`sekha-node3` &bull; `192.168.8.183`) service of the **Sekha Tri-Node Edge Cognitive Cluster**. Node 3 is the single entry point for all input to memory:

1. The caller sends **labelled** input: dialogue, document or log.
2. Node 3 **chunks** it by type and drops only non-information (the **floor**).
3. When a task is given, every chunk gets a **semantic task score** (MiniLM cosine similarity). The score **routes** chunks and never gates them: a *strong* chunk is processed by Node 2's model, and a *weak* chunk is only stored.
4. Every chunk that passes the floor goes into an in-memory buffer. **Node 2 drains** the buffer and **acknowledges** what it received. Nothing unacknowledged is ever evicted: a full buffer refuses new input instead (back-pressure).

```
caller ──POST /ingest──▶ label check ─▶ chunk by type ─▶ floor ─▶ task score ─▶ buffer ◀──GET /drain── Node 2
                          (400 if missing)              (counts)  (strong/weak)  (503 if full) ──POST /ack──▶
```

---

## Supported Input

| `type` | Chunked by | Body |
| :--- | :--- | :--- |
| `dialogue` | One chunk per turn. A turn larger than `SEKHA_CHUNK_MAX_BYTES` is split into paragraphs (and then at sentence boundaries if a paragraph is still too large). Every part keeps the turn's `speaker` and `turn_id`, plus `parent_id`, `part` and `parts`. | JSON only: `turns: [{speaker, text, turn_id?, ts?}]`, or `speaker` + `text` for a single turn. |
| `document` | One chunk per paragraph. Markdown chunks carry their heading path in `heading`, headings are their own chunks, and fenced code blocks stay whole. csv/xml/json/pdf use the existing parsers' items (rows, entities, objects, text blocks). | JSON `text` with optional `format`, a multipart `file`, or a raw body. |
| `log` | One chunk per line. Indented continuation lines, such as stack traces, stay with their entry. | JSON `text`, a multipart `file`, or a raw body. |

Document formats: `txt`, `md`, `csv`, `xml`, `json`, `pdf`. The format comes from `format` if given, otherwise from the file extension, `Content-Type` or content sniffing, as before. PDF must be sent as a multipart upload or a raw `application/pdf` body, not inside JSON.

**Verbatim text.** For txt, md, dialogue and log input, chunk `text` is exactly the input text; only paragraph and line separators are removed at chunk boundaries. For csv, xml, json and pdf, `text` is exactly what the format parser extracted. For example, a CSV row becomes `col: val | col2: val2`, and the record's `format` field says which parser produced it.

### The floor

Only these chunks are dropped. Nothing is ever dropped for a low score.

| Reason | Rule | Applies to |
| :--- | :--- | :--- |
| `blank` | Chunk is empty or whitespace (a blank log line, an empty turn or item). Blank lines *between* paragraphs are delimiters and are not counted. | all |
| `separator` | Whole chunk matches `SEKHA_FLOOR_SEPARATOR_PATTERN`, e.g. `-----`, `=====`, `****`. | all |
| `heartbeat` | Every line matches `SEKHA_FLOOR_HEARTBEAT_PATTERN` (ping/pong, `64 bytes from … icmp_seq=`, `heartbeat … ok`) and none matches `SEKHA_FLOOR_HEARTBEAT_EXCLUDE_PATTERN`, so `heartbeat missed`, `status not ok` and `Destination Host Unreachable` are kept. | `SEKHA_FLOOR_HEARTBEAT_TYPES` (default `log`, so a chat message saying "ping" is kept) |
| `duplicate` | Exact repeat of an earlier chunk in the same request. For dialogue the key is speaker + text + **the question it answers** (the most recent earlier non-blank turn by a different speaker, matched by its text). So a user's "yes" to "Shall I delete the logs?" and "yes" to "Shall I restart?" are both kept, a second "yes" to the same question text is stored once, and the same words from two different speakers are both kept. | `SEKHA_FLOOR_DEDUP_TYPES` (default all) |

Every ingest response reports `discarded` counts for all four reasons.

### Task score and routing

- With a `task`, the task and every chunk are embedded with **all-MiniLM-L6-v2 (384-D)**. `task_score` is their cosine similarity, rounded to 4 decimal places. For Markdown, the heading path is prepended to the text that is embedded, which gives it context; the stored text doesn't change.
- `strong = task_score >= SEKHA_STRONG_THRESHOLD` (default `0.30`, provisional until set by experiment). Strong chunks are for Node 2's model; weak chunks are stored only. **Both are stored and delivered.**
- Without a task, `task_score` is `null`, `strong` is `false` and `score_status` is `no_task`.
- **If the embedder fails or times out, the chunks are still stored** with `task_score: null`, `strong: false` and `score_status: "embedder_unavailable"`, so Node 2 can re-score them. If the server rejects a whole batch (for example one input is too long), the chunks are retried one at a time, so only the failing chunk is left unscored.
- The embedder is Node 3's own `llama-server` running MiniLM on `127.0.0.1` (see [Local embedding server](#local-embedding-server-on-node-3)). Any OpenAI-compatible `/v1/embeddings` endpoint works via `SEKHA_EMBED_URL`.

---

## API

The service listens on port `8081` by default.

### 1. Ingest labelled input (`POST /api/v1/sensory/ingest`)

**Labels are required and never guessed.**
- Every request needs `type` (`dialogue` | `document` | `log`), `source` and `session`.
- For dialogue, every turn also needs a `speaker`.
- Optional: `task` and, for documents, `format`.
- With JSON, labels go in the body, and unknown fields are rejected, so old `{"origin","data"}` bodies fail loudly. With multipart, labels go in form fields; with a raw body, in the query string.

```bash
# Dialogue
curl -X POST http://192.168.8.183:8081/api/v1/sensory/ingest -H 'Content-Type: application/json' -d '{
  "type": "dialogue", "source": "chat-ui", "session": "s-42",
  "task": "what is the log retention period?",
  "turns": [
    {"speaker": "user", "text": "How long do we keep logs?"},
    {"speaker": "assistant", "text": "The retention period is 90 days.", "turn_id": "a-17", "ts": "2026-09-29T10:00:00Z"}
  ]}'

# Document (Markdown file upload)
curl -X POST http://192.168.8.183:8081/api/v1/sensory/ingest \
  -F type=document -F source=policy-wiki -F session=s-42 -F "task=log retention" -F "file=@policy.md"

# Log (raw body, labels in the query string)
cat syslog.log | curl -X POST "http://192.168.8.183:8081/api/v1/sensory/ingest?type=log&source=syslog&session=boot-7" \
  -H 'Content-Type: text/plain' --data-binary @-
```

`201 Created` (`?summary=true` omits `chunks`):
```json
{
  "memory_id": "01999a3c-6f2e-7c41-9d2a-5b1e0c7f4a10",
  "epoch": "4f9c2a1b7e3d5a60",
  "type": "dialogue", "source": "chat-ui", "session": "s-42",
  "task": "what is the log retention period?",
  "score_status": "scored",
  "strong_threshold": 0.3,
  "accepted": 2, "strong": 1, "weak": 1,
  "discarded": {"blank": 0, "separator": 0, "heartbeat": 0, "duplicate": 0},
  "first_seq": 101, "last_seq": 102,
  "chunks": [
    {
      "id": "5d41402abc4b2a76b9719d911017c592",
      "memory_id": "01999a3c-6f2e-7c41-9d2a-5b1e0c7f4a10",
      "text": "The retention period is 90 days.",
      "type": "dialogue", "source": "chat-ui", "session": "s-42",
      "speaker": "assistant", "turn_id": "a-17",
      "seq": 102, "ts": "2026-09-29T10:00:00Z",
      "task": "what is the log retention period?",
      "task_score": 0.6123, "strong": true, "score_status": "scored"
    }
  ]
}
```

#### Chunk record

| Field | Meaning |
| :--- | :--- |
| `id` | Stable content hash of `text` (first 128 bits of SHA-256, hex). Identical text gives an identical `id`. |
| `memory_id` | One UUIDv7 per ingest call; all its chunks share it and it is returned to the caller. |
| `text` | Chunk text, never rewritten (see *Verbatim text*). |
| `type`, `source`, `session` | The caller's labels. `format` is set for documents. |
| `speaker`, `turn_id` | Dialogue. `turn_id` is the caller's value or `t<N>` (1-based position in the request). |
| `parent_id`, `part`, `parts` | Set only when a unit (turn, paragraph or log entry) was split: `parent_id` is the `id` of the unsplit text; `part` is 1-based. Joining parts in `part` order with blank lines rebuilds the unit (whitespace at the split points may differ). |
| `heading` | Markdown heading path, e.g. `# Policy > ## Retention`. |
| `seq` | Buffer sequence number, strictly increasing within an `epoch`; a single ingest gets contiguous seqs. |
| `ts` | The caller's turn `ts` if given, otherwise the ingest time (UTC). |
| `task` | The task the chunk was scored against. |
| `task_score` | Cosine similarity, or `null` (no task, scoring disabled, or embedder unavailable). |
| `strong` | `task_score >= SEKHA_STRONG_THRESHOLD`. |
| `score_status` | `scored`, `no_task`, `disabled` or `embedder_unavailable`. The response-level field can also be `partial`. |

#### Errors

| Status | `error` | When |
| :--- | :--- | :--- |
| 400 | `missing_label` / `invalid_label` | A label is missing or invalid; `field` names it (e.g. `type`, `turns[3].speaker`). |
| 400 | `invalid_json`, `empty_input`, `unparseable_document`, … | A malformed request. |
| 413 | `payload_too_large` | The body exceeds `SEKHA_MAX_BODY_BYTES`. |
| 413 | `exceeds_buffer_capacity` | One request is larger than the whole buffer; split it. |
| 503 | `buffer_full` | **Back-pressure.** Unacknowledged chunks fill the buffer. Nothing from the request was stored (each ingest is all-or-nothing). Retry after `Retry-After` seconds. |

### 2. Drain / ack contract (for Node 2, Task 30)

Node 2 **pulls**; Node 3 never needs Node 2's address. Delivery is **at-least-once** to a **single consumer**.

**`GET /api/v1/sensory/drain?max=N&after_seq=S`** returns the oldest unacknowledged chunks, in `seq` order:
```json
{"epoch": "4f9c2a1b7e3d5a60", "chunks": [ /* chunk records */ ], "last_seq": 356, "pending": 1200, "more": true}
```
- `max` defaults to `SEKHA_DRAIN_MAX_DEFAULT` and is capped at `SEKHA_DRAIN_MAX_LIMIT`.
- `after_seq` (optional) skips chunks with `seq <= S`, so Node 2 can page ahead before acking.
- `pending` is the count of all unacknowledged chunks; `more` means another page is waiting.
- Draining changes nothing: without an ack, the next drain returns the same chunks.

**`POST /api/v1/sensory/ack`** with `{"epoch": "4f9c2a1b7e3d5a60", "up_to_seq": 356}` is a **cumulative** acknowledgement: every chunk with `seq <= up_to_seq` is received and may be evicted.
- `200 {"epoch", "acked", "acked_up_to_seq", "pending"}`. Acking an already-acked seq is a no-op (`acked: 0`).
- `409 epoch_mismatch`: Node 3 restarted since that drain. Everything it held was lost (see *Limitations*), and seqs restart at 1. Drain again to pick up the new epoch.
- `400 ack_ahead`: `up_to_seq` was never issued.

**Node 2 loop:**
1. `GET /drain`.
2. Persist or process each chunk. Route on `strong`; `score_status: embedder_unavailable` means it may re-score.
3. `POST /ack` with that response's `epoch` and `last_seq`. Ack only what is durably received.
4. If `chunks` is empty, back off briefly and drain again.
5. De-duplicate redeliveries on `(epoch, seq)`, e.g. after a crash between step 2 and step 3.

**Eviction:** an acknowledged chunk stays readable through `GET /buffer` until its space is needed, then acked chunks are evicted oldest first. An unacknowledged chunk is never evicted. When unacknowledged chunks fill `SEKHA_CAPACITY_MB`, ingest returns `503 buffer_full`.

### 3. Query buffer window (`GET /api/v1/sensory/buffer`)

A read-only peek at recent chunk records, acknowledged or not. It never consumes; use `/drain` for delivery. Parameters: `limit` (default `100`, max `10000`), `since_ms`, `since_seq`.

### 4. Stats (`GET /api/v1/sensory/stats`) and health (`GET /healthz`, `GET /api/v1/sensory/health`)

- **Stats:** buffer fill, `pending_count`, `acked_up_to_seq`, `evicted_acked_count` and `backpressure_rejections`. `ingest_telemetry` gives accepted, strong and unscored chunks plus floor discards by reason. `dropped_packets`/`dropped_bytes` count unacknowledged chunks lost to eviction; by design they are always 0 and are kept for existing dashboards.
- **Health:** returns `version` and `epoch`.

### 5. Deprecated: heuristic filter (`POST /api/v1/sensory/filter`)

> **Deprecated.** Kept unchanged only for `sekha-cluster-tool`; it will be removed in Task 34. It does not feed memory, its salience score (entropy, lexical density, entity density, boilerplate and task keywords) plays no part in routing, and responses carry a `Deprecation: true` header. Use `POST /api/v1/sensory/ingest`.

Request and response shapes are the same as before. The default threshold is now read from `SEKHA_FILTER_THRESHOLD` (default `0.75`).

```bash
curl -X POST http://192.168.8.183:8081/api/v1/sensory/filter -H "Content-Type: application/json" \
  -d '{"task": "investigate thermal throttling", "threshold": 0.45,
       "items": [{"origin": "dmesg", "data": "CRITICAL: thermal throttle event on SoC BCM2712 at 82.4C"}]}'
```

---

## Configuration

All configuration is environment variables with built-in defaults. On Node 3 they live in `/etc/sekha/sensory-buffer.env`, loaded by systemd; `sudo make install` creates it from `.env.example` if it doesn't exist and never overwrites it. Invalid values stop the service at startup with a message naming the key. `.env.example` is checked against the code defaults by `go test`.

| Key | Default | Purpose |
| :--- | :--- | :--- |
| `SEKHA_HOST` / `SEKHA_PORT` | `0.0.0.0` / `8081` | Bind address (the flags `-host`/`-port` override). |
| `SEKHA_CAPACITY_MB` | `64` | Buffer byte budget (`-capacity-mb` overrides). It counts text plus each record's in-memory structure, so it tracks live data closely. Plan for 2–3× this in process RAM because of garbage-collection headroom; 64 MB measured about 300 MiB RSS on Node 3. |
| `SEKHA_MAX_BODY_BYTES` | `16777216` | Maximum ingest body. |
| `SEKHA_HTTP_WRITE_TIMEOUT_S` | `120` | HTTP write timeout (ingest waits for embedding). |
| `SEKHA_BACKPRESSURE_RETRY_AFTER_S` | `5` | `Retry-After` on `503 buffer_full`. |
| `SEKHA_DRAIN_MAX_DEFAULT` / `SEKHA_DRAIN_MAX_LIMIT` | `256` / `4096` | Drain page size and its cap. |
| `SEKHA_CHUNK_MAX_BYTES` | `1024` | Chunk size limit (~250 tokens, inside MiniLM's window). |
| `SEKHA_FLOOR_SEPARATOR_PATTERN` | see `.env.example` | Separator-line regex (RE2). |
| `SEKHA_FLOOR_HEARTBEAT_PATTERN` | see `.env.example` | Heartbeat/ping regex (RE2). |
| `SEKHA_FLOOR_HEARTBEAT_EXCLUDE_PATTERN` | see `.env.example` | Words that keep a heartbeat line (error, missed, not, …). |
| `SEKHA_FLOOR_HEARTBEAT_TYPES` | `log` | Types the heartbeat rule applies to. |
| `SEKHA_FLOOR_DEDUP_TYPES` | `log,document,dialogue` | Types with in-request exact-duplicate removal. |
| `SEKHA_EMBED_PROVIDER` | `openai` | `openai` (OpenAI-compatible HTTP) or `none` (no scoring). |
| `SEKHA_EMBED_URL` | `http://127.0.0.1:8086/v1/embeddings` | Embedding endpoint (Node 3's local llama-server). |
| `SEKHA_EMBED_API_KEY` | *(empty)* | Bearer token, if the server needs one. Never logged. |
| `SEKHA_EMBED_MODEL` | `all-MiniLM-L6-v2` | Model name sent to the server. |
| `SEKHA_EMBED_DIM` | `384` | Expected vector size; a mismatch is treated as an embedder failure. |
| `SEKHA_EMBED_TIMEOUT_MS` | `10000` | Per-request embedding timeout. |
| `SEKHA_EMBED_BATCH_SIZE` | `32` | Inputs per embedding request. |
| `SEKHA_STRONG_THRESHOLD` | `0.30` | Strong/weak routing threshold (cosine). |
| `SEKHA_EMBED_SERVER_BIN` / `SEKHA_EMBED_MODEL_PATH` | `/usr/local/bin/llama-server` / `/opt/sekha/models/all-MiniLM-L6-v2.gguf` | Embed unit only: binary and model file. |
| `SEKHA_EMBED_BIND` / `SEKHA_EMBED_PORT` | `127.0.0.1` / `8086` | Embed unit only: listen address (port must match `SEKHA_EMBED_URL`). |
| `SEKHA_EMBED_THREADS` / `SEKHA_EMBED_PARALLEL` | `2` / `1` | Embed unit only: CPU threads and parallel slots. |
| `SEKHA_EMBED_CTX` / `SEKHA_EMBED_UBATCH` | `512` / `512` | Embed unit only: total context (`512 × SEKHA_EMBED_PARALLEL`, since llama-server splits it across slots) and physical batch (≥ tokens in one chunk). |
| `SEKHA_FILTER_THRESHOLD` | `0.75` | Deprecated `/filter` default threshold. |

Keep regex values in single quotes in the env file: systemd strips backslashes from unquoted values.

---

## Limitations

- **Restart loss (accepted).** The buffer is RAM only. Unacknowledged chunks are lost on restart or crash. The new `epoch` tells Node 2 this happened: an ack with the old epoch gets `409`. The shutdown log line records how many were pending.
- **No secret detection.** This service has never detected or redacted secrets, and it still doesn't. Input is stored verbatim and sent to the embedding server, which runs on loopback on Node 3 by default.
- **No idempotency key.** If a client retries after a timeout, the memory can be stored twice under two `memory_id`s. Chunk `id`s are content hashes, so downstream can spot the duplicates.
- **Memory is 2–3× the buffer budget.** `SEKHA_CAPACITY_MB` counts the chunks themselves, and Go's garbage collector needs room on top. Size it against the node's free RAM, e.g. 64 MB → about 300 MiB, 512 MB → about 1.5 GB.
- **Single consumer.** The drain/ack contract assumes one consumer, Node 2.

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

# Run server on port 8081 with 64MB buffer (no local embedder: scoring disabled)
SEKHA_EMBED_PROVIDER=none ./bin/sekha-sensory-buffer -port 8081 -capacity-mb 64
```

### Cross-Compile for Raspberry Pi 5 (`linux/arm64`)
From any workstation (macOS / x86 / Linux):
```bash
make build-arm64
```
Produces `bin/sekha-sensory-buffer-linux-arm64` plus the benchmark, validate and stress tools.

---

## Local embedding server on Node 3

Node 3 runs its own MiniLM so that scoring doesn't depend on Node 1. `systemd/sekha-embed.service` starts `llama-server` on `${SEKHA_EMBED_BIND}:${SEKHA_EMBED_PORT}` (default `127.0.0.1:8086`) with `--embeddings --pooling mean`, one slot and a 512-token context and ubatch, all taken from the env file.

1. Install a `llama-server` binary at `SEKHA_EMBED_SERVER_BIN`, built from llama.cpp for arm64.
2. Put an all-MiniLM-L6-v2 GGUF file at `SEKHA_EMBED_MODEL_PATH`. Use the same file as Node 1's embedding server so vectors are comparable across nodes.
3. `sudo systemctl enable --now sekha-embed.service`, then check it:
   ```bash
   curl -s http://127.0.0.1:8086/v1/embeddings -H 'Content-Type: application/json' \
     -d '{"model":"all-MiniLM-L6-v2","input":["hello"]}' | head -c 200
   ```

To score against another server instead, e.g. Node 1's, set `SEKHA_EMBED_URL` and `SEKHA_EMBED_API_KEY`. To turn scoring off, set `SEKHA_EMBED_PROVIDER=none`.

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
sudo make install     # binary, both units, /etc/sekha/sensory-buffer.env (created once)
sudo systemctl enable sekha-sensory-buffer.service
```

### Option B: Deploy Cross-Compiled Static Binary
```bash
# From developer workstation:
make build-arm64
scp bin/sekha-sensory-buffer-linux-arm64 admin@192.168.8.183:/tmp/sekha-sensory-buffer
scp systemd/sekha-sensory-buffer.service systemd/sekha-embed.service .env.example admin@192.168.8.183:/tmp/

# On Node 3:
sudo mv /tmp/sekha-sensory-buffer /usr/local/bin/sekha-sensory-buffer
sudo chmod +x /usr/local/bin/sekha-sensory-buffer
sudo mv /tmp/sekha-sensory-buffer.service /tmp/sekha-embed.service /etc/systemd/system/
sudo install -d -m 0755 /etc/sekha
test -f /etc/sekha/sensory-buffer.env || sudo install -m 0600 /tmp/.env.example /etc/sekha/sensory-buffer.env
sudo systemctl daemon-reload
sudo systemctl enable --now sekha-sensory-buffer.service
sudo systemctl try-restart sekha-sensory-buffer.service sekha-embed.service   # pick up new units
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

The benchmark sends labelled `type=log` input. Nothing drains the buffer during the run, so a long run eventually gets `503 buffer_full`; that is back-pressure working as designed.

### 2. Sensory Noise Filter Validation (Task 04, deprecated `/filter`)
Evaluates classifier accuracy on synthetic mixed edge workloads (70% noise, 30% signal) and measures latency per chunk:
```bash
./bin/sekha-validate \
  -url http://192.168.8.183:8081/api/v1/sensory/filter \
  -threshold 0.45 \
  -iterations 100
```

### 3. Multi-Rate Subsystem Stress Test (Task 05)
Executes a 4-tier burst sweep (100, 500, 1,000, 5,000 req/s) of labelled log ingest with concurrent `/filter` calls, monitoring SoC temperatures and memory stability. By default it also drains and acks as a stand-in for Node 2; pass `-drain=false` to measure back-pressure instead:
```bash
# From a workstation pointing to Node 3 (not on Node 3: see *Two cores per node*):
./bin/sekha-stress \
  -base-url http://192.168.8.183:8081 \
  -stage-duration 15s \
  -concurrency 32
```

---

## License

Apache License 2.0. Copyright (c) 2026 Duara Cortex Limited.
