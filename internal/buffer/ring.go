package buffer

import (
	"sync"
	"time"
)

// OverheadPerChunk represents estimated memory overhead for struct headers and node pointers.
const OverheadPerChunk int64 = 88

// Chunk represents a single timestamped, sequenced textual unit ingested into the buffer.
type Chunk struct {
	Seq       uint64    `json:"seq"`
	Timestamp time.Time `json:"timestamp"`
	Origin    string    `json:"origin"`
	Data      string    `json:"data"`
	SizeBytes int64     `json:"size_bytes"`
}

// node is an internal doubly linked list element holding a Chunk.
type node struct {
	chunk Chunk
	prev  *node
	next  *node
}

// BufferStats encapsulates telemetry and operational metrics for the buffer.
type BufferStats struct {
	CapacityBytes      int64   `json:"capacity_bytes"`
	UsedBytes          int64   `json:"used_bytes"`
	FillPercent        float64 `json:"fill_percent"`
	CurrentItemCount   int     `json:"current_item_count"`
	TotalIngestedCount uint64  `json:"total_ingested_count"`
	TotalIngestedBytes uint64  `json:"total_ingested_bytes"`
	DroppedPackets     uint64  `json:"dropped_packets"`
	DroppedBytes       uint64  `json:"dropped_bytes"`
	IngestionRateKBps  float64 `json:"ingestion_rate_kbps"`
	UptimeSeconds      float64 `json:"uptime_seconds"`
}

// rateTracker tracks ingested bytes over a rolling 5-second window.
type rateTracker struct {
	slots   [5]int64
	lastSec int64
}

func (rt *rateTracker) record(bytes int64, nowSec int64) {
	if rt.lastSec == 0 {
		rt.lastSec = nowSec
		rt.slots[nowSec%5] = bytes
		return
	}

	diff := nowSec - rt.lastSec
	if diff > 0 {
		if diff >= 5 {
			for i := range rt.slots {
				rt.slots[i] = 0
			}
		} else {
			for s := rt.lastSec + 1; s <= nowSec; s++ {
				rt.slots[s%5] = 0
			}
		}
		rt.lastSec = nowSec
	}
	rt.slots[nowSec%5] += bytes
}

func (rt *rateTracker) rateKBps(nowSec int64) float64 {
	diff := nowSec - rt.lastSec
	if diff >= 5 {
		return 0.0
	}
	var total int64
	// count slots that are within 5 seconds
	for s := nowSec - 4; s <= nowSec; s++ {
		if s > rt.lastSec {
			continue
		}
		if s >= rt.lastSec-4 {
			total += rt.slots[((s%5)+5)%5]
		}
	}
	// Rate over 5 seconds in KB/s
	return float64(total) / 5.0 / 1024.0
}

// RingBuffer is a thread-safe in-memory circular text ring buffer with FIFO eviction.
type RingBuffer struct {
	mu sync.RWMutex

	capacityBytes int64
	usedBytes     int64
	itemCount     int

	head *node // oldest item
	tail *node // newest item

	nextSeq            uint64
	totalIngestedCount uint64
	totalIngestedBytes uint64
	droppedPackets     uint64
	droppedBytes       uint64

	rateTracker rateTracker
	startTime   time.Time
}

// New creates a new RingBuffer with the given memory capacity in bytes.
func New(capacityBytes int64) *RingBuffer {
	if capacityBytes <= 0 {
		capacityBytes = 64 * 1024 * 1024 // Default 64MB
	}
	return &RingBuffer{
		capacityBytes: capacityBytes,
		startTime:     time.Now().UTC(),
		nextSeq:       1,
	}
}

// Ingest inserts a single text packet into the ring buffer, evicting oldest items if capacity is exceeded.
func (rb *RingBuffer) Ingest(origin, data string) Chunk {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return rb.ingestLocked(origin, data, time.Now().UTC())
}

// IngestBatch inserts a slice of text packets into the ring buffer atomically.
func (rb *RingBuffer) IngestBatch(origin string, items []string) []Chunk {
	if len(items) == 0 {
		return nil
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	now := time.Now().UTC()
	result := make([]Chunk, len(items))
	for i, data := range items {
		result[i] = rb.ingestLocked(origin, data, now)
	}
	return result
}

// ingestLocked handles internal insertion and FIFO eviction. Caller must hold rb.mu lock.
func (rb *RingBuffer) ingestLocked(origin, data string, now time.Time) Chunk {
	chunkSize := OverheadPerChunk + int64(len(origin)+len(data))

	// Evict oldest items while capacity is exceeded
	for rb.itemCount > 0 && (rb.usedBytes+chunkSize > rb.capacityBytes) {
		rb.evictOldestLocked()
	}

	chunk := Chunk{
		Seq:       rb.nextSeq,
		Timestamp: now,
		Origin:    origin,
		Data:      data,
		SizeBytes: chunkSize,
	}
	rb.nextSeq++

	newNode := &node{
		chunk: chunk,
		prev:  rb.tail,
		next:  nil,
	}

	if rb.tail != nil {
		rb.tail.next = newNode
		rb.tail = newNode
	} else {
		rb.head = newNode
		rb.tail = newNode
	}

	rb.itemCount++
	rb.usedBytes += chunkSize
	rb.totalIngestedCount++
	rb.totalIngestedBytes += uint64(chunkSize)

	rb.rateTracker.record(chunkSize, now.Unix())

	return chunk
}

// evictOldestLocked removes the oldest item from the head of the buffer.
func (rb *RingBuffer) evictOldestLocked() {
	if rb.head == nil {
		return
	}

	evicted := rb.head
	rb.droppedPackets++
	rb.droppedBytes += uint64(evicted.chunk.SizeBytes)
	rb.usedBytes -= evicted.chunk.SizeBytes
	rb.itemCount--

	if rb.head.next != nil {
		rb.head = rb.head.next
		rb.head.prev = nil
	} else {
		rb.head = nil
		rb.tail = nil
	}
}

// Query retrieves chunks from the buffer based on limit, sinceMs (window in ms), and sinceSeq.
// Results are returned in chronological order (oldest to newest).
func (rb *RingBuffer) Query(limit int, sinceMs int64, sinceSeq uint64) []Chunk {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if limit <= 0 || limit > 10000 {
		limit = 100
	}

	var cutoffTime time.Time
	if sinceMs > 0 {
		cutoffTime = time.Now().UTC().Add(-time.Duration(sinceMs) * time.Millisecond)
	}

	var matched []*node
	curr := rb.tail

	for curr != nil && len(matched) < limit {
		// If sinceMs is set and chunk is older than cutoff, we can stop traversing back
		if sinceMs > 0 && curr.chunk.Timestamp.Before(cutoffTime) {
			break
		}

		// Check sequence filter
		if sinceSeq > 0 && curr.chunk.Seq <= sinceSeq {
			break
		}

		matched = append(matched, curr)
		curr = curr.prev
	}

	// Reverse matched list so that chunks are in chronological order
	n := len(matched)
	result := make([]Chunk, n)
	for i := 0; i < n; i++ {
		result[i] = matched[n-1-i].chunk
	}

	return result
}

// Stats returns current operational metrics for the buffer.
func (rb *RingBuffer) Stats() BufferStats {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	now := time.Now().UTC()
	var fillPercent float64
	if rb.capacityBytes > 0 {
		fillPercent = (float64(rb.usedBytes) / float64(rb.capacityBytes)) * 100.0
	}

	uptime := now.Sub(rb.startTime).Seconds()

	return BufferStats{
		CapacityBytes:      rb.capacityBytes,
		UsedBytes:          rb.usedBytes,
		FillPercent:        fillPercent,
		CurrentItemCount:   rb.itemCount,
		TotalIngestedCount: rb.totalIngestedCount,
		TotalIngestedBytes: rb.totalIngestedBytes,
		DroppedPackets:     rb.droppedPackets,
		DroppedBytes:       rb.droppedBytes,
		IngestionRateKBps:  rb.rateTracker.rateKBps(now.Unix()),
		UptimeSeconds:      uptime,
	}
}
