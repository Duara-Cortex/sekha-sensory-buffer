package buffer

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// OverheadPerChunk represents estimated memory overhead for struct headers and slice slots.
const OverheadPerChunk int64 = 88

// Record is one labelled chunk held in the buffer until Node 2 acknowledges it.
type Record struct {
	ID          string    `json:"id"`
	MemoryID    string    `json:"memory_id"`
	Text        string    `json:"text"`
	Type        string    `json:"type"`
	Format      string    `json:"format,omitempty"`
	Source      string    `json:"source"`
	Session     string    `json:"session"`
	Speaker     string    `json:"speaker,omitempty"`
	TurnID      string    `json:"turn_id,omitempty"`
	ParentID    string    `json:"parent_id,omitempty"`
	Part        int       `json:"part,omitempty"`
	Parts       int       `json:"parts,omitempty"`
	Heading     string    `json:"heading,omitempty"`
	Seq         uint64    `json:"seq"`
	TS          time.Time `json:"ts"`
	Task        string    `json:"task,omitempty"`
	TaskScore   *float64  `json:"task_score"`
	Strong      bool      `json:"strong"`
	ScoreStatus string    `json:"score_status"`
}

// size estimates the in-memory footprint of a record for capacity accounting.
func (r *Record) size() int64 {
	return OverheadPerChunk + int64(len(r.ID)+len(r.MemoryID)+len(r.Text)+len(r.Type)+len(r.Format)+
		len(r.Source)+len(r.Session)+len(r.Speaker)+len(r.TurnID)+len(r.ParentID)+len(r.Heading)+len(r.Task)+len(r.ScoreStatus))
}

// Chunk is the legacy record shape used by the deprecated /filter endpoint and the classifier.
type Chunk struct {
	Seq       uint64    `json:"seq"`
	Timestamp time.Time `json:"timestamp"`
	Origin    string    `json:"origin"`
	Data      string    `json:"data"`
	SizeBytes int64     `json:"size_bytes"`
}

// Legacy converts a record to the legacy chunk shape.
func (r Record) Legacy() Chunk {
	return Chunk{Seq: r.Seq, Timestamp: r.TS, Origin: r.Source, Data: r.Text, SizeBytes: r.size()}
}

// FullError is returned when an append does not fit even after evicting every acknowledged record.
type FullError struct {
	NeededBytes   int64
	FreeBytes     int64
	PendingChunks int
}

func (e *FullError) Error() string {
	return fmt.Sprintf("buffer full: need %d bytes, %d free after evicting acknowledged chunks, %d chunks awaiting acknowledgement",
		e.NeededBytes, e.FreeBytes, e.PendingChunks)
}

// TooLargeError is returned when a single append exceeds the whole buffer capacity.
type TooLargeError struct {
	NeededBytes   int64
	CapacityBytes int64
}

func (e *TooLargeError) Error() string {
	return fmt.Sprintf("payload needs %d bytes but buffer capacity is %d bytes", e.NeededBytes, e.CapacityBytes)
}

var (
	// ErrEpochMismatch means the ack refers to a previous process lifetime (Node 3 restarted).
	ErrEpochMismatch = errors.New("epoch mismatch: the buffer restarted and its contents were lost; drain again to get the new epoch")
	// ErrAckAhead means the ack refers to a sequence number that was never issued.
	ErrAckAhead = errors.New("up_to_seq is beyond the last issued sequence number")
)

// BufferStats encapsulates telemetry and operational metrics for the buffer.
type BufferStats struct {
	Epoch              string  `json:"epoch"`
	CapacityBytes      int64   `json:"capacity_bytes"`
	UsedBytes          int64   `json:"used_bytes"`
	FillPercent        float64 `json:"fill_percent"`
	CurrentItemCount   int     `json:"current_item_count"`
	PendingCount       int     `json:"pending_count"`
	AckedRetainedCount int     `json:"acked_retained_count"`
	AckedUpToSeq       uint64  `json:"acked_up_to_seq"`
	LastSeq            uint64  `json:"last_seq"`
	TotalIngestedCount uint64  `json:"total_ingested_count"`
	TotalIngestedBytes uint64  `json:"total_ingested_bytes"`
	EvictedAckedCount  uint64  `json:"evicted_acked_count"`
	BackpressureCount  uint64  `json:"backpressure_rejections"`
	// DroppedPackets/DroppedBytes count unacknowledged chunks lost to eviction. By design
	// they are always zero; they are kept so existing dashboards keep working.
	DroppedPackets    uint64  `json:"dropped_packets"`
	DroppedBytes      uint64  `json:"dropped_bytes"`
	IngestionRateKBps float64 `json:"ingestion_rate_kbps"`
	UptimeSeconds     float64 `json:"uptime_seconds"`
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

type entry struct {
	rec  Record
	size int64
}

// RingBuffer is a thread-safe, byte-budgeted FIFO of records. Records are evicted only
// after they have been acknowledged; when unacknowledged records fill the capacity,
// Append returns a *FullError instead of evicting (back-pressure).
type RingBuffer struct {
	mu sync.RWMutex

	epoch         string
	capacityBytes int64
	usedBytes     int64

	items      []entry // oldest first; seqs strictly increasing
	nextSeq    uint64
	ackedUpTo  uint64
	ackedCount int // number of acknowledged records still retained (always a prefix of items)

	totalIngestedCount uint64
	totalIngestedBytes uint64
	evictedAcked       uint64
	backpressure       uint64

	rateTracker rateTracker
	startTime   time.Time
}

// New creates a new RingBuffer with the given memory capacity in bytes and a fresh epoch.
func New(capacityBytes int64) *RingBuffer {
	if capacityBytes <= 0 {
		panic("buffer: capacity must be > 0")
	}
	return &RingBuffer{
		epoch:         newEpoch(),
		capacityBytes: capacityBytes,
		startTime:     time.Now().UTC(),
		nextSeq:       1,
	}
}

func newEpoch() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// Epoch identifies this process lifetime. It changes on every restart.
func (rb *RingBuffer) Epoch() string { return rb.epoch }

// Append stores recs atomically: either all are stored (with contiguous seqs assigned)
// or none are. Only acknowledged records are evicted to make room.
func (rb *RingBuffer) Append(recs []Record) ([]Record, error) {
	if len(recs) == 0 {
		return nil, nil
	}
	var needed int64
	sizes := make([]int64, len(recs))
	for i := range recs {
		sizes[i] = recs[i].size()
		needed += sizes[i]
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	if needed > rb.capacityBytes {
		rb.backpressure++
		return nil, &TooLargeError{NeededBytes: needed, CapacityBytes: rb.capacityBytes}
	}

	var reclaimable int64
	for i := 0; i < rb.ackedCount; i++ {
		reclaimable += rb.items[i].size
	}
	free := rb.capacityBytes - rb.usedBytes
	if needed > free+reclaimable {
		rb.backpressure++
		return nil, &FullError{NeededBytes: needed, FreeBytes: free + reclaimable, PendingChunks: len(rb.items) - rb.ackedCount}
	}

	for rb.capacityBytes-rb.usedBytes < needed {
		rb.evictOldestAckedLocked()
	}

	now := time.Now().UTC()
	out := make([]Record, len(recs))
	for i := range recs {
		r := recs[i]
		r.Seq = rb.nextSeq
		rb.nextSeq++
		if r.TS.IsZero() {
			r.TS = now
		}
		rb.items = append(rb.items, entry{rec: r, size: sizes[i]})
		rb.usedBytes += sizes[i]
		rb.totalIngestedCount++
		rb.totalIngestedBytes += uint64(sizes[i])
		out[i] = r
	}
	rb.rateTracker.record(needed, now.Unix())
	return out, nil
}

// evictOldestAckedLocked removes the head record, which must be acknowledged. Caller holds rb.mu.
func (rb *RingBuffer) evictOldestAckedLocked() {
	if rb.ackedCount == 0 {
		panic("buffer: attempted to evict an unacknowledged record")
	}
	head := rb.items[0]
	rb.items[0] = entry{}
	rb.items = rb.items[1:]
	rb.ackedCount--
	rb.usedBytes -= head.size
	rb.evictedAcked++
	if cap(rb.items) > 1024 && len(rb.items) < cap(rb.items)/4 {
		rb.items = append(make([]entry, 0, len(rb.items)*2), rb.items...)
	}
}

// DrainResult is a page of unacknowledged records.
type DrainResult struct {
	Epoch   string   `json:"epoch"`
	Chunks  []Record `json:"chunks"`
	LastSeq uint64   `json:"last_seq"`
	Pending int      `json:"pending"`
	More    bool     `json:"more"`
}

// Drain returns up to max unacknowledged records with seq > afterSeq, oldest first.
// It does not modify the buffer: repeated calls return the same records until acked.
func (rb *RingBuffer) Drain(afterSeq uint64, max int) DrainResult {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	floor := afterSeq
	if rb.ackedUpTo > floor {
		floor = rb.ackedUpTo
	}
	res := DrainResult{Epoch: rb.epoch, Chunks: []Record{}, Pending: len(rb.items) - rb.ackedCount}
	for i := rb.ackedCount; i < len(rb.items); i++ {
		r := rb.items[i].rec
		if r.Seq <= floor {
			continue
		}
		if len(res.Chunks) >= max {
			res.More = true
			break
		}
		res.Chunks = append(res.Chunks, r)
		res.LastSeq = r.Seq
	}
	return res
}

// Ack marks every record with seq <= upTo as acknowledged (cumulative) and returns how many
// records became newly acknowledged. Acknowledged records remain readable via Query until
// their space is needed.
func (rb *RingBuffer) Ack(epoch string, upTo uint64) (int, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if epoch != rb.epoch {
		return 0, ErrEpochMismatch
	}
	if upTo >= rb.nextSeq {
		return 0, ErrAckAhead
	}
	if upTo <= rb.ackedUpTo {
		return 0, nil
	}
	n := 0
	for i := rb.ackedCount; i < len(rb.items) && rb.items[i].rec.Seq <= upTo; i++ {
		n++
	}
	rb.ackedCount += n
	rb.ackedUpTo = upTo
	return n, nil
}

// Pending returns the number of unacknowledged records.
func (rb *RingBuffer) Pending() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return len(rb.items) - rb.ackedCount
}

// Query retrieves records (acknowledged or not) based on limit, sinceMs (window in ms), and
// sinceSeq. Results are returned in chronological order (oldest to newest). It never consumes.
func (rb *RingBuffer) Query(limit int, sinceMs int64, sinceSeq uint64) []Record {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if limit <= 0 || limit > 10000 {
		limit = 100
	}

	var cutoffTime time.Time
	if sinceMs > 0 {
		cutoffTime = time.Now().UTC().Add(-time.Duration(sinceMs) * time.Millisecond)
	}

	start := len(rb.items)
	for start > 0 && len(rb.items)-start < limit {
		r := rb.items[start-1].rec
		if sinceMs > 0 && r.TS.Before(cutoffTime) {
			break
		}
		if sinceSeq > 0 && r.Seq <= sinceSeq {
			break
		}
		start--
	}

	result := make([]Record, 0, len(rb.items)-start)
	for i := start; i < len(rb.items); i++ {
		result = append(result, rb.items[i].rec)
	}
	return result
}

// Stats returns current operational metrics for the buffer.
func (rb *RingBuffer) Stats() BufferStats {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	now := time.Now().UTC()
	return BufferStats{
		Epoch:              rb.epoch,
		CapacityBytes:      rb.capacityBytes,
		UsedBytes:          rb.usedBytes,
		FillPercent:        float64(rb.usedBytes) / float64(rb.capacityBytes) * 100.0,
		CurrentItemCount:   len(rb.items),
		PendingCount:       len(rb.items) - rb.ackedCount,
		AckedRetainedCount: rb.ackedCount,
		AckedUpToSeq:       rb.ackedUpTo,
		LastSeq:            rb.nextSeq - 1,
		TotalIngestedCount: rb.totalIngestedCount,
		TotalIngestedBytes: rb.totalIngestedBytes,
		EvictedAckedCount:  rb.evictedAcked,
		BackpressureCount:  rb.backpressure,
		IngestionRateKBps:  rb.rateTracker.rateKBps(now.Unix()),
		UptimeSeconds:      now.Sub(rb.startTime).Seconds(),
	}
}
