package buffer

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRingBufferBasicIngestAndStats(t *testing.T) {
	// Small buffer of 1000 bytes
	rb := New(1000)

	chunk1 := rb.Ingest("test", "hello world")
	if chunk1.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", chunk1.Seq)
	}
	if chunk1.Origin != "test" || chunk1.Data != "hello world" {
		t.Fatalf("unexpected chunk content: %+v", chunk1)
	}

	stats := rb.Stats()
	if stats.CurrentItemCount != 1 {
		t.Fatalf("expected item count 1, got %d", stats.CurrentItemCount)
	}
	if stats.TotalIngestedCount != 1 {
		t.Fatalf("expected total ingested 1, got %d", stats.TotalIngestedCount)
	}
	if stats.DroppedPackets != 0 {
		t.Fatalf("expected 0 dropped packets, got %d", stats.DroppedPackets)
	}
	if stats.FillPercent <= 0 || stats.FillPercent > 100 {
		t.Fatalf("unexpected fill percent: %f", stats.FillPercent)
	}
}

func TestRingBufferFIFOEviction(t *testing.T) {
	// Capacity for roughly 2 chunks
	// Chunk overhead is 88 bytes + origin + data
	// Let data be 100 bytes, origin 5 bytes => chunk size = 193 bytes
	// Buffer capacity = 400 bytes (holds 2 chunks, 3rd will evict first)
	capacity := int64(400)
	rb := New(capacity)

	data := string(make([]byte, 100))
	c1 := rb.Ingest("sys", data)
	c2 := rb.Ingest("sys", data)

	if c1.Seq != 1 || c2.Seq != 2 {
		t.Fatalf("unexpected sequences: c1=%d, c2=%d", c1.Seq, c2.Seq)
	}

	stats := rb.Stats()
	if stats.CurrentItemCount != 2 {
		t.Fatalf("expected 2 items, got %d", stats.CurrentItemCount)
	}
	if stats.DroppedPackets != 0 {
		t.Fatalf("expected 0 dropped, got %d", stats.DroppedPackets)
	}

	// Insert 3rd chunk: should evict c1
	c3 := rb.Ingest("sys", data)
	if c3.Seq != 3 {
		t.Fatalf("expected seq 3, got %d", c3.Seq)
	}

	stats = rb.Stats()
	if stats.CurrentItemCount != 2 {
		t.Fatalf("expected 2 items after eviction, got %d", stats.CurrentItemCount)
	}
	if stats.TotalIngestedCount != 3 {
		t.Fatalf("expected total ingested 3, got %d", stats.TotalIngestedCount)
	}
	if stats.DroppedPackets != 1 {
		t.Fatalf("expected 1 dropped packet, got %d", stats.DroppedPackets)
	}

	// Verify query returns only c2 and c3 in chronological order
	chunks := rb.Query(10, 0, 0)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks returned, got %d", len(chunks))
	}
	if chunks[0].Seq != 2 || chunks[1].Seq != 3 {
		t.Fatalf("expected seqs [2, 3], got [%d, %d]", chunks[0].Seq, chunks[1].Seq)
	}
}

func TestRingBufferBatchIngest(t *testing.T) {
	rb := New(1024 * 1024)
	items := []string{"chunk 1", "chunk 2", "chunk 3"}
	result := rb.IngestBatch("batch-origin", items)

	if len(result) != 3 {
		t.Fatalf("expected 3 chunks in result, got %d", len(result))
	}
	for i, c := range result {
		if c.Seq != uint64(i+1) {
			t.Fatalf("expected seq %d, got %d", i+1, c.Seq)
		}
		if c.Data != items[i] {
			t.Fatalf("expected data %s, got %s", items[i], c.Data)
		}
	}
}

func TestRingBufferQueryFilters(t *testing.T) {
	rb := New(1024 * 1024)

	rb.Ingest("originA", "item 1")
	rb.Ingest("originB", "item 2")
	rb.Ingest("originC", "item 3")
	rb.Ingest("originD", "item 4")
	rb.Ingest("originE", "item 5")

	// Limit test: top 2 latest chunks
	resLimit := rb.Query(2, 0, 0)
	if len(resLimit) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(resLimit))
	}
	if resLimit[0].Seq != 4 || resLimit[1].Seq != 5 {
		t.Fatalf("expected [4, 5], got [%d, %d]", resLimit[0].Seq, resLimit[1].Seq)
	}

	// sinceSeq test: items after seq 3
	resSeq := rb.Query(10, 0, 3)
	if len(resSeq) != 2 {
		t.Fatalf("expected 2 chunks after seq 3, got %d", len(resSeq))
	}
	if resSeq[0].Seq != 4 || resSeq[1].Seq != 5 {
		t.Fatalf("expected [4, 5], got [%d, %d]", resSeq[0].Seq, resSeq[1].Seq)
	}

	// sinceMs test: all chunks should be within last 5000ms
	resTime := rb.Query(10, 5000, 0)
	if len(resTime) != 5 {
		t.Fatalf("expected 5 chunks within 5000ms, got %d", len(resTime))
	}
}

func TestRingBufferConcurrentAccess(t *testing.T) {
	rb := New(50 * 1024 * 1024) // 50MB
	var wg sync.WaitGroup

	numProducers := 10
	itemsPerProducer := 500

	// Concurrent ingestors
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(prodID int) {
			defer wg.Done()
			origin := fmt.Sprintf("producer-%d", prodID)
			for i := 0; i < itemsPerProducer; i++ {
				rb.Ingest(origin, fmt.Sprintf("payload-%d-%d", prodID, i))
			}
		}(p)
	}

	// Concurrent readers
	numReaders := 4
	stopReaders := make(chan struct{})
	var readerWg sync.WaitGroup
	for r := 0; r < numReaders; r++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					_ = rb.Query(20, 0, 0)
					_ = rb.Stats()
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	wg.Wait()
	close(stopReaders)
	readerWg.Wait()

	stats := rb.Stats()
	expectedTotal := uint64(numProducers * itemsPerProducer)
	if stats.TotalIngestedCount != expectedTotal {
		t.Fatalf("expected %d total ingested, got %d", expectedTotal, stats.TotalIngestedCount)
	}
}
