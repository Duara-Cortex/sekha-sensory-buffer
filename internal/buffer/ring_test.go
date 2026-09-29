package buffer

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func rec(text string) Record {
	return Record{Text: text, Type: "log", Source: "test", Session: "s1", ScoreStatus: "no_task"}
}

func recs(n int, size int) []Record {
	out := make([]Record, n)
	for i := range out {
		out[i] = rec(fmt.Sprintf("%04d%s", i, strings.Repeat("x", size)))
	}
	return out
}

func TestAppendAssignsContiguousSeqsAndTimestamps(t *testing.T) {
	rb := New(1024 * 1024)
	out, err := rb.Append(recs(3, 10))
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range out {
		if r.Seq != uint64(i+1) || r.TS.IsZero() {
			t.Fatalf("record %d: seq=%d ts=%v", i, r.Seq, r.TS)
		}
	}
	st := rb.Stats()
	if st.CurrentItemCount != 3 || st.PendingCount != 3 || st.LastSeq != 3 || st.UsedBytes <= 0 {
		t.Fatalf("unexpected stats %+v", st)
	}
}

// Acceptance 5: filling past capacity with nothing acknowledged returns back-pressure errors
// and evicts nothing.
func TestBackPressureNeverEvictsUnacknowledged(t *testing.T) {
	one := rec(strings.Repeat("a", 100))
	capacity := one.size() * 10
	rb := New(capacity)

	for i := 0; i < 10; i++ {
		if _, err := rb.Append([]Record{rec(strings.Repeat("a", 100))}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		_, err := rb.Append([]Record{rec(strings.Repeat("b", 100))})
		var full *FullError
		if !errors.As(err, &full) {
			t.Fatalf("overflow append %d: want *FullError, got %v", i, err)
		}
		if full.PendingChunks != 10 {
			t.Fatalf("pending in error = %d, want 10", full.PendingChunks)
		}
	}
	st := rb.Stats()
	if st.CurrentItemCount != 10 || st.PendingCount != 10 || st.EvictedAckedCount != 0 || st.DroppedPackets != 0 || st.BackpressureCount != 5 {
		t.Fatalf("buffer changed under back-pressure: %+v", st)
	}
	got := rb.Drain(0, 100)
	if len(got.Chunks) != 10 || got.Chunks[0].Seq != 1 || got.Chunks[9].Seq != 10 {
		t.Fatalf("original records not intact: %d chunks", len(got.Chunks))
	}
}

func TestAppendIsAllOrNothing(t *testing.T) {
	one := rec(strings.Repeat("a", 100))
	rb := New(one.size() * 4)
	if _, err := rb.Append(recs(3, 97)); err != nil {
		t.Fatal(err)
	}
	// Two more do not fit; neither may be stored.
	if _, err := rb.Append(recs(2, 97)); err == nil {
		t.Fatal("expected back-pressure")
	}
	if n := rb.Stats().CurrentItemCount; n != 3 {
		t.Fatalf("partial append stored: %d items", n)
	}
}

func TestTooLargeForCapacity(t *testing.T) {
	rb := New(200)
	_, err := rb.Append([]Record{rec(strings.Repeat("z", 500))})
	var tl *TooLargeError
	if !errors.As(err, &tl) {
		t.Fatalf("want *TooLargeError, got %v", err)
	}
}

func TestAckMakesSpaceAndOnlyAckedAreEvicted(t *testing.T) {
	one := rec(strings.Repeat("a", 100))
	rb := New(one.size() * 4)
	if _, err := rb.Append(recs(4, 96)); err != nil {
		t.Fatal(err)
	}
	if _, err := rb.Append(recs(1, 96)); err == nil {
		t.Fatal("expected back-pressure before ack")
	}
	n, err := rb.Ack(rb.Epoch(), 2)
	if err != nil || n != 2 {
		t.Fatalf("ack: n=%d err=%v", n, err)
	}
	// Acked records stay readable until their space is needed.
	if q := rb.Query(10, 0, 0); len(q) != 4 {
		t.Fatalf("acked records should be retained until needed, got %d", len(q))
	}
	out, err := rb.Append(recs(2, 96))
	if err != nil {
		t.Fatalf("append after ack: %v", err)
	}
	if out[0].Seq != 5 || out[1].Seq != 6 {
		t.Fatalf("seqs %d,%d", out[0].Seq, out[1].Seq)
	}
	st := rb.Stats()
	if st.EvictedAckedCount != 2 || st.PendingCount != 4 || st.DroppedPackets != 0 {
		t.Fatalf("unexpected stats %+v", st)
	}
	d := rb.Drain(0, 10)
	if len(d.Chunks) != 4 || d.Chunks[0].Seq != 3 {
		t.Fatalf("unacked records lost: %+v", d)
	}
	// Still full of unacknowledged records: back-pressure again.
	if _, err := rb.Append(recs(1, 96)); err == nil {
		t.Fatal("expected back-pressure")
	}
}

func TestDrainIsRepeatableUntilAcked(t *testing.T) {
	rb := New(1024 * 1024)
	rb.Append(recs(5, 10))

	a := rb.Drain(0, 3)
	b := rb.Drain(0, 3)
	if len(a.Chunks) != 3 || !a.More || a.LastSeq != 3 || a.Pending != 5 {
		t.Fatalf("first drain %+v", a)
	}
	if b.LastSeq != a.LastSeq {
		t.Fatal("drain without ack must return the same records")
	}
	// after_seq lets the consumer page ahead before acking.
	c := rb.Drain(3, 3)
	if len(c.Chunks) != 2 || c.Chunks[0].Seq != 4 || c.More {
		t.Fatalf("paged drain %+v", c)
	}
	if _, err := rb.Ack(a.Epoch, a.LastSeq); err != nil {
		t.Fatal(err)
	}
	d := rb.Drain(0, 10)
	if len(d.Chunks) != 2 || d.Chunks[0].Seq != 4 || d.Pending != 2 {
		t.Fatalf("drain after ack %+v", d)
	}
}

func TestAckErrors(t *testing.T) {
	rb := New(1024 * 1024)
	rb.Append(recs(3, 10))
	if _, err := rb.Ack("stale-epoch", 1); !errors.Is(err, ErrEpochMismatch) {
		t.Fatalf("want ErrEpochMismatch, got %v", err)
	}
	if _, err := rb.Ack(rb.Epoch(), 4); !errors.Is(err, ErrAckAhead) {
		t.Fatalf("want ErrAckAhead, got %v", err)
	}
	if n, _ := rb.Ack(rb.Epoch(), 3); n != 3 {
		t.Fatalf("acked %d", n)
	}
	if n, err := rb.Ack(rb.Epoch(), 2); n != 0 || err != nil {
		t.Fatalf("re-ack of older seq should be a no-op, got n=%d err=%v", n, err)
	}
}

func TestEpochDiffersPerInstance(t *testing.T) {
	if New(1024).Epoch() == New(1024).Epoch() {
		t.Fatal("epochs must differ between buffer instances")
	}
}

func TestQueryFilters(t *testing.T) {
	rb := New(1024 * 1024)
	rb.Append(recs(5, 10))
	if q := rb.Query(2, 0, 0); len(q) != 2 || q[0].Seq != 4 || q[1].Seq != 5 {
		t.Fatalf("limit query %+v", q)
	}
	if q := rb.Query(10, 0, 3); len(q) != 2 || q[0].Seq != 4 {
		t.Fatalf("since_seq query %+v", q)
	}
}

func TestConcurrentAppendDrainAck(t *testing.T) {
	rb := New(64 * 1024)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var consumed int
	wg.Add(1)
	go func() { // single consumer, as Node 2 will be
		defer wg.Done()
		for {
			d := rb.Drain(0, 50)
			if len(d.Chunks) > 0 {
				consumed += len(d.Chunks)
				if _, err := rb.Ack(d.Epoch, d.LastSeq); err != nil {
					t.Error(err)
					return
				}
				continue
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	var pwg sync.WaitGroup
	for p := 0; p < 8; p++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for i := 0; i < 200; i++ {
				for {
					_, err := rb.Append(recs(1, 50))
					if err == nil {
						break
					}
				}
			}
		}()
	}
	pwg.Wait()
	for rb.Pending() > 0 {
	}
	close(stop)
	wg.Wait()
	st := rb.Stats()
	if consumed != 1600 || st.TotalIngestedCount != 1600 || st.DroppedPackets != 0 {
		t.Fatalf("consumed=%d stats=%+v", consumed, st)
	}
}
