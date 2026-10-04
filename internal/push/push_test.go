package push

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
)

func fill(t *testing.T, n int) *buffer.RingBuffer {
	t.Helper()
	rb := buffer.New(1 << 20)
	recs := make([]buffer.Record, n)
	for i := range recs {
		recs[i] = buffer.Record{Text: fmt.Sprintf("line %d", i), Type: "log", Source: "test", Session: "s1", ScoreStatus: "no_task"}
	}
	if _, err := rb.Append(recs); err != nil {
		t.Fatal(err)
	}
	return rb
}

// start runs a Pusher against url until the returned stop function is called.
func start(rb *buffer.RingBuffer, url string, batch int, timeout time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pusher{
		URL: url, APIKey: "k", Batch: batch,
		Interval: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
		Client: &http.Client{Timeout: timeout}, Buf: rb,
		Logf: func(string, ...any) {},
	}
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	return func() { cancel(); <-done }
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A 200 acknowledges and evicts the batch; the payload carries the epoch for de-duplication.
func TestOKAcksAndEvicts(t *testing.T) {
	rb := fill(t, 10)
	var mu sync.Mutex
	var got []buffer.DrainResult
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("headers %v", r.Header)
		}
		var res buffer.DrainResult
		if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
			t.Error(err)
		}
		mu.Lock()
		got = append(got, res)
		mu.Unlock()
	}))
	defer srv.Close()

	stop := start(rb, srv.URL, 4, time.Second)
	waitFor(t, func() bool { return rb.Stats().CurrentItemCount == 0 })
	stop()

	st := rb.Stats()
	if st.PendingCount != 0 || st.EvictedAckedCount != 10 || st.AckedUpToSeq != 10 {
		t.Fatalf("stats %+v", st)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 || !got[0].More || got[2].More || got[2].LastSeq != 10 {
		t.Fatalf("batches %+v", got)
	}
	var seq uint64
	for _, b := range got {
		if b.Epoch != rb.Epoch() {
			t.Fatalf("epoch %q, want %q", b.Epoch, rb.Epoch())
		}
		for _, c := range b.Chunks {
			seq++
			if c.Seq != seq {
				t.Fatalf("chunk seq %d, want %d", c.Seq, seq)
			}
		}
	}
}

// Anything but 200 keeps every chunk, including other 2xx codes, and the same batch is retried.
func TestNon200KeepsChunks(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusNoContent, http.StatusAccepted, http.StatusBadRequest} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			rb := fill(t, 3)
			var mu sync.Mutex
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				calls++
				mu.Unlock()
				w.WriteHeader(code)
			}))
			defer srv.Close()

			stop := start(rb, srv.URL, 10, time.Second)
			waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls >= 3 })
			stop()

			if st := rb.Stats(); st.PendingCount != 3 || st.CurrentItemCount != 3 || st.AckedUpToSeq != 0 {
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

// A reply slower than the client timeout counts as a failure.
func TestTimeoutKeepsChunks(t *testing.T) {
	rb := fill(t, 3)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	stop := start(rb, srv.URL, 10, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	stop()

	if st := rb.Stats(); st.PendingCount != 3 || st.AckedUpToSeq != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// Node 2 being down, then coming back, delivers everything.
func TestRecoversAfterFailures(t *testing.T) {
	rb := fill(t, 5)
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	stop := start(rb, srv.URL, 10, time.Second)
	waitFor(t, func() bool { return rb.Stats().CurrentItemCount == 0 })
	stop()
	if st := rb.Stats(); st.EvictedAckedCount != 5 {
		t.Fatalf("stats %+v", st)
	}
}

// Chunks appended while the loop is idle are picked up.
func TestPicksUpNewChunks(t *testing.T) {
	rb := buffer.New(1 << 20)
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	stop := start(rb, srv.URL, 10, time.Second)
	defer stop()
	time.Sleep(20 * time.Millisecond)
	if _, err := rb.Append([]buffer.Record{{Text: "late", Type: "log", Source: "t", Session: "s"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return rb.Stats().EvictedAckedCount == 1 })
}
