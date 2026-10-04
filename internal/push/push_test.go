package push

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
)

func fill(t *testing.T, n int, textLen int) *buffer.RingBuffer {
	t.Helper()
	rb := buffer.New(1 << 20)
	recs := make([]buffer.Record, n)
	for i := range recs {
		recs[i] = buffer.Record{MemoryID: "m1", Text: fmt.Sprintf("%04d%s", i, strings.Repeat("x", textLen)),
			Type: "log", Source: "test", Session: "s1", ScoreStatus: "no_task"}
	}
	if _, err := rb.Append(recs); err != nil {
		t.Fatal(err)
	}
	return rb
}

// node2 is a stand-in for Node 2's POST /api/v1/working/chunks. By default it checks the
// contract (epoch, memory_id, seq > 0 and increasing) and accepts the whole batch. reply
// overrides the response for one request; returning false falls back to the default.
type node2 struct {
	t        *testing.T
	mu       sync.Mutex
	batches  []buffer.DrainResult
	sizes    []int
	at       []time.Time
	lastSeq  uint64
	reply    func(n int, b buffer.DrainResult, w http.ResponseWriter) bool
	srv      *httptest.Server
	maxBytes int
}

func newNode2(t *testing.T) *node2 {
	n := &node2{t: t, maxBytes: 8 << 20}
	n.srv = httptest.NewServer(http.HandlerFunc(n.handle))
	t.Cleanup(n.srv.Close)
	return n
}

func (n *node2) handle(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer k" || r.Header.Get("Content-Type") != "application/json" {
		n.t.Errorf("headers %v", r.Header)
	}
	raw, _ := io.ReadAll(r.Body)
	var b buffer.DrainResult
	if err := json.Unmarshal(raw, &b); err != nil || b.Epoch == "" || len(raw) > n.maxBytes {
		n.t.Errorf("bad batch (%d bytes): %v", len(raw), err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.batches = append(n.batches, b)
	n.sizes = append(n.sizes, len(raw))
	n.at = append(n.at, time.Now())
	if n.reply != nil && n.reply(len(n.batches), b, w) {
		return
	}
	accepted, dups := 0, 0
	for _, c := range b.Chunks {
		if c.MemoryID == "" || c.Seq == 0 {
			n.t.Errorf("chunk %+v breaks the contract", c)
		}
		if c.Seq <= n.lastSeq {
			dups++
			continue
		}
		n.lastSeq = c.Seq
		accepted++
	}
	json.NewEncoder(w).Encode(map[string]any{"epoch": b.Epoch, "accepted": accepted, "duplicates": dups, "accepted_up_to_seq": n.lastSeq})
}

func (n *node2) calls() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.batches)
}

type logBuf struct {
	mu    sync.Mutex
	lines []string
}

func (l *logBuf) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logBuf) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			n++
		}
	}
	return n
}

// start runs a Pusher against url until the returned stop function is called.
func start(rb *buffer.RingBuffer, url string, logs *logBuf, opts ...func(*Pusher)) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pusher{
		URL: url, APIKey: "k", Batch: 4, MaxBodyBytes: 8 << 20,
		Interval: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
		Client: &http.Client{Timeout: time.Second}, Buf: rb, Logf: logs.logf,
	}
	for _, o := range opts {
		o(p)
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

func delivered(rb *buffer.RingBuffer) func() bool {
	return func() bool { return rb.Stats().CurrentItemCount == 0 }
}

func keptAll(t *testing.T, rb *buffer.RingBuffer, n int) {
	t.Helper()
	if st := rb.Stats(); st.PendingCount != n || st.CurrentItemCount != n || st.AckedUpToSeq != 0 {
		t.Fatalf("chunks were not kept: %+v", st)
	}
}

// A 200 evicts the batch; batches arrive in seq order and carry the epoch.
func TestOKAcksAndEvicts(t *testing.T) {
	rb := fill(t, 10, 10)
	n2 := newNode2(t)
	stop := start(rb, n2.srv.URL, &logBuf{})
	waitFor(t, delivered(rb))
	stop()

	if st := rb.Stats(); st.PendingCount != 0 || st.EvictedAckedCount != 10 || st.AckedUpToSeq != 10 {
		t.Fatalf("stats %+v", st)
	}
	n2.mu.Lock()
	defer n2.mu.Unlock()
	if len(n2.batches) != 3 || !n2.batches[0].More || n2.batches[2].More {
		t.Fatalf("batches %+v", n2.batches)
	}
	var seq uint64
	for _, b := range n2.batches {
		if b.Epoch != rb.Epoch() {
			t.Fatalf("epoch %q, want %q", b.Epoch, rb.Epoch())
		}
		for _, c := range b.Chunks {
			if seq++; c.Seq != seq {
				t.Fatalf("chunk seq %d, want %d", c.Seq, seq)
			}
		}
	}
}

// Only chunks up to accepted_up_to_seq are evicted; the rest are sent again next.
func TestPartialAcceptanceEvictsOnlyAccepted(t *testing.T) {
	rb := fill(t, 4, 10)
	n2 := newNode2(t)
	n2.reply = func(n int, b buffer.DrainResult, w http.ResponseWriter) bool {
		if n != 1 {
			return false
		}
		n2.lastSeq = 2
		json.NewEncoder(w).Encode(map[string]any{"epoch": b.Epoch, "accepted": 2, "duplicates": 0, "accepted_up_to_seq": 2})
		return true
	}
	stop := start(rb, n2.srv.URL, &logBuf{})
	waitFor(t, delivered(rb))
	stop()

	n2.mu.Lock()
	defer n2.mu.Unlock()
	if len(n2.batches) != 2 || n2.batches[1].Chunks[0].Seq != 3 || n2.batches[1].LastSeq != 4 {
		t.Fatalf("batches %+v", n2.batches)
	}
}

// A 503 keeps the chunks and the retry waits for Node 2's Retry-After.
func TestServiceUnavailableHonoursRetryAfter(t *testing.T) {
	rb := fill(t, 2, 10)
	n2 := newNode2(t)
	n2.reply = func(n int, b buffer.DrainResult, w http.ResponseWriter) bool {
		if n != 1 {
			return false
		}
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	logs := &logBuf{}
	stop := start(rb, n2.srv.URL, logs)
	time.Sleep(300 * time.Millisecond)
	keptAll(t, rb, 2)
	if n2.calls() != 1 {
		t.Fatalf("retried after %d calls, before Retry-After", n2.calls())
	}
	waitFor(t, delivered(rb))
	stop()

	n2.mu.Lock()
	gap := n2.at[1].Sub(n2.at[0])
	n2.mu.Unlock()
	if gap < 900*time.Millisecond {
		t.Fatalf("retried after %v, want ~1s", gap)
	}
	if logs.count("503") != 1 || logs.count("recovered") != 1 {
		t.Fatalf("logs %q", logs.lines)
	}
}

// A 400 is a bug on Node 3's side: the chunks are kept (never dropped), Node 2's message is
// logged once, and retries wait the maximum backoff.
func TestBadRequestKeepsChunksAndLogsOnce(t *testing.T) {
	rb := fill(t, 2, 10)
	n2 := newNode2(t)
	n2.reply = func(n int, b buffer.DrainResult, w http.ResponseWriter) bool {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"error":"seq_out_of_order"}`)
		return true
	}
	logs := &logBuf{}
	stop := start(rb, n2.srv.URL, logs, func(p *Pusher) { p.MaxBackoff = 50 * time.Millisecond })
	waitFor(t, func() bool { return n2.calls() >= 3 })
	stop()

	keptAll(t, rb, 2)
	if logs.count("seq_out_of_order") != 1 || logs.count("(400)") != 1 {
		t.Fatalf("logs %q", logs.lines)
	}
	n2.mu.Lock()
	defer n2.mu.Unlock()
	if gap := n2.at[2].Sub(n2.at[1]); gap < 40*time.Millisecond {
		t.Fatalf("400 retried after %v, want the max backoff", gap)
	}
}

// Any reply that does not confirm acceptance keeps every chunk.
func TestUnconfirmedRepliesKeepChunks(t *testing.T) {
	cases := map[string]func(b buffer.DrainResult, w http.ResponseWriter){
		"500":       func(_ buffer.DrainResult, w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) },
		"204":       func(_ buffer.DrainResult, w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
		"202":       func(_ buffer.DrainResult, w http.ResponseWriter) { w.WriteHeader(http.StatusAccepted) },
		"200 empty": func(buffer.DrainResult, http.ResponseWriter) {},
		"200 no upto": func(b buffer.DrainResult, w http.ResponseWriter) {
			fmt.Fprintf(w, `{"epoch":%q,"accepted":3}`, b.Epoch)
		},
		"200 old epoch": func(_ buffer.DrainResult, w http.ResponseWriter) {
			io.WriteString(w, `{"epoch":"old","accepted_up_to_seq":3}`)
		},
		"200 below batch": func(b buffer.DrainResult, w http.ResponseWriter) {
			fmt.Fprintf(w, `{"epoch":%q,"accepted":0,"accepted_up_to_seq":0}`, b.Epoch)
		},
	}
	for name, reply := range cases {
		t.Run(name, func(t *testing.T) {
			rb := fill(t, 3, 10)
			n2 := newNode2(t)
			n2.reply = func(_ int, b buffer.DrainResult, w http.ResponseWriter) bool { reply(b, w); return true }
			stop := start(rb, n2.srv.URL, &logBuf{})
			waitFor(t, func() bool { return n2.calls() >= 3 })
			stop()
			keptAll(t, rb, 3)
		})
	}
}

// No reply within the timeout keeps the chunks; they are resent once Node 2 answers.
func TestTimeoutKeepsChunksThenResends(t *testing.T) {
	rb := fill(t, 3, 10)
	n2 := newNode2(t)
	n2.reply = func(n int, b buffer.DrainResult, w http.ResponseWriter) bool {
		if n > 1 {
			return false
		}
		time.Sleep(100 * time.Millisecond) // past the client timeout below
		return true
	}
	stop := start(rb, n2.srv.URL, &logBuf{}, func(p *Pusher) { p.Client.Timeout = 30 * time.Millisecond })
	waitFor(t, delivered(rb))
	stop()
	if n2.calls() < 2 {
		t.Fatalf("calls %d", n2.calls())
	}
}

// A batch over MaxBodyBytes is split so every request fits.
func TestLargeBatchIsSplitToFit(t *testing.T) {
	rb := fill(t, 16, 400)
	n2 := newNode2(t)
	n2.maxBytes = 2000
	stop := start(rb, n2.srv.URL, &logBuf{}, func(p *Pusher) { p.Batch = 16; p.MaxBodyBytes = 2000 })
	waitFor(t, delivered(rb))
	stop()

	n2.mu.Lock()
	defer n2.mu.Unlock()
	if len(n2.batches) < 4 {
		t.Fatalf("only %d requests for 16 chunks of ~700 bytes", len(n2.batches))
	}
	for i, s := range n2.sizes {
		if s > 2000 {
			t.Fatalf("request %d is %d bytes", i, s)
		}
	}
}

// A single chunk over the limit is never sent and never dropped.
func TestOversizedChunkIsKept(t *testing.T) {
	rb := fill(t, 1, 5000)
	n2 := newNode2(t)
	logs := &logBuf{}
	stop := start(rb, n2.srv.URL, logs, func(p *Pusher) { p.MaxBodyBytes = 4096 })
	waitFor(t, func() bool { return logs.count("too large") == 1 })
	time.Sleep(50 * time.Millisecond)
	stop()
	keptAll(t, rb, 1)
	if n2.calls() != 0 || logs.count("too large") != 1 {
		t.Fatalf("calls %d, logs %q", n2.calls(), logs.lines)
	}
}

// Chunks appended while the loop is idle are picked up.
func TestPicksUpNewChunks(t *testing.T) {
	rb := buffer.New(1 << 20)
	n2 := newNode2(t)
	stop := start(rb, n2.srv.URL, &logBuf{})
	defer stop()
	time.Sleep(20 * time.Millisecond)
	if _, err := rb.Append([]buffer.Record{{MemoryID: "m", Text: "late", Type: "log", Source: "t", Session: "s"}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return rb.Stats().EvictedAckedCount == 1 })
}

func TestRetryAfterParsing(t *testing.T) {
	for h, want := range map[string]time.Duration{
		"": 0, "5": 5 * time.Second, "0": 0, "-3": 0, "junk": 0, "99999": maxRetryAfter,
	} {
		if got := retryAfter(h); got != want {
			t.Errorf("retryAfter(%q) = %v, want %v", h, got, want)
		}
	}
	if d := retryAfter(time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat)); d < 8*time.Second || d > 10*time.Second {
		t.Errorf("HTTP-date Retry-After gave %v", d)
	}
}
