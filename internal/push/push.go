// Package push delivers buffered chunks to Node 2's working-memory endpoint
// (POST /api/v1/working/chunks). It POSTs the oldest unacknowledged chunks as one batch
// and evicts only the chunks Node 2's 200 reply covers (seq <= accepted_up_to_seq). Any
// other outcome keeps the chunks and retries, so delivery is at-least-once: Node 2 skips
// seqs it already has when a batch is resent.
package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
)

// maxRetryAfter caps a Retry-After from Node 2, so a bad header cannot stall delivery for days.
const maxRetryAfter = time.Hour

// Buffer is the part of *buffer.RingBuffer the push loop uses.
type Buffer interface {
	Drain(afterSeq uint64, max int) buffer.DrainResult
	Ack(epoch string, upTo uint64) (int, error)
	EvictAcked() int
}

// Pusher sends batches from a Buffer to one URL.
type Pusher struct {
	URL          string
	APIKey       string // sent as a bearer token when set; never logged
	Batch        int
	MaxBodyBytes int           // a batch whose JSON is larger is split
	Interval     time.Duration // wait when the buffer is empty, and the first retry delay
	MaxBackoff   time.Duration
	Client       *http.Client
	Buf          Buffer
	Logf         func(format string, args ...any)
}

// reply is Node 2's 200 body.
type reply struct {
	Epoch           string  `json:"epoch"`
	Accepted        int     `json:"accepted"`
	Duplicates      int     `json:"duplicates"`
	AcceptedUpToSeq *uint64 `json:"accepted_up_to_seq"`
}

// failure is one failed delivery attempt.
type failure struct {
	kind       string        // what went wrong; a new kind is logged, a repeat is not
	detail     string        // logged with the kind
	retryAfter time.Duration // Node 2's Retry-After, when it sent one
	senderBug  bool          // 400 or an unsendable batch: retrying soon will not help
}

func (f *failure) Error() string { return f.kind + ": " + f.detail }

// Run pushes until ctx is cancelled. A failure is logged when it starts or changes kind,
// and once when delivery recovers, not on every retry, to keep journal writes to the SD
// card low.
func (p *Pusher) Run(ctx context.Context) {
	var backoff time.Duration
	failing := ""
	for {
		res := p.Buf.Drain(0, p.Batch)
		if len(res.Chunks) == 0 {
			if !sleep(ctx, p.Interval) {
				return
			}
			continue
		}

		upTo, err := p.deliver(ctx, res)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			var f *failure
			if !errors.As(err, &f) {
				f = &failure{kind: "request failed", detail: err.Error()}
			}
			if f.kind != failing {
				p.Logf("[push] delivery to %s failing, keeping %d pending chunks and retrying: %v", p.URL, res.Pending, f)
				failing = f.kind
			}
			switch {
			case f.retryAfter > 0:
				backoff = f.retryAfter
			case f.senderBug:
				backoff = p.MaxBackoff
			case backoff == 0:
				backoff = p.Interval
			default:
				backoff = min(backoff*2, p.MaxBackoff)
			}
			if !sleep(ctx, backoff) {
				return
			}
			continue
		}
		if failing != "" {
			p.Logf("[push] delivery to %s recovered", p.URL)
			failing = ""
		}
		backoff = 0

		if _, err := p.Buf.Ack(res.Epoch, upTo); err != nil {
			// Cannot happen within one process (same epoch, issued seq); log rather than loop.
			p.Logf("[push] ack of seq %d failed: %v", upTo, err)
			if !sleep(ctx, p.Interval) {
				return
			}
			continue
		}
		p.Buf.EvictAcked()
	}
}

// deliver sends one batch, shrunk to fit MaxBodyBytes, and returns the highest seq Node 2
// accepted. It succeeds only on a 200 whose reply covers at least the batch's first chunk.
func (p *Pusher) deliver(ctx context.Context, res buffer.DrainResult) (uint64, error) {
	body, res, err := p.encode(res)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.APIKey)
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Read the body (bounded) so the connection can be reused.
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusServiceUnavailable:
		return 0, &failure{kind: "Node 2 is full or cannot commit (503)", detail: snippet(raw), retryAfter: retryAfter(resp.Header.Get("Retry-After"))}
	case resp.StatusCode == http.StatusBadRequest:
		return 0, &failure{kind: "Node 2 rejected the batch as malformed (400), a bug on Node 3's side", detail: snippet(raw), senderBug: true}
	default:
		return 0, &failure{kind: "Node 2 replied " + resp.Status, detail: snippet(raw), retryAfter: retryAfter(resp.Header.Get("Retry-After"))}
	}

	var rep reply
	if err := json.Unmarshal(raw, &rep); err != nil || rep.AcceptedUpToSeq == nil {
		return 0, &failure{kind: "Node 2 sent a 200 without accepted_up_to_seq", detail: snippet(raw)}
	}
	if rep.Epoch != res.Epoch {
		return 0, &failure{kind: "Node 2 replied for another epoch", detail: fmt.Sprintf("sent %s, got %s", res.Epoch, rep.Epoch)}
	}
	upTo := min(*rep.AcceptedUpToSeq, res.LastSeq)
	if upTo < res.Chunks[0].Seq {
		return 0, &failure{kind: "Node 2 accepted none of the batch", detail: fmt.Sprintf("accepted_up_to_seq %d, batch starts at seq %d", upTo, res.Chunks[0].Seq)}
	}
	return upTo, nil
}

// encode marshals res, halving the batch until it fits MaxBodyBytes. It returns the batch
// actually encoded, whose LastSeq is what a full acceptance covers.
func (p *Pusher) encode(res buffer.DrainResult) ([]byte, buffer.DrainResult, error) {
	for {
		body, err := json.Marshal(res)
		if err != nil {
			return nil, res, err
		}
		if p.MaxBodyBytes <= 0 || len(body) <= p.MaxBodyBytes {
			return body, res, nil
		}
		if len(res.Chunks) == 1 {
			return nil, res, &failure{kind: "a chunk is too large to send", senderBug: true,
				detail: fmt.Sprintf("seq %d encodes to %d bytes, limit %d", res.Chunks[0].Seq, len(body), p.MaxBodyBytes)}
		}
		res.Chunks = res.Chunks[:len(res.Chunks)/2]
		res.LastSeq = res.Chunks[len(res.Chunks)-1].Seq
		res.More = true
	}
}

// retryAfter parses a Retry-After header (seconds or an HTTP date); 0 means none.
func retryAfter(h string) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	var d time.Duration
	if s, err := strconv.Atoi(h); err == nil {
		d = time.Duration(s) * time.Second
	} else if t, err := http.ParseTime(h); err == nil {
		d = time.Until(t)
	}
	if d <= 0 {
		return 0
	}
	return min(d, maxRetryAfter)
}

// snippet shortens Node 2's error body for the log.
func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
