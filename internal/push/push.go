// Package push delivers buffered chunks to Node 2. It POSTs the oldest unacknowledged
// chunks as one batch and, only when Node 2 replies 200, acknowledges and evicts them.
// Any other outcome keeps the chunks and retries with exponential backoff, so delivery
// is at-least-once: a 200 that is lost on the way back means the batch is sent again.
package push

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
)

// Buffer is the part of *buffer.RingBuffer the push loop uses.
type Buffer interface {
	Drain(afterSeq uint64, max int) buffer.DrainResult
	Ack(epoch string, upTo uint64) (int, error)
	EvictAcked() int
}

// Pusher sends batches from a Buffer to one URL.
type Pusher struct {
	URL        string
	APIKey     string // sent as a bearer token when set; never logged
	Batch      int
	Interval   time.Duration // wait when the buffer is empty, and the first retry delay
	MaxBackoff time.Duration
	Client     *http.Client
	Buf        Buffer
	Logf       func(format string, args ...any)
}

// Run pushes until ctx is cancelled. Failures are logged once when they start and once
// when delivery recovers, not on every retry, to keep journal writes to the SD card low.
func (p *Pusher) Run(ctx context.Context) {
	var backoff time.Duration
	for {
		res := p.Buf.Drain(0, p.Batch)
		if len(res.Chunks) == 0 {
			if !sleep(ctx, p.Interval) {
				return
			}
			continue
		}

		if err := p.send(ctx, res); err != nil {
			if ctx.Err() != nil {
				return
			}
			if backoff == 0 {
				p.Logf("[push] delivery to %s failing, keeping %d pending chunks and retrying: %v", p.URL, res.Pending, err)
				backoff = p.Interval
			} else {
				backoff = min(backoff*2, p.MaxBackoff)
			}
			if !sleep(ctx, backoff) {
				return
			}
			continue
		}
		if backoff != 0 {
			p.Logf("[push] delivery to %s recovered", p.URL)
			backoff = 0
		}

		if _, err := p.Buf.Ack(res.Epoch, res.LastSeq); err != nil {
			// Cannot happen within one process (same epoch, issued seq); log rather than loop.
			p.Logf("[push] ack of seq %d failed: %v", res.LastSeq, err)
			if !sleep(ctx, p.Interval) {
				return
			}
			continue
		}
		p.Buf.EvictAcked()
	}
}

// send POSTs one batch and succeeds only on HTTP 200.
func (p *Pusher) send(ctx context.Context, res buffer.DrainResult) error {
	body, err := json.Marshal(res)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.APIKey)
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return err
	}
	// Read the body so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Node 2 replied %s", resp.Status)
	}
	return nil
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
