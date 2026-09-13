package classifier

import (
	"fmt"
	"testing"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
)

func TestCalculateEntropy(t *testing.T) {
	// Repetitive text: low entropy
	_, normLow := CalculateEntropy("--------------------------------------------------")
	if normLow > 0.25 {
		t.Fatalf("expected low entropy for repeated dashes, got %f", normLow)
	}

	// Rich text: high entropy
	_, normHigh := CalculateEntropy("The quick brown fox jumps over the lazy dog 1234567890!")
	if normHigh < 0.70 {
		t.Fatalf("expected high entropy for rich sentence, got %f", normHigh)
	}
}

func TestScoreDistinguishesNoiseFromSignal(t *testing.T) {
	c := New(0.45)

	// Sample noise inputs
	noiseSamples := []string{
		"ping 64 bytes from 192.168.8.1: icmp_seq=1 ttl=64 time=0.4 ms",
		"[DEBUG] 2026-09-13 13:40:00 heartbeat status=ok",
		"........................................................",
		"GET /healthz 200 OK 127.0.0.1 - 0.2ms",
		"--- --- --- --- --- --- ---",
		"ok ok ok ok ok ok ok ok ok ok",
	}

	for _, noise := range noiseSamples {
		m := c.Score(noise, "", 0.45)
		if m.IsSalient {
			t.Errorf("expected noise to be discarded, but got salient: true (score=%f) for: %s", m.SalienceScore, noise)
		}
	}

	// Sample signal inputs
	signalSamples := []string{
		"African Fractals represent a sophisticated mathematical paradigm observed in traditional African architecture and social systems.",
		"CRITICAL ALERT: Node 2 RAM usage exceeded 90% (14.5GB/16GB). SLM inference throttling activated.",
		"Logone-Birni Courtyard Recursion: Courtyards are nested inside other courtyards in a recursive layout where scaling factor alpha=0.33.",
		"Fatal error: kernel panic - unable to handle kernel paging request at virtual address 0000000000000010.",
	}

	for _, sig := range signalSamples {
		m := c.Score(sig, "", 0.45)
		if !m.IsSalient {
			t.Errorf("expected signal to be kept, but got salient: false (score=%f, reason=%s) for: %s", m.SalienceScore, m.DiscardReason, sig)
		}
	}
}

func TestTaskDirectiveAlignment(t *testing.T) {
	c := New(0.45)
	task := "investigate memory leaks and thermal throttling"

	relevantText := "Host sekha-node2 reported thermal throttling at 82C with memory exhaustion in working memory scratchpad."
	irrelevantText := "User changed UI theme to dark mode in navigation sidebar preferences."

	mRel := c.Score(relevantText, task, 0.45)
	mIrrel := c.Score(irrelevantText, task, 0.45)

	if mRel.TaskRelevance <= mIrrel.TaskRelevance {
		t.Fatalf("expected relevant text to have higher task relevance: rel=%f, irrel=%f", mRel.TaskRelevance, mIrrel.TaskRelevance)
	}
	if mRel.SalienceScore <= mIrrel.SalienceScore {
		t.Fatalf("expected relevant text to have higher salience: rel=%f, irrel=%f", mRel.SalienceScore, mIrrel.SalienceScore)
	}
}

func TestNoiseReductionRatio(t *testing.T) {
	c := New(0.45)

	// Feed 70 noise chunks and 30 signal chunks (total 100)
	var chunks []buffer.Chunk
	seq := uint64(1)

	// 70 noise items
	for i := 0; i < 70; i++ {
		chunks = append(chunks, buffer.Chunk{
			Seq:       seq,
			Timestamp: time.Now().UTC(),
			Origin:    "syslog",
			Data:      fmt.Sprintf("[DEBUG] heartbeat probe ping seq=%d status=ok", i),
		})
		seq++
	}

	// 30 signal items
	for i := 0; i < 30; i++ {
		chunks = append(chunks, buffer.Chunk{
			Seq:       seq,
			Timestamp: time.Now().UTC(),
			Origin:    "agent",
			Data:      fmt.Sprintf("Instruction step %d: Extract entities from section %d and update long-term relational graph on node 1.", i, i),
		})
		seq++
	}

	result := c.FilterChunks(chunks, "update long-term relational graph", 0.45, true)

	if result.TotalEvaluated != 100 {
		t.Fatalf("expected 100 evaluated, got %d", result.TotalEvaluated)
	}

	// Noise reduction target is > 60%
	if result.NoiseReductionRatio < 0.60 {
		t.Fatalf("expected noise reduction ratio >= 0.60, got %f (salient=%d, discarded=%d)",
			result.NoiseReductionRatio, result.SalientCount, result.DiscardedCount)
	}

	// Actionable instruction retention target is >= 90%
	if result.SalientCount < 27 { // 27/30 is 90%
		t.Fatalf("expected at least 27/30 signal retained, got %d", result.SalientCount)
	}
}

func TestClassifierLatencyPerChunk(t *testing.T) {
	c := New(0.45)
	sample := "African Fractals represent a sophisticated mathematical paradigm observed in traditional African architecture."

	start := time.Now()
	iterations := 1000
	for i := 0; i < iterations; i++ {
		_ = c.Score(sample, "mathematical architecture", 0.45)
	}
	elapsed := time.Since(start)
	perChunkLatency := elapsed / time.Duration(iterations)

	// Target is < 10ms per chunk. Typically pure Go should be < 0.1ms (100 microseconds)!
	if perChunkLatency > 10*time.Millisecond {
		t.Fatalf("latency too high: %v per chunk (target < 10ms)", perChunkLatency)
	}
}
