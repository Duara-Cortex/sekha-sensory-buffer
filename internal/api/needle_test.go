package api

import (
	"context"
	"math"
	"strings"
	"testing"
)

// BEAM 100k SWE needle (haystack 100k_001, global turns 181-182, probe probe_ie_0112).
// The old heuristic filter dropped it at θ 0.75.
const (
	needleHash  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b78520457"
	taskGeneric = "Remember this conversation for later questions"
	taskProbe   = "What is the exact verbatim configured value of BUILD_ARTIFACT_HASH for Project proj-1d9c10?"
)

// measuredEmbedder reproduces the scores MiniLM gave on Node 3 (live test 3, 2026-09-29):
// needle vs probe 0.82, needle vs generic 0.03, filler medians 0.14 (probe) and 0.05 (generic).
type measuredEmbedder struct{}

func (measuredEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		switch {
		case t == taskProbe:
			out[i] = []float32{1, 0, 0}
		case t == taskGeneric:
			out[i] = []float32{0, 0, 1}
		case strings.Contains(t, "BUILD_ARTIFACT_HASH"):
			out[i] = []float32{0.82, 0.57, 0.026}
		default:
			out[i] = []float32{0.14, 0.985, 0.05}
		}
	}
	return out, nil
}

// Acceptance 3 (offline): the needle is stored and delivered whatever it scores.
func TestBenchmarkNeedleIsNeverDropped(t *testing.T) {
	turns := []map[string]string{
		{"speaker": "user", "text": "The CI pipeline is failing on the lint step, can you check?"},
		{"speaker": "assistant", "text": "The linter flags an unused import in pkg/cache/store.go; I removed it."},
		{"speaker": "user", "text": "Project proj-1d9c10 setup parameter: BUILD_ARTIFACT_HASH = '" + needleHash + "'."},
		{"speaker": "assistant", "text": "Acknowledged. Set BUILD_ARTIFACT_HASH='" + needleHash + "' for Project proj-1d9c10."},
		{"speaker": "user", "text": "Let's add a unit test for the pagination helper."},
		{"speaker": "assistant", "text": "Added TestPaginate covering empty input and a partial last page."},
	}
	cases := []struct {
		task       string
		wantScore  float64
		wantStrong bool
	}{
		{taskGeneric, 0.026, false}, // realistic ingest: weak, but stored
		{taskProbe, 0.82, true},     // the later question: strong
	}
	for _, tc := range cases {
		srv := newTestServer(1<<20, measuredEmbedder{})
		resp := decodeIngest(t, ingestJSON(t, srv, map[string]any{
			"type": "dialogue", "source": "beam_100k_swe", "session": "s00041", "task": tc.task, "turns": turns,
		}))
		if resp.Accepted != len(turns) {
			t.Fatalf("%q: accepted %d of %d turns", tc.task, resp.Accepted, len(turns))
		}
		for _, c := range resp.Chunks {
			isNeedle := strings.Contains(c.Text, "BUILD_ARTIFACT_HASH")
			if !isNeedle {
				if c.Strong {
					t.Fatalf("%q: filler marked strong: %+v", tc.task, c)
				}
				continue
			}
			if !strings.Contains(c.Text, needleHash) {
				t.Fatalf("hash not verbatim: %q", c.Text)
			}
			if c.TaskScore == nil || math.Abs(*c.TaskScore-tc.wantScore) > 0.01 || c.Strong != tc.wantStrong {
				t.Fatalf("%q: needle score=%v strong=%v, want ~%.3f strong=%v", tc.task, c.TaskScore, c.Strong, tc.wantScore, tc.wantStrong)
			}
		}
		drained := 0
		for _, c := range srv.RingBuffer.Drain(0, 100).Chunks {
			if strings.Contains(c.Text, needleHash) {
				drained++
			}
		}
		if drained != 2 {
			t.Fatalf("%q: %d needle turns in /drain, want 2", tc.task, drained)
		}
	}
}
