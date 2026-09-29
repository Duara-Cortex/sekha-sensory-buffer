package api

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
)

// Fixture from the BEAM 100k SWE benchmark (haystack 100k_001, session s00041, global
// turns 181-182, probe probe_ie_0112). The old heuristic filter scored this answer
// 0.55-0.57 and dropped it at θ 0.75; the new path must store it whatever it scores.
const (
	needleHash      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b78520457"
	needleUser      = "Project proj-1d9c10 setup parameter: BUILD_ARTIFACT_HASH = '" + needleHash + "'."
	needleAssistant = "Acknowledged. Set BUILD_ARTIFACT_HASH='" + needleHash + "' for Project proj-1d9c10."
	taskGeneric     = "Remember this conversation for later questions"
	taskProbe       = "What is the exact verbatim configured value of BUILD_ARTIFACT_HASH for Project proj-1d9c10?"
)

// needleEmbedder stands in for MiniLM: the needle topic is dimension 1, everything else
// dimension 0. The generic task sits on the filler topic, so the needle scores low
// against it (weak) and high against the probe (strong).
type needleEmbedder struct{}

func (needleEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		if strings.Contains(t, "BUILD_ARTIFACT_HASH") {
			out[i] = []float32{0.1, 1}
		} else {
			out[i] = []float32{1, 0}
		}
	}
	return out, nil
}

func needleDialogue(task string) map[string]any {
	filler := []string{
		"Can you refactor the retry loop in the HTTP client to use exponential backoff?",
		"Sure. I replaced the fixed sleep with a jittered exponential backoff capped at 30 seconds.",
		"The CI pipeline is failing on the lint step, can you check?",
		"The linter flags an unused import in pkg/cache/store.go; I removed it and the step passes.",
		"Let's add a unit test for the pagination helper.",
		"Added TestPaginate covering empty input, a partial last page and an out-of-range page.",
	}
	var turns []map[string]string
	add := func(speaker, text string) {
		turns = append(turns, map[string]string{"speaker": speaker, "text": text, "turn_id": fmt.Sprintf("g%d", len(turns)+1)})
	}
	for i := 0; i < 30; i++ { // filler before the needle
		add([]string{"user", "assistant"}[i%2], filler[i%len(filler)]+fmt.Sprintf(" (step %d)", i))
	}
	add("user", needleUser)
	add("assistant", needleAssistant)
	for i := 30; i < 60; i++ { // filler after the needle
		add([]string{"user", "assistant"}[i%2], filler[i%len(filler)]+fmt.Sprintf(" (step %d)", i))
	}
	body := map[string]any{"type": "dialogue", "source": "beam_100k_swe", "session": "s00041", "turns": turns}
	if task != "" {
		body["task"] = task
	}
	return body
}

func needleChunks(recs []buffer.Record) []buffer.Record {
	var out []buffer.Record
	for _, r := range recs {
		if strings.Contains(r.Text, "BUILD_ARTIFACT_HASH") {
			out = append(out, r)
		}
	}
	return out
}

// Acceptance 3 (offline, real benchmark needle): stored and delivered under any task.
func TestBenchmarkNeedleIsNeverDropped(t *testing.T) {
	cases := []struct {
		name       string
		task       string
		wantStrong bool
		wantScored bool
	}{
		{"generic ingestion task: weak but stored", taskGeneric, false, true},
		{"probe question: strong", taskProbe, true, true},
		{"no task: unscored but stored", "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(1<<20, needleEmbedder{})
			resp := decodeIngest(t, ingestJSON(t, srv, needleDialogue(tc.task)))

			if resp.Accepted != 62 || resp.Discarded["duplicate"] != 0 {
				t.Fatalf("every turn must be kept: accepted=%d discarded=%v", resp.Accepted, resp.Discarded)
			}
			got := needleChunks(resp.Chunks)
			if len(got) != 2 {
				t.Fatalf("want both needle turns in the response, got %d", len(got))
			}
			wantText := map[string]string{"g31": needleUser, "g32": needleAssistant}
			wantSpeaker := map[string]string{"g31": "user", "g32": "assistant"}
			for _, c := range got {
				if c.Text != wantText[c.TurnID] || c.Speaker != wantSpeaker[c.TurnID] || c.Session != "s00041" {
					t.Fatalf("needle turn altered or mislabelled: %+v", c)
				}
				if !strings.Contains(c.Text, needleHash) {
					t.Fatal("the 64-hex value must survive verbatim")
				}
				if (c.TaskScore != nil) != tc.wantScored || c.Strong != tc.wantStrong {
					t.Fatalf("routing: score=%v strong=%v, want scored=%v strong=%v", c.TaskScore, c.Strong, tc.wantScored, tc.wantStrong)
				}
			}

			// Delivered to Node 2 regardless of routing.
			d := srv.RingBuffer.Drain(0, 1000)
			if len(needleChunks(d.Chunks)) != 2 {
				t.Fatalf("needle turns missing from /drain")
			}
		})
	}
}
