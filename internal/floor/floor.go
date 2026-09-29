// Package floor drops chunks that carry no information. It is rule-based and never looks
// at a score: only blank chunks, exact repeats within one input, separator lines and
// heartbeat/ping lines are dropped. Everything else is kept.
package floor

import (
	"regexp"
	"strings"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/chunker"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/config"
)

// Discard reasons, in the order the rules are checked.
const (
	ReasonBlank     = "blank"
	ReasonSeparator = "separator"
	ReasonHeartbeat = "heartbeat"
	ReasonDuplicate = "duplicate"
)

// Reasons lists every discard reason; counts always include all of them.
var Reasons = []string{ReasonBlank, ReasonSeparator, ReasonHeartbeat, ReasonDuplicate}

// Rules configures the floor.
type Rules struct {
	Separator        *regexp.Regexp
	Heartbeat        *regexp.Regexp
	HeartbeatExclude *regexp.Regexp // a heartbeat match that also matches this is kept
	HeartbeatTypes   map[string]bool
	DedupTypes       map[string]bool
}

// Apply returns the kept candidates (order preserved) and discard counts by reason.
// Duplicate detection is exact text match within this call; for dialogue the speaker is
// part of the key, so the same words from two different speakers are both kept.
func (r Rules) Apply(inputType string, cands []chunker.Candidate) ([]chunker.Candidate, map[string]int) {
	counts := make(map[string]int, len(Reasons))
	for _, reason := range Reasons {
		counts[reason] = 0
	}
	checkHeartbeat := r.HeartbeatTypes[inputType]
	dedup := r.DedupTypes[inputType]
	seen := make(map[string]struct{})

	kept := make([]chunker.Candidate, 0, len(cands))
	for _, c := range cands {
		if strings.TrimSpace(c.Text) == "" {
			counts[ReasonBlank]++
			continue
		}
		if r.Separator != nil && r.Separator.MatchString(c.Text) {
			counts[ReasonSeparator]++
			continue
		}
		if checkHeartbeat && r.isHeartbeat(c.Text) {
			counts[ReasonHeartbeat]++
			continue
		}
		if dedup {
			key := c.Text
			if inputType == config.TypeDialogue {
				key = c.Speaker + "\x00" + c.Text
			}
			if _, dup := seen[key]; dup {
				counts[ReasonDuplicate]++
				continue
			}
			seen[key] = struct{}{}
		}
		kept = append(kept, c)
	}
	return kept, counts
}

// isHeartbeat checks each line, so a multi-line entry is a heartbeat only if every line is.
func (r Rules) isHeartbeat(text string) bool {
	if r.Heartbeat == nil {
		return false
	}
	for _, line := range strings.Split(text, "\n") {
		if !r.Heartbeat.MatchString(line) {
			return false
		}
		if r.HeartbeatExclude != nil && r.HeartbeatExclude.MatchString(line) {
			return false
		}
	}
	return true
}
