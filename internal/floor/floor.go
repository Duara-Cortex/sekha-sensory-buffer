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
// Duplicate detection is exact text match within this call. For dialogue the key is
// speaker + the question being answered + text: a repeated "yes" is one memory only when it
// answers the same question; a "yes" to a different question, or from another speaker, is kept.
func (r Rules) Apply(inputType string, cands []chunker.Candidate) ([]chunker.Candidate, map[string]int) {
	counts := make(map[string]int, len(Reasons))
	for _, reason := range Reasons {
		counts[reason] = 0
	}
	checkHeartbeat := r.HeartbeatTypes[inputType]
	dedup := r.DedupTypes[inputType]
	seen := make(map[string]struct{})
	var questions []string
	if dedup && inputType == config.TypeDialogue {
		questions = questionContexts(cands)
	}

	kept := make([]chunker.Candidate, 0, len(cands))
	for i, c := range cands {
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
				key = c.Speaker + "\x00" + questions[i] + "\x00" + c.Text
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

// questionContexts returns, for each dialogue candidate, the question its turn answers:
// the content id of the most recent earlier non-blank turn by a different speaker, or ""
// when there is none. Candidates of one turn are consecutive and share a turn_id. The
// question is identified by its text, not its position, so the same question asked twice
// gives the same context.
func questionContexts(cands []chunker.Candidate) []string {
	type turn struct{ speaker, id string }
	var history []turn
	ctx := make([]string, len(cands))
	for i := 0; i < len(cands); {
		j := i
		for j < len(cands) && cands[j].TurnID == cands[i].TurnID {
			j++
		}
		speaker := cands[i].Speaker
		question := ""
		for k := len(history) - 1; k >= 0; k-- {
			if history[k].speaker != speaker {
				question = history[k].id
				break
			}
		}
		for k := i; k < j; k++ {
			ctx[k] = question
		}
		if strings.TrimSpace(cands[i].Text) != "" {
			id := cands[i].ParentID // a split turn is identified by its unsplit text
			if id == "" {
				id = chunker.ID(cands[i].Text)
			}
			history = append(history, turn{speaker: speaker, id: id})
		}
		i = j
	}
	return ctx
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
