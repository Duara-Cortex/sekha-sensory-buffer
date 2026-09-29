package floor

import (
	"strings"
	"testing"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/chunker"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/config"
)

func defaultRules() Rules {
	c := config.Defaults()
	return Rules{Separator: c.SeparatorPattern, Heartbeat: c.HeartbeatPattern, HeartbeatExclude: c.HeartbeatExclude,
		HeartbeatTypes: c.HeartbeatTypes, DedupTypes: c.DedupTypes}
}

// Acceptance 2: planted blanks, separators, duplicates and pings are dropped with counts;
// everything else is kept, in order.
func TestFloorDropsOnlyPlantedNoise(t *testing.T) {
	log := strings.Join([]string{
		"2026-09-29T10:00:00Z INFO service started",
		"",
		"-----",
		"64 bytes from 192.168.8.1: icmp_seq=1 ttl=64 time=0.4 ms",
		"2026-09-29T10:00:01Z WARN disk at 91%",
		"   ",
		"==========",
		"heartbeat seq=12 status=ok",
		"2026-09-29T10:00:01Z WARN disk at 91%",
		"PING 192.168.8.1 (192.168.8.1) 56(84) bytes of data.",
		"ERROR heartbeat missed from node2 for 30s",
		"From 192.168.8.9 icmp_seq=3 Destination Host Unreachable",
		"heartbeat failed: status not ok",
		"retention period is 90 days",
		"****",
		"pong",
	}, "\n")
	kept, counts := defaultRules().Apply(config.TypeLog, chunker.Log(log, 1024))

	wantKept := []string{
		"2026-09-29T10:00:00Z INFO service started",
		"2026-09-29T10:00:01Z WARN disk at 91%",
		"ERROR heartbeat missed from node2 for 30s",
		"From 192.168.8.9 icmp_seq=3 Destination Host Unreachable",
		"heartbeat failed: status not ok",
		"retention period is 90 days",
	}
	if len(kept) != len(wantKept) {
		t.Fatalf("kept %d: %+v", len(kept), kept)
	}
	for i, w := range wantKept {
		if kept[i].Text != w {
			t.Fatalf("kept[%d] = %q, want %q", i, kept[i].Text, w)
		}
	}
	want := map[string]int{ReasonBlank: 2, ReasonSeparator: 3, ReasonHeartbeat: 4, ReasonDuplicate: 1}
	for reason, n := range want {
		if counts[reason] != n {
			t.Fatalf("%s = %d, want %d (all: %v)", reason, counts[reason], n, counts)
		}
	}
}

func TestHeartbeatRuleOnlyForConfiguredTypes(t *testing.T) {
	cands := []chunker.Candidate{{Text: "ping", Speaker: "user"}, {Text: "heartbeat ok", Speaker: "user"}}
	kept, counts := defaultRules().Apply(config.TypeDialogue, cands)
	if len(kept) != 2 || counts[ReasonHeartbeat] != 0 {
		t.Fatalf("dialogue must not lose 'ping' messages: kept=%d counts=%v", len(kept), counts)
	}
}

func dialogue(turns ...[2]string) []chunker.Candidate {
	var ts []chunker.Turn
	for _, t := range turns {
		ts = append(ts, chunker.Turn{Speaker: t[0], Text: t[1]})
	}
	return chunker.Dialogue(ts, 1024)
}

func texts(cs []chunker.Candidate) string {
	var out []string
	for _, c := range cs {
		out = append(out, c.TurnID+":"+c.Text)
	}
	return strings.Join(out, " | ")
}

func TestDialogueYesToDifferentQuestionsIsKept(t *testing.T) {
	kept, counts := defaultRules().Apply(config.TypeDialogue, dialogue(
		[2]string{"assistant", "Shall I delete the logs?"},
		[2]string{"user", "yes"},
		[2]string{"assistant", "Shall I restart the service?"},
		[2]string{"user", "yes"},
	))
	if len(kept) != 4 || counts[ReasonDuplicate] != 0 {
		t.Fatalf("a yes to a different question must be kept: %s (%v)", texts(kept), counts)
	}
}

func TestDialogueYesToSameQuestionIsOne(t *testing.T) {
	kept, counts := defaultRules().Apply(config.TypeDialogue, dialogue(
		[2]string{"assistant", "Are you sure?"},
		[2]string{"user", "yes"},
		[2]string{"assistant", "Are you sure?"},
		[2]string{"user", "yes"},
	))
	// Asking again after a "yes" is kept (it follows a different turn); the second "yes"
	// answers the same question text, so it is the one duplicate.
	if texts(kept) != "t1:Are you sure? | t2:yes | t3:Are you sure?" || counts[ReasonDuplicate] != 1 {
		t.Fatalf("got %s (%v)", texts(kept), counts)
	}
}

func TestDialogueDedupIsPerSpeaker(t *testing.T) {
	kept, counts := defaultRules().Apply(config.TypeDialogue, dialogue(
		[2]string{"user", "Ready?"},
		[2]string{"assistant", "yes"},
		[2]string{"user", "yes"},
	))
	if len(kept) != 3 || counts[ReasonDuplicate] != 0 {
		t.Fatalf("same words from different speakers must both be kept: %s (%v)", texts(kept), counts)
	}
}

func TestDialogueQuestionSkipsBlankTurnsAndSameSpeaker(t *testing.T) {
	kept, counts := defaultRules().Apply(config.TypeDialogue, dialogue(
		[2]string{"assistant", "Proceed?"},
		[2]string{"user", "yes"},
		[2]string{"assistant", "   "}, // blank: not a question
		[2]string{"user", "hold on"},  // same speaker: not a question for the next turn
		[2]string{"user", "yes"},      // still answering "Proceed?" -> duplicate
	))
	if texts(kept) != "t1:Proceed? | t2:yes | t4:hold on" || counts[ReasonDuplicate] != 1 || counts[ReasonBlank] != 1 {
		t.Fatalf("got %s (%v)", texts(kept), counts)
	}
}

func TestDedupCanBeDisabledPerType(t *testing.T) {
	r := defaultRules()
	r.DedupTypes = map[string]bool{}
	kept, _ := r.Apply(config.TypeLog, []chunker.Candidate{{Text: "a"}, {Text: "a"}})
	if len(kept) != 2 {
		t.Fatal("dedup should be off")
	}
}

func TestNothingElseIsDropped(t *testing.T) {
	// Low-information but real content must survive the floor.
	cands := []chunker.Candidate{{Text: "ok"}, {Text: "-"}, {Text: "--"}, {Text: "# Title"}, {Text: "1"}, {Text: "Title\n====="}}
	kept, _ := defaultRules().Apply(config.TypeDocument, cands)
	if len(kept) != len(cands) {
		t.Fatalf("kept %d of %d: %+v", len(kept), len(cands), kept)
	}
}

func TestCountsAlwaysIncludeEveryReason(t *testing.T) {
	_, counts := defaultRules().Apply(config.TypeLog, nil)
	for _, r := range Reasons {
		if _, ok := counts[r]; !ok {
			t.Fatalf("missing reason %s", r)
		}
	}
}
