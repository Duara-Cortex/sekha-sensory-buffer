package chunker

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/parser"
)

// longTurn builds a turn of paras paragraphs of wordsPer words each.
func longTurn(paras, wordsPer int) string {
	ps := make([]string, paras)
	for p := range ps {
		words := make([]string, wordsPer)
		for w := range words {
			words[w] = fmt.Sprintf("w%d_%d", p, w)
		}
		ps[p] = strings.Join(words, " ") + "."
	}
	return strings.Join(ps, "\n\n")
}

func TestDialogueOneChunkPerTurnAndSplitsLongTurn(t *testing.T) {
	long := longTurn(30, 100) // 3,000 words
	var turns []Turn
	for i := 0; i < 20; i++ {
		sp := "user"
		if i%2 == 1 {
			sp = "assistant"
		}
		text := fmt.Sprintf("turn %d says something short", i+1)
		if i == 7 {
			text = long
		}
		turns = append(turns, Turn{Speaker: sp, Text: text})
	}
	cands := Dialogue(turns, 1024)

	var whole, parts []Candidate
	for _, c := range cands {
		if c.ParentID == "" {
			whole = append(whole, c)
		} else {
			parts = append(parts, c)
		}
	}
	if len(whole) != 19 {
		t.Fatalf("want 19 whole-turn chunks, got %d", len(whole))
	}
	if len(parts) != 30 {
		t.Fatalf("want 30 paragraph chunks for the long turn, got %d", len(parts))
	}
	for i, p := range parts {
		if p.TurnID != "t8" || p.Speaker != "assistant" || p.ParentID != ID(long) || p.Part != i+1 || p.Parts != 30 {
			t.Fatalf("part %d has wrong lineage: %+v", i, p)
		}
		if !strings.Contains(long, p.Text) {
			t.Fatalf("part %d text is not verbatim from the turn", i)
		}
	}
	for _, w := range whole {
		if w.Part != 0 || w.Parts != 0 || w.TurnID == "" {
			t.Fatalf("whole turn has split metadata: %+v", w)
		}
	}
	if whole[0].TurnID != "t1" || whole[0].Speaker != "user" || whole[7].TurnID != "t9" {
		t.Fatalf("turn ids/speakers wrong: %+v / %+v", whole[0], whole[7])
	}
}

func TestDialogueKeepsCallerTurnIDsAndOversizeParagraphs(t *testing.T) {
	huge := strings.Repeat("A sentence that keeps going. ", 100) // one ~2.9KB paragraph
	cands := Dialogue([]Turn{{Speaker: "user", Text: huge, TurnID: "abc"}}, 1024)
	if len(cands) < 3 {
		t.Fatalf("oversize paragraph not split: %d", len(cands))
	}
	for _, c := range cands {
		if c.TurnID != "abc" || len(c.Text) > 1024 || c.Parts != len(cands) {
			t.Fatalf("bad part %+v (len %d)", c, len(c.Text))
		}
	}
}

func TestDialogueTextIsVerbatim(t *testing.T) {
	text := "  Hello,\nworld!  "
	cands := Dialogue([]Turn{{Speaker: "u", Text: text}}, 1024)
	if len(cands) != 1 || cands[0].Text != text {
		t.Fatalf("turn text rewritten: %q", cands[0].Text)
	}
}

func TestLogChunksByLineKeepingContinuationsAndBlanks(t *testing.T) {
	in := "2026-09-29 INFO start\n\nERROR boom\n    at foo()\n\tat bar()\nINFO done\r\n"
	cands := Log(in, 1024)
	want := []string{"2026-09-29 INFO start", "", "ERROR boom\n    at foo()\n\tat bar()", "INFO done"}
	if len(cands) != len(want) {
		t.Fatalf("got %d chunks: %+v", len(cands), cands)
	}
	for i, w := range want {
		if cands[i].Text != w {
			t.Fatalf("chunk %d = %q, want %q", i, cands[i].Text, w)
		}
	}
}

func TestLogSplitsOverlongLine(t *testing.T) {
	line := strings.Repeat("k=v ", 600)
	cands := Log(line, 1024)
	if len(cands) < 2 || cands[0].ParentID == "" || cands[0].Parts != len(cands) {
		t.Fatalf("overlong line not split with lineage: %+v", cands[0])
	}
}

func TestDocumentTextByParagraph(t *testing.T) {
	cands, err := Document([]byte("First para\nline two.\n\n\n\nSecond para.\n"), parser.FormatTXT, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 2 || cands[0].Text != "First para\nline two." || cands[1].Text != "Second para." {
		t.Fatalf("got %+v", cands)
	}
}

func TestDocumentMarkdownKeepsHeadingContext(t *testing.T) {
	md := "# Policy\n\nIntro text.\n\n## Retention\n\nLogs are kept for 90 days.\n\n```\ncode\n\nstill code\n```\n\n### Detail\n\nDeep.\n\n## Access\n\nAdmins only.\n"
	cands, err := Document([]byte(md), parser.FormatMD, 1024)
	if err != nil {
		t.Fatal(err)
	}
	type want struct{ text, heading string }
	wants := []want{
		{"# Policy", ""},
		{"Intro text.", "# Policy"},
		{"## Retention", "# Policy"},
		{"Logs are kept for 90 days.", "# Policy > ## Retention"},
		{"```\ncode\n\nstill code\n```", "# Policy > ## Retention"},
		{"### Detail", "# Policy > ## Retention"},
		{"Deep.", "# Policy > ## Retention > ### Detail"},
		{"## Access", "# Policy"},
		{"Admins only.", "# Policy > ## Access"},
	}
	if len(cands) != len(wants) {
		t.Fatalf("got %d chunks: %+v", len(cands), cands)
	}
	for i, w := range wants {
		if cands[i].Text != w.text || cands[i].Heading != w.heading {
			t.Fatalf("chunk %d = %q [%s], want %q [%s]", i, cands[i].Text, cands[i].Heading, w.text, w.heading)
		}
	}
}

func TestDocumentStructuredFormatsUseParserItems(t *testing.T) {
	cands, err := Document([]byte("host,status\nnode3,online\nnode2,online\n"), parser.FormatCSV, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 2 || !strings.Contains(cands[0].Text, "node3") {
		t.Fatalf("csv rows: %+v", cands)
	}
}

func TestIDIsStable(t *testing.T) {
	if ID("abc") != ID("abc") || ID("abc") == ID("abd") || len(ID("x")) != 32 {
		t.Fatal("ID must be a stable 32-hex content hash")
	}
}
