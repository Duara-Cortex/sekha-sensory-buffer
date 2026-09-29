// Package chunker splits labelled input into candidate chunks by input type:
// dialogue by turn, document by paragraph (Markdown keeps heading context) and log by line.
//
// Chunk text is never rewritten. Candidates that are blank are still emitted so that the
// floor can count them; the chunker itself never drops anything.
package chunker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/parser"
)

// Candidate is one chunk before the floor and scoring are applied.
type Candidate struct {
	Text     string
	Speaker  string
	TurnID   string
	ParentID string // ID of the unsplit unit, set only when that unit was split
	Part     int    // 1-based index within the split unit; 0 when not split
	Parts    int    // total parts of the split unit; 0 when not split
	Heading  string // Markdown heading path, e.g. "# Policy > ## Retention"
	TS       time.Time
}

// Turn is one dialogue turn as supplied by the caller.
type Turn struct {
	Speaker string
	Text    string
	TurnID  string
	TS      time.Time
}

// ID returns the stable content hash used as a chunk id.
func ID(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:16])
}

var paraSplit = regexp.MustCompile(`\r?\n[ \t]*\r?\n`)

// split bounds a unit to maxBytes. A unit within the limit is returned as one candidate;
// otherwise it is split into paragraphs, then at line/sentence/word boundaries, and every
// part carries the parent's id and its part index.
func split(base Candidate, maxBytes int) []Candidate {
	if len(base.Text) <= maxBytes {
		return []Candidate{base}
	}
	var pieces []string
	for _, p := range paraSplit.Split(base.Text, -1) {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if len(p) > maxBytes {
			pieces = append(pieces, parser.SplitAtBoundaries(p, maxBytes)...)
		} else {
			pieces = append(pieces, p)
		}
	}
	parentID := ID(base.Text)
	out := make([]Candidate, len(pieces))
	for i, p := range pieces {
		c := base
		c.Text = p
		c.ParentID = parentID
		c.Part = i + 1
		c.Parts = len(pieces)
		out[i] = c
	}
	return out
}

// Dialogue emits one candidate per turn; a turn over maxBytes becomes several paragraph
// candidates sharing the turn's turn_id and parent_id. Turns without a caller-supplied
// turn_id get "t<N>" (1-based position in the request).
func Dialogue(turns []Turn, maxBytes int) []Candidate {
	var out []Candidate
	for i, t := range turns {
		turnID := t.TurnID
		if turnID == "" {
			turnID = fmt.Sprintf("t%d", i+1)
		}
		out = append(out, split(Candidate{Text: t.Text, Speaker: t.Speaker, TurnID: turnID, TS: t.TS}, maxBytes)...)
	}
	return out
}

// Log emits one candidate per line. Indented lines (stack traces, wrapped output) stay with
// the entry above them. Blank lines are emitted as blank candidates for the floor to count.
func Log(text string, maxBytes int) []Candidate {
	var out []Candidate
	var cur []string
	flush := func() {
		if len(cur) > 0 {
			out = append(out, split(Candidate{Text: strings.Join(cur, "\n")}, maxBytes)...)
			cur = nil
		}
	}
	for _, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" {
			flush()
			out = append(out, Candidate{Text: line})
			continue
		}
		if (line[0] == ' ' || line[0] == '\t') && len(cur) > 0 {
			cur = append(cur, line)
			continue
		}
		flush()
		cur = append(cur, line)
	}
	flush()
	return out
}

// Document chunks a document by paragraph. Markdown paragraphs carry their heading path;
// txt is split on blank lines; csv, xml, json and pdf use the format parser's items
// (rows, entities, objects, text blocks) as paragraphs.
func Document(data []byte, format parser.Format, maxBytes int) ([]Candidate, error) {
	switch format {
	case parser.FormatMD:
		return markdown(string(data), maxBytes), nil
	case parser.FormatTXT, parser.FormatRaw, "":
		return paragraphs(string(data), maxBytes), nil
	}
	opts := parser.DefaultOptions()
	opts.MaxChunkBytes = 0 // size bounding is done by split, which records part lineage
	items, err := parser.Parse(format, bytes.NewReader(data), opts)
	if err != nil {
		return nil, err
	}
	var out []Candidate
	for _, it := range items {
		out = append(out, split(Candidate{Text: it}, maxBytes)...)
	}
	return out, nil
}

func paragraphs(text string, maxBytes int) []Candidate {
	var out []Candidate
	for _, p := range paraSplit.Split(text, -1) {
		p = strings.Trim(p, "\r\n")
		if strings.TrimSpace(p) == "" {
			continue // paragraph delimiter, not content
		}
		out = append(out, split(Candidate{Text: p}, maxBytes)...)
	}
	return out
}

var atxHeading = regexp.MustCompile(`^ {0,3}(#{1,6})(?:[ \t]+|$)`)

// markdown walks the document line by line. Blank lines end a paragraph except inside a
// fenced code block, so a code block with blank lines stays one chunk. ATX headings are
// emitted as their own chunk (with the enclosing heading path) and set the heading path
// for the paragraphs that follow.
func markdown(text string, maxBytes int) []Candidate {
	type level struct {
		depth int
		line  string
	}
	var stack []level
	path := func() string {
		parts := make([]string, len(stack))
		for i, l := range stack {
			parts[i] = l.line
		}
		return strings.Join(parts, " > ")
	}

	var out []Candidate
	var cur []string
	fence := ""
	flush := func() {
		if len(cur) > 0 {
			out = append(out, split(Candidate{Text: strings.Join(cur, "\n"), Heading: path()}, maxBytes)...)
			cur = nil
		}
	}

	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSuffix(line, "\r")
		trimmed := strings.TrimSpace(line)

		if fence != "" {
			cur = append(cur, line)
			if strings.HasPrefix(trimmed, fence) {
				fence = ""
			}
			continue
		}
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			fence = trimmed[:3]
			cur = append(cur, line)
			continue
		}
		if trimmed == "" {
			flush()
			continue
		}
		if m := atxHeading.FindStringSubmatch(line); m != nil {
			flush()
			depth := len(m[1])
			for len(stack) > 0 && stack[len(stack)-1].depth >= depth {
				stack = stack[:len(stack)-1]
			}
			out = append(out, split(Candidate{Text: line, Heading: path()}, maxBytes)...)
			stack = append(stack, level{depth: depth, line: trimmed})
			continue
		}
		cur = append(cur, line)
	}
	flush()
	return out
}
