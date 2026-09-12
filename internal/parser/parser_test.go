package parser

import (
	"bytes"
	"compress/zlib"
	"strings"
	"testing"
)

func TestParseTXT(t *testing.T) {
	content := "Line 1\n\nLine 2 paragraph\nMore text\n\nLine 3"
	paras, err := ParseTXT([]byte(content), Options{ChunkBy: "para"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paras) != 3 {
		t.Fatalf("expected 3 paragraphs, got %d: %+v", len(paras), paras)
	}

	lines, err := ParseTXT([]byte(content), Options{ChunkBy: "line"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lines) != 4 {
		t.Fatalf("expected 4 non-empty lines, got %d: %+v", len(lines), lines)
	}
}

func TestParseMarkdown(t *testing.T) {
	md := `# Task 03: Implement Sensory Buffer
Some intro details.

## Requirements
- [x] Requirement 1
- [ ] Requirement 2

## Expected Output
Running daemon on port 8081.`

	sections, err := ParseMarkdown([]byte(md), Options{ChunkBy: "section"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sections) != 3 {
		t.Fatalf("expected 3 sections, got %d: %+v", len(sections), sections)
	}
	if !strings.HasPrefix(sections[0], "# Task 03") {
		t.Fatalf("expected section 0 to start with # Task 03, got: %s", sections[0])
	}
	if !strings.HasPrefix(sections[1], "## Requirements") {
		t.Fatalf("expected section 1 to start with ## Requirements, got: %s", sections[1])
	}
}

func TestParseCSV(t *testing.T) {
	csvData := `timestamp,sensor,reading
2026-09-12T10:00:00Z,temperature,42.1
2026-09-12T10:00:01Z,temperature,42.5`

	chunks, err := ParseCSV([]byte(csvData), Options{CSVWithHeaders: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 row chunks, got %d: %+v", len(chunks), chunks)
	}
	expectedFirst := "timestamp: 2026-09-12T10:00:00Z | sensor: temperature | reading: 42.1"
	if chunks[0] != expectedFirst {
		t.Fatalf("expected '%s', got '%s'", expectedFirst, chunks[0])
	}
}

func TestParseXML(t *testing.T) {
	xmlData := `<?xml version="1.0"?>
<events>
  <event>
    <type>thermal_alert</type>
    <value>55C</value>
  </event>
  <event>
    <type>cpu_burst</type>
    <value>92%</value>
  </event>
</events>`

	chunks, err := ParseXML([]byte(xmlData), DefaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 XML event chunks, got %d: %+v", len(chunks), chunks)
	}
	if !strings.Contains(chunks[0], "type: thermal_alert") || !strings.Contains(chunks[0], "value: 55C") {
		t.Fatalf("unexpected chunk content: %s", chunks[0])
	}
}

func TestParsePDF(t *testing.T) {
	// Synthesize a valid minimal PDF stream with FlateDecode and BT ... ET
	streamText := "BT\n/F1 12 Tf\n(Sekha Cognitive Architecture) Tj\nET\n"
	var zlibBuf bytes.Buffer
	zw := zlib.NewWriter(&zlibBuf)
	zw.Write([]byte(streamText))
	zw.Close()

	var pdfBuf bytes.Buffer
	pdfBuf.WriteString("%PDF-1.4\n")
	pdfBuf.WriteString("1 0 obj\n<< /Length 100 /Filter /FlateDecode >>\nstream\n")
	pdfBuf.Write(zlibBuf.Bytes())
	pdfBuf.WriteString("\nendstream\nendobj\n%%EOF")

	chunks, err := ParsePDF(pdfBuf.Bytes(), DefaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatalf("expected at least 1 chunk from PDF, got 0")
	}
	if !strings.Contains(chunks[0], "Sekha Cognitive Architecture") {
		t.Fatalf("expected extracted text to contain 'Sekha Cognitive Architecture', got: %s", chunks[0])
	}
}

func TestDetectFormat(t *testing.T) {
	if DetectFormat("doc.txt", "", nil) != FormatTXT {
		t.Fatalf("failed txt detection")
	}
	if DetectFormat("paper.pdf", "", nil) != FormatPDF {
		t.Fatalf("failed pdf detection")
	}
	if DetectFormat("metrics.csv", "", nil) != FormatCSV {
		t.Fatalf("failed csv detection")
	}
	if DetectFormat("feed.xml", "", nil) != FormatXML {
		t.Fatalf("failed xml detection")
	}
	if DetectFormat("notes.md", "", nil) != FormatMD {
		t.Fatalf("failed md detection")
	}
	// Content-Type fallback
	if DetectFormat("", "text/markdown", nil) != FormatMD {
		t.Fatalf("failed content-type md detection")
	}
	// Sniffing
	if DetectFormat("", "", []byte("%PDF-1.7")) != FormatPDF {
		t.Fatalf("failed pdf sniffing")
	}
	if DetectFormat("events.json", "", nil) != FormatJSON {
		t.Fatalf("failed json detection")
	}
}

func TestParseJSON(t *testing.T) {
	// JSON Array
	arrayJSON := `[{"id": 1, "text": "event 1"}, {"id": 2, "text": "event 2"}]`
	chunks, err := ParseJSON([]byte(arrayJSON), DefaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}

	// JSON Container with items
	containerJSON := `{"origin": "scraper", "items": ["line 1", "line 2", "line 3"]}`
	chunks, err = ParseJSON([]byte(containerJSON), DefaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks from container, got %d", len(chunks))
	}

	// NDJSON
	ndjson := "{\"seq\": 1}\n{\"seq\": 2}\n{\"seq\": 3}\n"
	chunks, err = ParseJSON([]byte(ndjson), DefaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks from ndjson, got %d", len(chunks))
	}
}

