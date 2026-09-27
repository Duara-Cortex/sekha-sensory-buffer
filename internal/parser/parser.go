package parser

import (
	"bytes"
	"io"
	"path/filepath"
	"strings"
)

// Format represents the recognized textual format.
type Format string

const (
	FormatTXT  Format = "txt"
	FormatMD   Format = "md"
	FormatCSV  Format = "csv"
	FormatXML  Format = "xml"
	FormatPDF  Format = "pdf"
	FormatJSON Format = "json"
	FormatRaw  Format = "raw"
)

// Options specifies how documents should be chunked and parsed.
type Options struct {
	// ChunkBy: "doc" (whole document), "para" (paragraphs), "line" (lines), "section" (headers in MD), "auto"
	ChunkBy string
	// MaxChunkBytes: maximum bytes/chars per chunk before splitting at semantic boundaries (default: 4096)
	MaxChunkBytes int
	// HeaderPrefix for CSV: whether to prefix values with column names (e.g. "col: val | col2: val2")
	CSVWithHeaders bool
}

// DefaultOptions provides sensible defaults for sensory ingestion.
func DefaultOptions() Options {
	return Options{
		ChunkBy:        "auto",
		MaxChunkBytes:  4096,
		CSVWithHeaders: true,
	}
}

// DetectFormat identifies the format based on filename, Content-Type, or byte sniff.
func DetectFormat(filename, contentType string, sample []byte) Format {
	// 1. By file extension
	if filename != "" {
		ext := strings.ToLower(filepath.Ext(filename))
		switch ext {
		case ".txt", ".text", ".log":
			return FormatTXT
		case ".md", ".markdown":
			return FormatMD
		case ".csv", ".tsv":
			return FormatCSV
		case ".xml", ".rss", ".atom", ".svg":
			return FormatXML
		case ".pdf":
			return FormatPDF
		case ".json", ".ndjson", ".jsonl":
			return FormatJSON
		}
	}

	// 2. By MIME Content-Type
	cleanType := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	switch cleanType {
	case "text/plain":
		return FormatTXT
	case "text/markdown", "text/x-markdown":
		return FormatMD
	case "text/csv", "application/csv":
		return FormatCSV
	case "application/xml", "text/xml":
		return FormatXML
	case "application/pdf":
		return FormatPDF
	case "application/json", "application/x-ndjson":
		return FormatJSON
	}

	// 3. Byte Sniffing
	trimmed := bytes.TrimSpace(sample)
	if bytes.HasPrefix(trimmed, []byte("%PDF-")) {
		return FormatPDF
	}
	if bytes.HasPrefix(trimmed, []byte("<?xml")) || (bytes.HasPrefix(trimmed, []byte("<")) && bytes.HasSuffix(trimmed, []byte(">"))) {
		return FormatXML
	}
	if (bytes.HasPrefix(trimmed, []byte("{")) && bytes.HasSuffix(trimmed, []byte("}"))) ||
		(bytes.HasPrefix(trimmed, []byte("[")) && bytes.HasSuffix(trimmed, []byte("]"))) {
		return FormatJSON
	}

	return FormatTXT
}

// Parse extracts text chunks from an io.Reader based on Format and Options.
func Parse(format Format, r io.Reader, opts Options) ([]string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var chunks []string
	switch format {
	case FormatTXT:
		return ParseTXT(data, opts)
	case FormatMD:
		chunks, err = ParseMarkdown(data, opts)
	case FormatCSV:
		chunks, err = ParseCSV(data, opts)
	case FormatXML:
		chunks, err = ParseXML(data, opts)
	case FormatPDF:
		chunks, err = ParsePDF(data, opts)
	case FormatJSON:
		chunks, err = ParseJSON(data, opts)
	default:
		return ParseTXT(data, opts)
	}
	if err != nil {
		return nil, err
	}
	return boundChunks(chunks, opts), nil
}

// boundChunks re-partitions any chunk exceeding opts.MaxChunkBytes at line/paragraph
// boundaries, so format-specific parsers never emit a single oversized block.
func boundChunks(chunks []string, opts Options) []string {
	if opts.MaxChunkBytes <= 0 {
		return chunks
	}
	out := make([]string, 0, len(chunks))
	for _, c := range chunks {
		if len(c) <= opts.MaxChunkBytes {
			out = append(out, c)
			continue
		}
		parts, _ := ParseTXT([]byte(c), Options{ChunkBy: "auto", MaxChunkBytes: opts.MaxChunkBytes})
		if len(parts) == 0 {
			out = append(out, c)
			continue
		}
		out = append(out, parts...)
	}
	return out
}
