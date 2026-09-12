package parser

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ParsePDF extracts plain text streams from a PDF document without external dependencies.
func ParsePDF(data []byte, opts Options) ([]string, error) {
	if len(data) == 0 {
		return nil, nil
	}

	if !bytes.HasPrefix(bytes.TrimSpace(data), []byte("%PDF-")) {
		// Fallback to text parsing if not a valid PDF header
		return ParseTXT(data, opts)
	}

	// 1. Extract and decompress all content streams in the PDF
	streams := extractPDFStreams(data)

	// 2. Parse text operators (BT ... ET, Tj, TJ, etc.) from each stream
	var pagesText []string
	for i, stream := range streams {
		extracted := extractTextFromStream(stream)
		trimmed := strings.TrimSpace(extracted)
		if trimmed != "" {
			if opts.ChunkBy == "para" {
				paras := strings.Split(trimmed, "\n\n")
				for _, p := range paras {
					p = strings.TrimSpace(p)
					if p != "" {
						pagesText = append(pagesText, p)
					}
				}
			} else {
				header := fmt.Sprintf("--- PDF Section %d ---\n%s", i+1, trimmed)
				pagesText = append(pagesText, header)
			}
		}
	}

	// 3. Fallback: If no stream-based text was extracted (e.g. uncompressed / direct objects)
	if len(pagesText) == 0 {
		directText := extractTextFromStream(data)
		trimmed := strings.TrimSpace(directText)
		if trimmed != "" {
			return ParseTXT([]byte(trimmed), opts)
		}
		return []string{"[PDF: No extractable text stream found - document may contain scanned raster images]"}, nil
	}

	if opts.ChunkBy == "doc" {
		return []string{strings.Join(pagesText, "\n\n")}, nil
	}

	return pagesText, nil
}

// extractPDFStreams locates `stream...endstream` blocks and decompresses FlateDecode streams.
func extractPDFStreams(data []byte) [][]byte {
	var streams [][]byte
	streamTag := []byte("stream")
	endStreamTag := []byte("endstream")

	offset := 0
	for {
		startIdx := bytes.Index(data[offset:], streamTag)
		if startIdx == -1 {
			break
		}
		streamStart := offset + startIdx + len(streamTag)

		// Skip immediate CRLF after 'stream'
		if streamStart < len(data) && data[streamStart] == '\r' {
			streamStart++
		}
		if streamStart < len(data) && data[streamStart] == '\n' {
			streamStart++
		}

		endIdx := bytes.Index(data[streamStart:], endStreamTag)
		if endIdx == -1 {
			break
		}
		streamEnd := streamStart + endIdx

		rawStream := data[streamStart:streamEnd]

		// Check if preceding dictionary contains /FlateDecode
		dictStart := bytes.LastIndex(data[:offset+startIdx], []byte("<<"))
		isFlate := false
		if dictStart != -1 {
			dictContent := data[dictStart : offset+startIdx]
			if bytes.Contains(dictContent, []byte("/FlateDecode")) {
				isFlate = true
			}
		}

		if isFlate {
			// Decompress zlib stream
			if decompressed, err := decompressZlib(rawStream); err == nil && len(decompressed) > 0 {
				streams = append(streams, decompressed)
			}
		} else {
			streams = append(streams, rawStream)
		}

		offset = streamEnd + len(endStreamTag)
	}

	return streams
}

func decompressZlib(data []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// extractTextFromStream parses PDF text operators (BT ... ET, Tj, TJ, ', ") within a content stream.
func extractTextFromStream(content []byte) string {
	var sb strings.Builder
	inTextObject := false
	n := len(content)

	for i := 0; i < n; {
		// Detect BT (Begin Text)
		if !inTextObject && i+2 <= n && string(content[i:i+2]) == "BT" && isDelimiterOrSpace(content, i-1, i+2) {
			inTextObject = true
			i += 2
			continue
		}

		// Detect ET (End Text)
		if inTextObject && i+2 <= n && string(content[i:i+2]) == "ET" && isDelimiterOrSpace(content, i-1, i+2) {
			inTextObject = false
			sb.WriteString("\n")
			i += 2
			continue
		}

		if inTextObject {
			// Check for literal string ( ... )
			if content[i] == '(' {
				str, nextIdx := parseLiteralString(content, i)
				i = nextIdx

				// Check operator following the string
				op, opIdx := readNextOperator(content, i)
				if op == "Tj" || op == "'" || op == "\"" {
					sb.WriteString(str)
					if op == "'" || op == "\"" {
						sb.WriteString("\n")
					} else {
						sb.WriteString(" ")
					}
					i = opIdx
				}
				continue
			}

			// Check for hex string < ... >
			if content[i] == '<' && i+1 < n && content[i+1] != '<' {
				str, nextIdx := parseHexString(content, i)
				i = nextIdx

				op, opIdx := readNextOperator(content, i)
				if op == "Tj" || op == "'" || op == "\"" {
					sb.WriteString(str)
					sb.WriteString(" ")
					i = opIdx
				}
				continue
			}

			// Check for TJ array [ (Hello) 20 (World) ] TJ
			if content[i] == '[' {
				arrStr, nextIdx := parseTJArray(content, i)
				i = nextIdx

				op, opIdx := readNextOperator(content, i)
				if op == "TJ" {
					sb.WriteString(arrStr)
					sb.WriteString(" ")
					i = opIdx
				}
				continue
			}

			// Newline operators: T*, TD, Td
			if i+2 <= n && (string(content[i:i+2]) == "T*" || string(content[i:i+2]) == "Td" || string(content[i:i+2]) == "TD") && isDelimiterOrSpace(content, i-1, i+2) {
				sb.WriteString("\n")
				i += 2
				continue
			}
		}

		i++
	}

	return cleanExtractedText(sb.String())
}

func isDelimiterOrSpace(data []byte, prevIdx, nextIdx int) bool {
	prevOk := (prevIdx < 0 || isSpace(data[prevIdx]))
	nextOk := (nextIdx >= len(data) || isSpace(data[nextIdx]))
	return prevOk && nextOk
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n' || b == '/' || b == '[' || b == ']' || b == '(' || b == ')'
}

func parseLiteralString(data []byte, start int) (string, int) {
	var sb strings.Builder
	depth := 0
	i := start + 1 // skip '('
	n := len(data)

	for i < n {
		b := data[i]
		if b == '\\' && i+1 < n {
			next := data[i+1]
			switch next {
			case 'n':
				sb.WriteByte('\n')
				i += 2
			case 'r':
				sb.WriteByte('\r')
				i += 2
			case 't':
				sb.WriteByte('\t')
				i += 2
			case 'b':
				sb.WriteByte('\b')
				i += 2
			case 'f':
				sb.WriteByte('\f')
				i += 2
			case '(', ')', '\\':
				sb.WriteByte(next)
				i += 2
			default:
				// Check for octal escape \ddd
				if next >= '0' && next <= '7' {
					octalLen := 1
					for octalLen < 3 && i+1+octalLen < n && data[i+1+octalLen] >= '0' && data[i+1+octalLen] <= '7' {
						octalLen++
					}
					octalStr := string(data[i+1 : i+1+octalLen])
					if val, err := strconv.ParseInt(octalStr, 8, 32); err == nil {
						sb.WriteByte(byte(val))
					}
					i += 1 + octalLen
				} else {
					sb.WriteByte(next)
					i += 2
				}
			}
			continue
		}

		if b == '(' {
			depth++
			sb.WriteByte(b)
		} else if b == ')' {
			if depth == 0 {
				return sb.String(), i + 1
			}
			depth--
			sb.WriteByte(b)
		} else {
			sb.WriteByte(b)
		}
		i++
	}

	return sb.String(), i
}

func parseHexString(data []byte, start int) (string, int) {
	end := bytes.IndexByte(data[start+1:], '>')
	if end == -1 {
		return "", start + 1
	}
	hexBytes := bytes.TrimSpace(data[start+1 : start+1+end])
	decoded, err := hex.DecodeString(string(hexBytes))
	if err != nil {
		return "", start + 1 + end + 1
	}
	return string(decoded), start + 1 + end + 1
}

func parseTJArray(data []byte, start int) (string, int) {
	var sb strings.Builder
	i := start + 1 // skip '['
	n := len(data)

	for i < n {
		// skip spaces
		for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\r' || data[i] == '\n') {
			i++
		}
		if i >= n || data[i] == ']' {
			return sb.String(), i + 1
		}

		if data[i] == '(' {
			str, nextIdx := parseLiteralString(data, i)
			sb.WriteString(str)
			i = nextIdx
			continue
		}

		if data[i] == '<' && i+1 < n && data[i+1] != '<' {
			str, nextIdx := parseHexString(data, i)
			sb.WriteString(str)
			i = nextIdx
			continue
		}

		// Number in TJ array indicates kerning / spacing: if significant negative number, add space
		if (data[i] >= '0' && data[i] <= '9') || data[i] == '-' || data[i] == '+' {
			numStart := i
			for i < n && ((data[i] >= '0' && data[i] <= '9') || data[i] == '.' || data[i] == '-' || data[i] == '+') {
				i++
			}
			if val, err := strconv.ParseFloat(string(data[numStart:i]), 64); err == nil {
				if val < -100 { // Large negative adjustment corresponds to word space
					sb.WriteString(" ")
				}
			}
			continue
		}

		i++
	}

	return sb.String(), i
}

func readNextOperator(data []byte, start int) (string, int) {
	n := len(data)
	i := start
	for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\r' || data[i] == '\n') {
		i++
	}
	opStart := i
	for i < n && data[i] > ' ' && data[i] != '(' && data[i] != '[' && data[i] != '<' && data[i] != '/' {
		i++
	}
	return string(data[opStart:i]), i
}

func cleanExtractedText(s string) string {
	lines := strings.Split(s, "\n")
	var cleaned []string
	for _, l := range lines {
		t := strings.TrimSpace(l)
		if t != "" {
			cleaned = append(cleaned, t)
		}
	}
	return strings.Join(cleaned, "\n")
}
