package parser

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	paraSplitRegex    = regexp.MustCompile(`\r?\n\s*\r?\n`)
	logTimestampRegex = regexp.MustCompile(`(?m)^(\d{4}-\d{2}-\d{2}|\[\d{4}-\d{2}-\d{2}|\[\s*\d+\.\d+\]|[A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})`)
	logPrefixRegex    = regexp.MustCompile(`(?mi)^(\[(DEBUG|INFO|WARN|WARNING|ERROR|FATAL|ALERT|CRITICAL|TRACE)\]|DEBUG:|INFO:|WARN:|ERROR:|CRITICAL:|ALERT:|kernel:|ping |GET |POST )`)
)

// isLogStream checks if text contains typical log entry markers (timestamps, log levels, syslog, etc.).
func isLogStream(text string) bool {
	return logTimestampRegex.MatchString(text) || logPrefixRegex.MatchString(text)
}

// splitAtSemanticBoundaries partitions a large string into chunks up to maxBytes without splitting mid-token.
func splitAtSemanticBoundaries(text string, maxBytes int) []string {
	if maxBytes <= 0 || len(text) <= maxBytes {
		return []string{text}
	}

	var chunks []string
	remaining := text

	for len(remaining) > maxBytes {
		sub := remaining[:maxBytes]

		// 1. Try to split at a line break
		splitIdx := strings.LastIndexAny(sub, "\r\n")

		// 2. Try to split at sentence punctuation followed by a space
		if splitIdx <= 0 {
			for _, punct := range []string{". ", "! ", "? ", "; "} {
				if idx := strings.LastIndex(sub, punct); idx > splitIdx {
					splitIdx = idx + len(punct) - 1
				}
			}
		}

		// 3. Try to split at whitespace (space or tab)
		if splitIdx <= 0 {
			splitIdx = strings.LastIndexAny(sub, " \t")
		}

		// 4. Try to split at punctuation delimiters
		if splitIdx <= 0 {
			splitIdx = strings.LastIndexAny(sub, ",:-|")
		}

		// 5. Lookahead slightly past maxBytes if a word is crossing the boundary (up to 20% past)
		if splitIdx <= 0 {
			lookAheadLimit := maxBytes + maxBytes/5
			if lookAheadLimit > len(remaining) {
				lookAheadLimit = len(remaining)
			}
			if idx := strings.IndexAny(remaining[maxBytes:lookAheadLimit], " \t\r\n"); idx >= 0 {
				splitIdx = maxBytes + idx
			}
		}

		// 6. Hard boundary: ensure we do not split in the middle of a multi-byte UTF-8 rune
		if splitIdx <= 0 {
			splitIdx = maxBytes
			for splitIdx > 0 && !utf8.RuneStart(remaining[splitIdx]) {
				splitIdx--
			}
			if splitIdx <= 0 {
				splitIdx = maxBytes
			}
		}

		chunk := strings.TrimSpace(remaining[:splitIdx])
		if chunk != "" {
			chunks = append(chunks, chunk)
		}
		remaining = strings.TrimSpace(remaining[splitIdx:])
	}

	if len(remaining) > 0 {
		chunks = append(chunks, remaining)
	}

	return chunks
}

// parseLogOrLineStream parses text into discrete log line chunks, preserving indented continuations.
func parseLogOrLineStream(text string, maxBytes int) []string {
	rawLines := strings.Split(text, "\n")
	var chunks []string
	var current strings.Builder

	flush := func() {
		if current.Len() > 0 {
			s := strings.TrimSpace(current.String())
			if s != "" {
				if maxBytes > 0 && len(s) > maxBytes {
					chunks = append(chunks, splitAtSemanticBoundaries(s, maxBytes)...)
				} else {
					chunks = append(chunks, s)
				}
			}
			current.Reset()
		}
	}

	for _, rawLine := range rawLines {
		trimmedLine := strings.TrimRight(rawLine, "\r")
		trimmedSpace := strings.TrimSpace(trimmedLine)
		if trimmedSpace == "" {
			continue
		}

		// If line begins with whitespace (indentation like spaces or tabs), it is a continuation of the prior log entry (e.g. stack trace)
		isContinuation := len(rawLine) > 0 && (rawLine[0] == ' ' || rawLine[0] == '\t')

		if isContinuation && current.Len() > 0 {
			current.WriteString("\n")
			current.WriteString(trimmedLine)
		} else {
			flush()
			current.WriteString(trimmedLine)
		}
	}
	flush()

	return chunks
}

// parseParagraphStream splits text on empty-line paragraph boundaries and bounds them by maxBytes.
func parseParagraphStream(text string, maxBytes int) []string {
	rawParas := paraSplitRegex.Split(text, -1)
	var chunks []string

	for _, p := range rawParas {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if maxBytes > 0 && len(p) > maxBytes {
			chunks = append(chunks, splitAtSemanticBoundaries(p, maxBytes)...)
		} else {
			chunks = append(chunks, p)
		}
	}

	return chunks
}

// ParseTXT splits raw plain text according to the requested chunking strategy.
func ParseTXT(data []byte, opts Options) ([]string, error) {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return nil, nil
	}

	maxBytes := opts.MaxChunkBytes
	if maxBytes <= 0 {
		maxBytes = 4096
	}

	switch opts.ChunkBy {
	case "doc":
		if len(text) > maxBytes {
			return splitAtSemanticBoundaries(text, maxBytes), nil
		}
		return []string{text}, nil

	case "line":
		return parseLogOrLineStream(text, maxBytes), nil

	case "para":
		return parseParagraphStream(text, maxBytes), nil

	case "auto", "":
		hasDoubleNewline := paraSplitRegex.MatchString(text)
		hasNewline := strings.Contains(text, "\n")

		// Case A: Multi-line text with no paragraph breaks -> line stream
		if !hasDoubleNewline && hasNewline {
			return parseLogOrLineStream(text, maxBytes), nil
		}

		// Case B: Text with paragraph breaks, but containing structured log entries
		if hasDoubleNewline {
			if isLogStream(text) {
				return parseLogOrLineStream(text, maxBytes), nil
			}
			return parseParagraphStream(text, maxBytes), nil
		}

		// Case C: Single line or bounded block
		if len(text) > maxBytes {
			return splitAtSemanticBoundaries(text, maxBytes), nil
		}
		return []string{text}, nil

	default:
		return parseLogOrLineStream(text, maxBytes), nil
	}
}
