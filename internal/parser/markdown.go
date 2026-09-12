package parser

import (
	"bufio"
	"bytes"
	"strings"
)

// ParseMarkdown extracts chunks from Markdown, splitting by headings or paragraphs.
func ParseMarkdown(data []byte, opts Options) ([]string, error) {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return nil, nil
	}

	if opts.ChunkBy == "doc" {
		return []string{text}, nil
	}
	if opts.ChunkBy == "para" {
		return ParseTXT(data, Options{ChunkBy: "para"})
	}
	if opts.ChunkBy == "line" {
		return ParseTXT(data, Options{ChunkBy: "line"})
	}

	// Default / "section" / "auto": Split by Markdown headings (#, ##, ###)
	var sections []string
	var currentSection strings.Builder
	var currentHeading string

	scanner := bufio.NewScanner(bytes.NewReader(data))
	inCodeBlock := false

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Track code fence to not split headings inside code blocks
		if strings.HasPrefix(trimmed, "```") {
			inCodeBlock = !inCodeBlock
		}

		isHeading := false
		if !inCodeBlock && (strings.HasPrefix(trimmed, "# ") || strings.HasPrefix(trimmed, "## ") || strings.HasPrefix(trimmed, "### ") || strings.HasPrefix(trimmed, "#### ")) {
			isHeading = true
		}

		if isHeading {
			if currentSection.Len() > 0 {
				content := strings.TrimSpace(currentSection.String())
				if content != "" {
					sections = append(sections, content)
				}
				currentSection.Reset()
			}
			currentHeading = trimmed
			currentSection.WriteString(line + "\n")
		} else {
			currentSection.WriteString(line + "\n")
		}
	}

	if currentSection.Len() > 0 {
		content := strings.TrimSpace(currentSection.String())
		if content != "" {
			sections = append(sections, content)
		}
	}

	// If no headings found, fallback to paragraph chunking
	if len(sections) <= 1 && currentHeading == "" {
		return ParseTXT(data, Options{ChunkBy: "para"})
	}

	return sections, nil
}
