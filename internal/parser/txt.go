package parser

import (
	"regexp"
	"strings"
)

var paraSplitRegex = regexp.MustCompile(`\r?\n\s*\r?\n`)

// ParseTXT splits raw plain text according to the requested chunking strategy.
func ParseTXT(data []byte, opts Options) ([]string, error) {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return nil, nil
	}

	switch opts.ChunkBy {
	case "doc":
		return []string{text}, nil

	case "line":
		var lines []string
		rawLines := strings.Split(text, "\n")
		for _, l := range rawLines {
			l = strings.TrimSpace(l)
			if l != "" {
				lines = append(lines, l)
			}
		}
		return lines, nil

	case "para", "auto", "":
		// Split by paragraph (empty line separation)
		rawParas := paraSplitRegex.Split(text, -1)
		var paras []string
		for _, p := range rawParas {
			p = strings.TrimSpace(p)
			if p != "" {
				paras = append(paras, p)
			}
		}
		if len(paras) == 0 && text != "" {
			return []string{text}, nil
		}
		return paras, nil

	default:
		return []string{text}, nil
	}
}
