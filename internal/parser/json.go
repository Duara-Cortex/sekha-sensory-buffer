package parser

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
)

// ParseJSON parses JSON documents, arrays, objects, or NDJSON into text chunks.
func ParseJSON(data []byte, opts Options) ([]string, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}

	if opts.ChunkBy == "doc" {
		return []string{string(trimmed)}, nil
	}

	// 1. Try parsing as JSON array: [{"...": "..."}, ...] or ["a", "b", ...]
	var jsonArray []interface{}
	if err := json.Unmarshal(trimmed, &jsonArray); err == nil && len(jsonArray) > 0 {
		var chunks []string
		for _, elem := range jsonArray {
			switch v := elem.(type) {
			case string:
				if strings.TrimSpace(v) != "" {
					chunks = append(chunks, v)
				}
			default:
				bytesVal, _ := json.Marshal(v)
				chunks = append(chunks, string(bytesVal))
			}
		}
		if len(chunks) > 0 {
			return chunks, nil
		}
	}

	// 2. Try parsing as JSON container with items: {"items": [...]}
	var container struct {
		Items []interface{} `json:"items"`
	}
	if err := json.Unmarshal(trimmed, &container); err == nil && len(container.Items) > 0 {
		var chunks []string
		for _, elem := range container.Items {
			switch v := elem.(type) {
			case string:
				if strings.TrimSpace(v) != "" {
					chunks = append(chunks, v)
				}
			default:
				bytesVal, _ := json.Marshal(v)
				chunks = append(chunks, string(bytesVal))
			}
		}
		if len(chunks) > 0 {
			return chunks, nil
		}
	}

	// 3. Try parsing as NDJSON / JSON Lines (each line is a valid JSON object or string)
	if bytes.Contains(trimmed, []byte("\n")) {
		scanner := bufio.NewScanner(bytes.NewReader(trimmed))
		var lines []string
		allValidJSON := true
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var testObj interface{}
			if err := json.Unmarshal([]byte(line), &testObj); err == nil {
				lines = append(lines, line)
			} else {
				allValidJSON = false
				break
			}
		}
		if allValidJSON && len(lines) > 0 {
			return lines, nil
		}
	}

	// 4. Single JSON Object: return as formatted or compact JSON string
	return []string{string(trimmed)}, nil
}
