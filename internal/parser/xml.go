package parser

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

// ParseXML extracts structured textual chunks from XML.
func ParseXML(data []byte, opts Options) ([]string, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}

	if opts.ChunkBy == "doc" {
		return []string{string(data)}, nil
	}

	decoder := xml.NewDecoder(bytes.NewReader(data))
	var chunks []string
	var elementStack []string

	var currentTag string
	var currentText strings.Builder
	var itemFields []string

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Fallback to text parsing if malformed XML
			return ParseTXT(data, opts)
		}

		switch elem := token.(type) {
		case xml.StartElement:
			elementStack = append(elementStack, elem.Name.Local)
			currentTag = elem.Name.Local
			currentText.Reset()

		case xml.EndElement:
			text := strings.TrimSpace(currentText.String())
			if text != "" && currentTag != "" {
				itemFields = append(itemFields, fmt.Sprintf("%s: %s", currentTag, text))
			}
			currentText.Reset()

			if len(elementStack) > 0 {
				closedTag := elementStack[len(elementStack)-1]
				elementStack = elementStack[:len(elementStack)-1]

				// If closing a repeated container element (e.g., item, entry, record, event, row)
				// or when stack depth drops back to 1 (top-level element children)
				if isRecordBoundary(closedTag) || (len(elementStack) == 1 && len(itemFields) > 0) {
					if len(itemFields) > 0 {
						chunks = append(chunks, strings.Join(itemFields, " | "))
						itemFields = nil
					}
				}
			}

		case xml.CharData:
			currentText.Write(elem)
		}
	}

	if len(itemFields) > 0 {
		chunks = append(chunks, strings.Join(itemFields, " | "))
	}

	if len(chunks) == 0 {
		return ParseTXT(data, Options{ChunkBy: "para"})
	}

	return chunks, nil
}

func isRecordBoundary(tag string) bool {
	lower := strings.ToLower(tag)
	switch lower {
	case "item", "entry", "record", "event", "message", "row", "article", "feed_item", "log":
		return true
	default:
		return false
	}
}
