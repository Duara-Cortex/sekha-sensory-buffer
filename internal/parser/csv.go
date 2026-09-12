package parser

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// ParseCSV extracts rows from CSV or TSV data.
func ParseCSV(data []byte, opts Options) ([]string, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}

	if opts.ChunkBy == "doc" {
		return []string{string(data)}, nil
	}

	r := bytes.NewReader(data)
	csvReader := csv.NewReader(r)
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields
	csvReader.TrimLeadingSpace = true

	// Check if TSV
	if bytes.Contains(data, []byte("\t")) && !bytes.Contains(data, []byte(",")) {
		csvReader.Comma = '\t'
	}

	var rows [][]string
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// If CSV parse error, fallback to line-by-line
			return ParseTXT(data, Options{ChunkBy: "line"})
		}
		rows = append(rows, record)
	}

	if len(rows) == 0 {
		return nil, nil
	}

	var chunks []string

	// If CSV has headers and CSVWithHeaders is requested
	if opts.CSVWithHeaders && len(rows) > 1 {
		headers := rows[0]
		for _, row := range rows[1:] {
			var b strings.Builder
			for i, val := range row {
				val = strings.TrimSpace(val)
				if val == "" {
					continue
				}
				headerName := fmt.Sprintf("col_%d", i+1)
				if i < len(headers) && strings.TrimSpace(headers[i]) != "" {
					headerName = strings.TrimSpace(headers[i])
				}
				if b.Len() > 0 {
					b.WriteString(" | ")
				}
				b.WriteString(fmt.Sprintf("%s: %s", headerName, val))
			}
			if b.Len() > 0 {
				chunks = append(chunks, b.String())
			}
		}
	} else {
		// Just output each row joined with commas
		for _, row := range rows {
			line := strings.Join(row, ", ")
			if strings.TrimSpace(line) != "" {
				chunks = append(chunks, line)
			}
		}
	}

	return chunks, nil
}
