package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/parser"
)

// Server encapsulates the HTTP handler and buffer reference.
type Server struct {
	RingBuffer *buffer.RingBuffer
	mux        *http.ServeMux
}

// NewServer initializes the HTTP multiplexer with registered routes.
func NewServer(rb *buffer.RingBuffer) *Server {
	s := &Server{
		RingBuffer: rb,
		mux:        http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("POST /api/v1/sensory/ingest", s.handleIngest)
	s.mux.HandleFunc("GET /api/v1/sensory/buffer", s.handleBufferQuery)
	s.mux.HandleFunc("GET /api/v1/sensory/stats", s.handleStats)
	s.mux.HandleFunc("GET /api/v1/sensory/health", s.handleHealth)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// IngestResponse is returned on successful packet ingestion.
type IngestResponse struct {
	Status        string        `json:"status"`
	Format        parser.Format `json:"format,omitempty"`
	IngestedCount int           `json:"ingested_count"`
	FirstSeq      uint64        `json:"first_seq,omitempty"`
	LastSeq       uint64        `json:"last_seq,omitempty"`
	Timestamp     time.Time     `json:"timestamp"`
}

// SingleIngestRequest represents a single structured JSON packet.
type SingleIngestRequest struct {
	Origin  string `json:"origin"`
	Data    string `json:"data"`
	Payload string `json:"payload"`
	Text    string `json:"text"`
}

func (r *SingleIngestRequest) ExtractData() string {
	if r.Data != "" {
		return r.Data
	}
	if r.Payload != "" {
		return r.Payload
	}
	return r.Text
}

// BatchIngestRequest represents a batch of strings under an origin.
type BatchIngestRequest struct {
	Origin string   `json:"origin"`
	Items  []string `json:"items"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	defaultOrigin := query.Get("origin")
	formatOverride := query.Get("format")
	chunkBy := query.Get("chunk_by")

	parseOpts := parser.DefaultOptions()
	if chunkBy != "" {
		parseOpts.ChunkBy = chunkBy
	}

	contentType := r.Header.Get("Content-Type")

	// 1. Multipart Form File Upload (e.g. curl -F "file=@document.pdf" ...)
	if strings.HasPrefix(contentType, "multipart/form-data") {
		err := r.ParseMultipartForm(16 * 1024 * 1024) // 16MB max
		if err != nil {
			http.Error(w, `{"error": "failed to parse multipart form: `+err.Error()+`"}`, http.StatusBadRequest)
			return
		}

		file, fileHeader, err := r.FormFile("file")
		if err != nil {
			http.Error(w, `{"error": "missing 'file' field in multipart form"}`, http.StatusBadRequest)
			return
		}
		defer file.Close()

		fileData, err := io.ReadAll(io.LimitReader(file, 16*1024*1024))
		if err != nil {
			http.Error(w, `{"error": "failed to read file content"}`, http.StatusBadRequest)
			return
		}

		if defaultOrigin == "" {
			defaultOrigin = fileHeader.Filename
			if defaultOrigin == "" {
				defaultOrigin = "file-upload"
			}
		}

		detectedFormat := parser.Format(formatOverride)
		if detectedFormat == "" {
			fileMime := fileHeader.Header.Get("Content-Type")
			detectedFormat = parser.DetectFormat(fileHeader.Filename, fileMime, fileData)
		}

		chunks, err := parser.Parse(detectedFormat, bytes.NewReader(fileData), parseOpts)
		if err != nil {
			http.Error(w, `{"error": "failed to parse document: `+err.Error()+`"}`, http.StatusBadRequest)
			return
		}

		if len(chunks) == 0 {
			http.Error(w, `{"error": "no extractable content found in file"}`, http.StatusBadRequest)
			return
		}

		ingested := s.RingBuffer.IngestBatch(defaultOrigin, chunks)
		writeJSON(w, http.StatusCreated, IngestResponse{
			Status:        "ingested",
			Format:        detectedFormat,
			IngestedCount: len(ingested),
			FirstSeq:      ingested[0].Seq,
			LastSeq:       ingested[len(ingested)-1].Seq,
			Timestamp:     time.Now().UTC(),
		})
		return
	}

	// 2. Read full request body
	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 16*1024*1024))
	if err != nil {
		http.Error(w, `{"error": "failed to read body"}`, http.StatusBadRequest)
		return
	}
	if len(bodyBytes) == 0 {
		http.Error(w, `{"error": "empty request body"}`, http.StatusBadRequest)
		return
	}

	if defaultOrigin == "" {
		defaultOrigin = "default"
	}

	// Check if explicit format specified or if binary/document Content-Type
	cleanType := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	isDocType := cleanType == "text/markdown" || cleanType == "text/x-markdown" ||
		cleanType == "text/csv" || cleanType == "application/csv" ||
		cleanType == "application/xml" || cleanType == "text/xml" ||
		cleanType == "application/pdf" || formatOverride != ""

	if isDocType {
		var detectedFormat parser.Format
		if formatOverride != "" {
			detectedFormat = parser.Format(formatOverride)
		} else {
			detectedFormat = parser.DetectFormat("", contentType, bodyBytes)
		}

		chunks, err := parser.Parse(detectedFormat, bytes.NewReader(bodyBytes), parseOpts)
		if err != nil {
			http.Error(w, `{"error": "failed to parse payload: `+err.Error()+`"}`, http.StatusBadRequest)
			return
		}

		if len(chunks) == 0 {
			http.Error(w, `{"error": "no extractable content found in payload"}`, http.StatusBadRequest)
			return
		}

		ingested := s.RingBuffer.IngestBatch(defaultOrigin, chunks)
		writeJSON(w, http.StatusCreated, IngestResponse{
			Status:        "ingested",
			Format:        detectedFormat,
			IngestedCount: len(ingested),
			FirstSeq:      ingested[0].Seq,
			LastSeq:       ingested[len(ingested)-1].Seq,
			Timestamp:     time.Now().UTC(),
		})
		return
	}

	// 3. Newline-delimited text or plain text stream
	if cleanType == "application/x-ndjson" || (cleanType == "text/plain" && chunkBy == "line") {
		chunksText, err := parser.ParseTXT(bodyBytes, parser.Options{ChunkBy: "line"})
		if err == nil && len(chunksText) > 0 {
			ingested := s.RingBuffer.IngestBatch(defaultOrigin, chunksText)
			writeJSON(w, http.StatusCreated, IngestResponse{
				Status:        "ingested",
				Format:        parser.FormatTXT,
				IngestedCount: len(ingested),
				FirstSeq:      ingested[0].Seq,
				LastSeq:       ingested[len(ingested)-1].Seq,
				Timestamp:     time.Now().UTC(),
			})
			return
		}
	}

	// 4. Try parsing as JSON structures
	if cleanType == "application/json" || cleanType == "" {
		// Try parsing as array of objects: [{"origin": "...", "data": "..."}, ...]
		var arrayReq []SingleIngestRequest
		if err := json.Unmarshal(bodyBytes, &arrayReq); err == nil && len(arrayReq) > 0 {
			chunks := make([]buffer.Chunk, 0, len(arrayReq))
			for _, item := range arrayReq {
				origin := item.Origin
				if origin == "" {
					origin = defaultOrigin
				}
				chunks = append(chunks, s.RingBuffer.Ingest(origin, item.ExtractData()))
			}
			writeJSON(w, http.StatusCreated, IngestResponse{
				Status:        "ingested",
				Format:        parser.FormatJSON,
				IngestedCount: len(chunks),
				FirstSeq:      chunks[0].Seq,
				LastSeq:       chunks[len(chunks)-1].Seq,
				Timestamp:     time.Now().UTC(),
			})
			return
		}

		// Try parsing as batch container: {"origin": "...", "items": ["a", "b"]}
		var batchReq BatchIngestRequest
		if err := json.Unmarshal(bodyBytes, &batchReq); err == nil && len(batchReq.Items) > 0 {
			origin := batchReq.Origin
			if origin == "" {
				origin = defaultOrigin
			}
			chunks := s.RingBuffer.IngestBatch(origin, batchReq.Items)
			writeJSON(w, http.StatusCreated, IngestResponse{
				Status:        "ingested",
				Format:        parser.FormatJSON,
				IngestedCount: len(chunks),
				FirstSeq:      chunks[0].Seq,
				LastSeq:       chunks[len(chunks)-1].Seq,
				Timestamp:     time.Now().UTC(),
			})
			return
		}

		// Try parsing as single object: {"origin": "...", "data": "..."}
		var singleReq SingleIngestRequest
		if err := json.Unmarshal(bodyBytes, &singleReq); err == nil && (singleReq.Data != "" || singleReq.Payload != "" || singleReq.Text != "" || singleReq.Origin != "") {
			origin := singleReq.Origin
			if origin == "" {
				origin = defaultOrigin
			}
			data := singleReq.ExtractData()
			if data == "" {
				data = string(bodyBytes)
			}
			chunk := s.RingBuffer.Ingest(origin, data)
			writeJSON(w, http.StatusCreated, IngestResponse{
				Status:        "ingested",
				Format:        parser.FormatJSON,
				IngestedCount: 1,
				FirstSeq:      chunk.Seq,
				LastSeq:       chunk.Seq,
				Timestamp:     time.Now().UTC(),
			})
			return
		}
	}

	// 5. Fallback: Parse as plain text (paragraphs or doc)
	txtChunks, err := parser.ParseTXT(bodyBytes, parseOpts)
	if err == nil && len(txtChunks) > 0 {
		ingested := s.RingBuffer.IngestBatch(defaultOrigin, txtChunks)
		writeJSON(w, http.StatusCreated, IngestResponse{
			Status:        "ingested",
			Format:        parser.FormatTXT,
			IngestedCount: len(ingested),
			FirstSeq:      ingested[0].Seq,
			LastSeq:       ingested[len(ingested)-1].Seq,
			Timestamp:     time.Now().UTC(),
		})
		return
	}

	// Final Fallback: Single raw chunk
	chunk := s.RingBuffer.Ingest(defaultOrigin, string(bodyBytes))
	writeJSON(w, http.StatusCreated, IngestResponse{
		Status:        "ingested",
		Format:        parser.FormatRaw,
		IngestedCount: 1,
		FirstSeq:      chunk.Seq,
		LastSeq:       chunk.Seq,
		Timestamp:     time.Now().UTC(),
	})
}

// BufferQueryResponse formats output for GET /api/v1/sensory/buffer.
type BufferQueryResponse struct {
	Count  int            `json:"count"`
	Chunks []buffer.Chunk `json:"chunks"`
}

func (s *Server) handleBufferQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit := 100
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	var sinceMs int64
	if sinceMsStr := q.Get("since_ms"); sinceMsStr != "" {
		if parsed, err := strconv.ParseInt(sinceMsStr, 10, 64); err == nil && parsed > 0 {
			sinceMs = parsed
		}
	}

	var sinceSeq uint64
	if sinceSeqStr := q.Get("since_seq"); sinceSeqStr != "" {
		if parsed, err := strconv.ParseUint(sinceSeqStr, 10, 64); err == nil {
			sinceSeq = parsed
		}
	}

	chunks := s.RingBuffer.Query(limit, sinceMs, sinceSeq)
	writeJSON(w, http.StatusOK, BufferQueryResponse{
		Count:  len(chunks),
		Chunks: chunks,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.RingBuffer.Stats()
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := s.RingBuffer.Stats()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":         "ok",
		"uptime_seconds": stats.UptimeSeconds,
		"buffer_fill":    stats.FillPercent,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}
