package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/classifier"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/parser"
)

// ClassifierTelemetry tracks cumulative filtering activity and noise reduction metrics.
type ClassifierTelemetry struct {
	TotalEvaluated      uint64  `json:"total_evaluated"`
	TotalSalient        uint64  `json:"total_salient"`
	TotalDiscarded      uint64  `json:"total_discarded"`
	NoiseReductionRatio float64 `json:"noise_reduction_ratio"`
}

// Server encapsulates the HTTP handler, buffer, and classifier.
type Server struct {
	RingBuffer *buffer.RingBuffer
	Classifier *classifier.Classifier
	mux        *http.ServeMux

	telemetryMu sync.RWMutex
	telemetry   ClassifierTelemetry
}

// NewServer initializes the HTTP multiplexer with registered routes.
func NewServer(rb *buffer.RingBuffer) *Server {
	s := &Server{
		RingBuffer: rb,
		Classifier: classifier.New(classifier.DefaultSalienceThreshold),
		mux:        http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("POST /api/v1/sensory/ingest", s.handleIngest)
	s.mux.HandleFunc("POST /api/v1/sensory/filter", s.handleFilter)
	s.mux.HandleFunc("GET /api/v1/sensory/buffer", s.handleBufferQuery)
	s.mux.HandleFunc("GET /api/v1/sensory/stats", s.handleStats)
	s.mux.HandleFunc("GET /api/v1/sensory/health", s.handleHealth)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) recordClassifierMetrics(evaluated, salient, discarded int) {
	s.telemetryMu.Lock()
	defer s.telemetryMu.Unlock()

	s.telemetry.TotalEvaluated += uint64(evaluated)
	s.telemetry.TotalSalient += uint64(salient)
	s.telemetry.TotalDiscarded += uint64(discarded)

	if s.telemetry.TotalEvaluated > 0 {
		s.telemetry.NoiseReductionRatio = float64(s.telemetry.TotalDiscarded) / float64(s.telemetry.TotalEvaluated)
	}
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

	// 1. Multipart Form File Upload
	if strings.HasPrefix(contentType, "multipart/form-data") {
		err := r.ParseMultipartForm(16 * 1024 * 1024)
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

	// 5. Fallback: Parse as plain text
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

// FilterRequest encapsulates payload and options for POST /api/v1/sensory/filter.
type FilterRequest struct {
	Task             string                `json:"task,omitempty"`
	TaskDirective    string                `json:"task_directive,omitempty"`
	Threshold        float64               `json:"threshold,omitempty"`
	ChunkBy          string                `json:"chunk_by,omitempty"`
	MaxChunkSize     int                   `json:"max_chunk_size,omitempty"`
	MaxTokens        int                   `json:"max_tokens,omitempty"`
	IncludeDiscarded bool                  `json:"include_discarded,omitempty"`
	FromBuffer       bool                  `json:"from_buffer,omitempty"`
	Limit            int                   `json:"limit,omitempty"`
	SinceMs          int64                 `json:"since_ms,omitempty"`
	SinceSeq         uint64                `json:"since_seq,omitempty"`
	Text             string                `json:"text,omitempty"`
	Data             string                `json:"data,omitempty"`
	Payload          string                `json:"payload,omitempty"`
	Items            []SingleIngestRequest `json:"items,omitempty"`
}

// ExtractText retrieves the raw stream text from any supported JSON text field.
func (r *FilterRequest) ExtractText() string {
	if r.Text != "" {
		return r.Text
	}
	if r.Data != "" {
		return r.Data
	}
	return r.Payload
}

// handleFilter implements POST /api/v1/sensory/filter for attention-gating noise reduction.
func (s *Server) handleFilter(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	q := r.URL.Query()

	task := q.Get("task")
	if task == "" {
		task = q.Get("task_directive")
	}

	threshold := s.Classifier.DefaultThreshold
	if thStr := q.Get("threshold"); thStr != "" {
		if parsed, err := strconv.ParseFloat(thStr, 64); err == nil && parsed > 0 && parsed < 1.0 {
			threshold = parsed
		}
	}
	includeDiscarded := q.Get("include_discarded") == "true"
	fromBuffer := q.Get("from_buffer") == "true"

	chunkBy := q.Get("chunk_by")
	maxChunkBytes := 4096
	if szStr := q.Get("max_chunk_size"); szStr != "" {
		if parsed, err := strconv.Atoi(szStr); err == nil && parsed > 0 {
			maxChunkBytes = parsed
		}
	}
	if tokStr := q.Get("max_tokens"); tokStr != "" {
		if parsed, err := strconv.Atoi(tokStr); err == nil && parsed > 0 {
			maxChunkBytes = parsed * 4
		}
	}

	limit := 100
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	var sinceMs int64
	if sMsStr := q.Get("since_ms"); sMsStr != "" {
		if parsed, err := strconv.ParseInt(sMsStr, 10, 64); err == nil {
			sinceMs = parsed
		}
	}

	var sinceSeq uint64
	if sSeqStr := q.Get("since_seq"); sSeqStr != "" {
		if parsed, err := strconv.ParseUint(sSeqStr, 10, 64); err == nil {
			sinceSeq = parsed
		}
	}

	var chunksToFilter []buffer.Chunk

	if fromBuffer {
		chunksToFilter = s.RingBuffer.Query(limit, sinceMs, sinceSeq)
	} else {
		// Body limit: support up to 16MB payloads (well above 500KB requirement)
		const maxBodyLimit = 16 * 1024 * 1024
		if r.ContentLength > maxBodyLimit {
			http.Error(w, `{"error": "payload too large, maximum size is 16MB"}`, http.StatusRequestEntityTooLarge)
			return
		}

		// MaxBytesReader rejects oversized bodies sent without Content-Length (chunked
		// transfer) instead of silently truncating them into unparseable JSON.
		bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyLimit))
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				http.Error(w, `{"error": "payload too large, maximum size is 16MB"}`, http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, `{"error": "failed to read body: `+err.Error()+`"}`, http.StatusBadRequest)
			return
		}

		trimmedBody := bytes.TrimSpace(bodyBytes)
		if len(trimmedBody) > 0 {
			// 1. Try parsing structured FilterRequest JSON
			var filterReq FilterRequest
			if err := json.Unmarshal(bodyBytes, &filterReq); err == nil {
				if filterReq.TaskDirective != "" && task == "" {
					task = filterReq.TaskDirective
				}
				if filterReq.Task != "" && task == "" {
					task = filterReq.Task
				}
				if filterReq.Threshold > 0 && filterReq.Threshold < 1.0 && q.Get("threshold") == "" {
					threshold = filterReq.Threshold
				}
				if filterReq.ChunkBy != "" && chunkBy == "" {
					chunkBy = filterReq.ChunkBy
				}
				if filterReq.MaxChunkSize > 0 && q.Get("max_chunk_size") == "" {
					maxChunkBytes = filterReq.MaxChunkSize
				}
				if filterReq.MaxTokens > 0 && q.Get("max_tokens") == "" {
					maxChunkBytes = filterReq.MaxTokens * 4
				}
				if filterReq.IncludeDiscarded {
					includeDiscarded = true
				}

				if filterReq.FromBuffer {
					l := filterReq.Limit
					if l <= 0 {
						l = limit
					}
					chunksToFilter = s.RingBuffer.Query(l, filterReq.SinceMs, filterReq.SinceSeq)
				} else if len(filterReq.Items) > 0 {
					for i, it := range filterReq.Items {
						chunksToFilter = append(chunksToFilter, buffer.Chunk{
							Seq:       uint64(i + 1),
							Timestamp: time.Now().UTC(),
							Origin:    it.Origin,
							Data:      it.ExtractData(),
						})
					}
				} else if streamText := filterReq.ExtractText(); streamText != "" {
					// Dynamic stream partitioning of inline text in JSON payload
					parseOpts := parser.Options{ChunkBy: chunkBy, MaxChunkBytes: maxChunkBytes}
					parsedChunks, pErr := parser.ParseTXT([]byte(streamText), parseOpts)
					if pErr == nil {
						for i, text := range parsedChunks {
							chunksToFilter = append(chunksToFilter, buffer.Chunk{
								Seq:       uint64(i + 1),
								Timestamp: time.Now().UTC(),
								Origin:    "filter_stream",
								Data:      text,
							})
						}
					}
				}
			}

			// 2. Try parsing as JSON array of items: [{"origin": "...", "data": "..."}]
			if len(chunksToFilter) == 0 {
				var arrayReq []SingleIngestRequest
				if err := json.Unmarshal(bodyBytes, &arrayReq); err == nil && len(arrayReq) > 0 {
					for i, it := range arrayReq {
						origin := it.Origin
						if origin == "" {
							origin = "filter_stream"
						}
						chunksToFilter = append(chunksToFilter, buffer.Chunk{
							Seq:       uint64(i + 1),
							Timestamp: time.Now().UTC(),
							Origin:    origin,
							Data:      it.ExtractData(),
						})
					}
				}
			}

			// 3. Fallback: Parse directly as raw document or plain text multi-line stream
			if len(chunksToFilter) == 0 {
				parseOpts := parser.Options{ChunkBy: chunkBy, MaxChunkBytes: maxChunkBytes}
				detectedFormat := parser.DetectFormat("", r.Header.Get("Content-Type"), bodyBytes)
				parsedChunks, pErr := parser.Parse(detectedFormat, bytes.NewReader(bodyBytes), parseOpts)
				if pErr == nil {
					for i, text := range parsedChunks {
						chunksToFilter = append(chunksToFilter, buffer.Chunk{
							Seq:       uint64(i + 1),
							Timestamp: time.Now().UTC(),
							Origin:    "filter_stream",
							Data:      text,
						})
					}
				}
			}
		}
	}

	chunksToFilter = partitionOversizedChunks(chunksToFilter, parser.Options{ChunkBy: chunkBy, MaxChunkBytes: maxChunkBytes})

	result := s.Classifier.FilterChunks(chunksToFilter, task, threshold, includeDiscarded)
	result.LatencyMS = float64(time.Since(startTime).Microseconds()) / 1000.0
	s.recordClassifierMetrics(result.TotalEvaluated, result.SalientCount, result.DiscardedCount)

	writeJSON(w, http.StatusOK, result)
}

// partitionOversizedChunks splits any chunk larger than opts.MaxChunkBytes at line/paragraph
// boundaries so that items, buffer windows and non-text formats are gated per chunk rather than
// as a single monolithic block. Chunks within the limit keep their caller-supplied boundaries.
// Sequence numbers are reassigned when splitting occurs so chunk IDs remain unique.
func partitionOversizedChunks(chunks []buffer.Chunk, opts parser.Options) []buffer.Chunk {
	if opts.MaxChunkBytes <= 0 {
		return chunks
	}
	split := false
	out := make([]buffer.Chunk, 0, len(chunks))
	for _, c := range chunks {
		if len(c.Data) <= opts.MaxChunkBytes {
			out = append(out, c)
			continue
		}
		parts, err := parser.ParseTXT([]byte(c.Data), opts)
		if err != nil || len(parts) == 0 {
			out = append(out, c)
			continue
		}
		split = true
		for _, part := range parts {
			sub := c
			sub.Data = part
			sub.SizeBytes = int64(len(part))
			out = append(out, sub)
		}
	}
	if split {
		for i := range out {
			out[i].Seq = uint64(i + 1)
		}
	}
	return out
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

	s.telemetryMu.RLock()
	cTele := s.telemetry
	s.telemetryMu.RUnlock()

	response := map[string]interface{}{
		"capacity_bytes":        stats.CapacityBytes,
		"used_bytes":            stats.UsedBytes,
		"fill_percent":          stats.FillPercent,
		"current_item_count":    stats.CurrentItemCount,
		"total_ingested_count":  stats.TotalIngestedCount,
		"total_ingested_bytes":  stats.TotalIngestedBytes,
		"dropped_packets":       stats.DroppedPackets,
		"dropped_bytes":         stats.DroppedBytes,
		"ingestion_rate_kbps":   stats.IngestionRateKBps,
		"uptime_seconds":        stats.UptimeSeconds,
		"classifier_telemetry":  cTele,
	}

	writeJSON(w, http.StatusOK, response)
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
