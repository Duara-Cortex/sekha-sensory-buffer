package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/classifier"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/config"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/embed"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/floor"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/parser"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/version"
)

// ClassifierTelemetry tracks cumulative filtering activity and noise reduction metrics.
type ClassifierTelemetry struct {
	TotalEvaluated      uint64  `json:"total_evaluated"`
	TotalSalient        uint64  `json:"total_salient"`
	TotalDiscarded      uint64  `json:"total_discarded"`
	NoiseReductionRatio float64 `json:"noise_reduction_ratio"`
}

// Server encapsulates the HTTP handler, buffer, embedder and floor rules.
type Server struct {
	Cfg        config.Config
	RingBuffer *buffer.RingBuffer
	Classifier *classifier.Classifier
	Embedder   embed.Embedder
	Floor      floor.Rules
	mux        *http.ServeMux

	telemetryMu sync.RWMutex
	telemetry   ClassifierTelemetry
	ingestTele  IngestTelemetry
}

// NewServer initializes the HTTP multiplexer with registered routes.
func NewServer(cfg config.Config, rb *buffer.RingBuffer, e embed.Embedder) *Server {
	s := &Server{
		Cfg:        cfg,
		RingBuffer: rb,
		Classifier: classifier.New(cfg.LegacyFilterThreshold),
		Embedder:   e,
		Floor: floor.Rules{
			Separator:        cfg.SeparatorPattern,
			Heartbeat:        cfg.HeartbeatPattern,
			HeartbeatExclude: cfg.HeartbeatExclude,
			HeartbeatTypes:   cfg.HeartbeatTypes,
			DedupTypes:       cfg.DedupTypes,
		},
		mux: http.NewServeMux(),
	}
	s.ingestTele.Discarded = make(map[string]uint64)
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("POST /api/v1/sensory/ingest", s.handleIngest)
	s.mux.HandleFunc("GET /api/v1/sensory/drain", s.handleDrain)
	s.mux.HandleFunc("POST /api/v1/sensory/ack", s.handleAck)
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

// SingleIngestRequest represents a single legacy {origin, data} item, as accepted by the
// deprecated /filter endpoint.
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
//
// Deprecated: kept only for sekha-cluster-tool until Task 34 removes it. It does not feed
// memory; use POST /api/v1/sensory/ingest.
func (s *Server) handleFilter(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Deprecation", "true")
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
		chunksToFilter = legacyChunks(s.RingBuffer.Query(limit, sinceMs, sinceSeq))
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
					chunksToFilter = legacyChunks(s.RingBuffer.Query(l, filterReq.SinceMs, filterReq.SinceSeq))
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

func legacyChunks(recs []buffer.Record) []buffer.Chunk {
	out := make([]buffer.Chunk, len(recs))
	for i, r := range recs {
		out[i] = r.Legacy()
	}
	return out
}

// BufferQueryResponse formats output for GET /api/v1/sensory/buffer.
type BufferQueryResponse struct {
	Count  int             `json:"count"`
	Chunks []buffer.Record `json:"chunks"`
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
	iTele := s.ingestTele.snapshot()
	s.telemetryMu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"version":                 version.Version,
		"epoch":                   stats.Epoch,
		"capacity_bytes":          stats.CapacityBytes,
		"used_bytes":              stats.UsedBytes,
		"fill_percent":            stats.FillPercent,
		"current_item_count":      stats.CurrentItemCount,
		"pending_count":           stats.PendingCount,
		"acked_retained_count":    stats.AckedRetainedCount,
		"acked_up_to_seq":         stats.AckedUpToSeq,
		"last_seq":                stats.LastSeq,
		"total_ingested_count":    stats.TotalIngestedCount,
		"total_ingested_bytes":    stats.TotalIngestedBytes,
		"evicted_acked_count":     stats.EvictedAckedCount,
		"backpressure_rejections": stats.BackpressureCount,
		"dropped_packets":         stats.DroppedPackets,
		"dropped_bytes":           stats.DroppedBytes,
		"ingestion_rate_kbps":     stats.IngestionRateKBps,
		"uptime_seconds":          stats.UptimeSeconds,
		"ingest_telemetry":        iTele,
		"classifier_telemetry":    cTele,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := s.RingBuffer.Stats()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":         "ok",
		"version":        version.Version,
		"epoch":          stats.Epoch,
		"uptime_seconds": stats.UptimeSeconds,
		"buffer_fill":    stats.FillPercent,
		"pending_count":  stats.PendingCount,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}
