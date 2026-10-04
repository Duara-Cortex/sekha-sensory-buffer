package api

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/chunker"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/config"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/embed"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/parser"
)

// Score statuses carried on each record and summarised on the ingest response.
const (
	ScoreScored      = "scored"               // task_score is set
	ScoreNoTask      = "no_task"              // no task given: task_score null, never strong
	ScoreDisabled    = "disabled"             // SEKHA_EMBED_PROVIDER=none
	ScoreUnavailable = "embedder_unavailable" // embedder failed; chunk stored unscored, Node 2 may re-score
	ScorePartial     = "partial"              // response-level only: some chunks scored, some unavailable
)

// TurnInput is one dialogue turn in an ingest request.
type TurnInput struct {
	Speaker string     `json:"speaker"`
	Text    string     `json:"text"`
	TurnID  string     `json:"turn_id,omitempty"`
	TS      *time.Time `json:"ts,omitempty"`
}

// IngestRequest is the JSON body of POST /api/v1/sensory/ingest.
type IngestRequest struct {
	Type    string      `json:"type"`
	Source  string      `json:"source"`
	Session string      `json:"session"`
	Task    string      `json:"task,omitempty"`
	Format  string      `json:"format,omitempty"`  // document only: txt, md, csv, xml, json
	Text    string      `json:"text,omitempty"`    // document or log body; or a single dialogue turn with speaker
	Speaker string      `json:"speaker,omitempty"` // dialogue: speaker of the single turn given in text
	Turns   []TurnInput `json:"turns,omitempty"`   // dialogue turns
}

// IngestResponse is returned on a successful ingest.
type IngestResponse struct {
	MemoryID        string          `json:"memory_id"`
	Epoch           string          `json:"epoch"`
	Type            string          `json:"type"`
	Format          string          `json:"format,omitempty"`
	Source          string          `json:"source"`
	Session         string          `json:"session"`
	Task            string          `json:"task,omitempty"`
	ScoreStatus     string          `json:"score_status"`
	StrongThreshold float64         `json:"strong_threshold"`
	Accepted        int             `json:"accepted"`
	Strong          int             `json:"strong"`
	Weak            int             `json:"weak"`
	Discarded       map[string]int  `json:"discarded"`
	FirstSeq        uint64          `json:"first_seq,omitempty"`
	LastSeq         uint64          `json:"last_seq,omitempty"`
	Chunks          []buffer.Record `json:"chunks,omitempty"`
}

// IngestTelemetry holds cumulative counters for the labelled ingest path.
type IngestTelemetry struct {
	Requests         uint64            `json:"requests"`
	ChunksAccepted   uint64            `json:"chunks_accepted"`
	ChunksStrong     uint64            `json:"chunks_strong"`
	ChunksUnscored   uint64            `json:"chunks_embedder_unavailable"`
	Discarded        map[string]uint64 `json:"discarded"`
	BackpressureHits uint64            `json:"backpressure_rejections"`
}

func (t IngestTelemetry) snapshot() IngestTelemetry {
	d := make(map[string]uint64, len(t.Discarded))
	for k, v := range t.Discarded {
		d[k] = v
	}
	t.Discarded = d
	return t
}

// apiError is a client-facing error with a stable code and, for label errors, the field.
type apiError struct {
	status  int
	Code    string `json:"error"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

func (e *apiError) Error() string { return e.Message }

func badRequest(code, field, msg string) *apiError {
	return &apiError{status: http.StatusBadRequest, Code: code, Field: field, Message: msg}
}

func writeError(w http.ResponseWriter, e *apiError) {
	writeJSON(w, e.status, e)
}

// ingestInput is a validated, labelled request ready for chunking.
type ingestInput struct {
	req    IngestRequest
	format parser.Format
	data   []byte // document or log body
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	in, apiErr := s.readIngest(w, r)
	if apiErr != nil {
		writeError(w, apiErr)
		return
	}
	resp, apiErr := s.ingest(r, in)
	if apiErr != nil {
		if apiErr.status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", strconv.Itoa(s.Cfg.BackpressureRetryS))
		}
		writeError(w, apiErr)
		return
	}
	if r.URL.Query().Get("summary") == "true" {
		resp.Chunks = nil
	}
	writeJSON(w, http.StatusCreated, resp)
}

// readIngest decodes a JSON, multipart or raw-body request and validates its labels.
// It never infers a missing label.
func (s *Server) readIngest(w http.ResponseWriter, r *http.Request) (*ingestInput, *apiError) {
	r.Body = http.MaxBytesReader(w, r.Body, s.Cfg.MaxBodyBytes)
	contentType := r.Header.Get("Content-Type")
	cleanType := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	tooLarge := &apiError{status: http.StatusRequestEntityTooLarge, Code: "payload_too_large",
		Message: fmt.Sprintf("request body exceeds SEKHA_MAX_BODY_BYTES (%d bytes)", s.Cfg.MaxBodyBytes)}

	in := &ingestInput{}
	switch {
	case cleanType == "application/json":
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in.req); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				return nil, tooLarge
			}
			return nil, badRequest("invalid_json", "", "could not decode ingest request: "+err.Error())
		}
		in.data = []byte(in.req.Text)
		if in.req.Type == config.TypeDocument && in.req.Format == string(parser.FormatPDF) {
			return nil, badRequest("invalid_label", "format", "pdf cannot be sent inside JSON; upload it as multipart/form-data or a raw application/pdf body")
		}

	case strings.HasPrefix(cleanType, "multipart/form-data"):
		if err := r.ParseMultipartForm(s.Cfg.MaxBodyBytes); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				return nil, tooLarge
			}
			return nil, badRequest("invalid_multipart", "", "failed to parse multipart form: "+err.Error())
		}
		in.req = labelsFrom(r.FormValue)
		file, header, err := r.FormFile("file")
		if err != nil {
			return nil, badRequest("missing_file", "file", "multipart ingest needs a 'file' field")
		}
		defer file.Close()
		if in.data, err = io.ReadAll(file); err != nil {
			return nil, badRequest("invalid_multipart", "file", "failed to read uploaded file")
		}
		if in.req.Format == "" {
			in.req.Format = string(parser.DetectFormat(header.Filename, header.Header.Get("Content-Type"), in.data))
		}

	default:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				return nil, tooLarge
			}
			return nil, badRequest("invalid_body", "", "failed to read body")
		}
		in.req = labelsFrom(r.URL.Query().Get)
		in.data = body
		if in.req.Format == "" {
			in.req.Format = string(parser.DetectFormat("", contentType, body))
		}
	}

	if apiErr := validateLabels(&in.req, cleanType == "application/json"); apiErr != nil {
		return nil, apiErr
	}
	if in.req.Type == config.TypeDocument {
		if in.req.Format == "" {
			in.req.Format = string(parser.DetectFormat("", "", in.data))
		}
		switch parser.Format(in.req.Format) {
		case parser.FormatTXT, parser.FormatMD, parser.FormatCSV, parser.FormatXML, parser.FormatJSON, parser.FormatPDF:
		default:
			return nil, badRequest("invalid_label", "format", fmt.Sprintf("unknown document format %q (want txt, md, csv, xml, json or pdf)", in.req.Format))
		}
		in.format = parser.Format(in.req.Format)
	} else {
		in.req.Format = ""
	}
	if in.req.Type != config.TypeDialogue && len(bytes.TrimSpace(in.data)) == 0 {
		return nil, badRequest("empty_input", "text", "no content to ingest")
	}
	return in, nil
}

// labelsFrom reads labels from form or query values (multipart and raw-body requests).
func labelsFrom(get func(string) string) IngestRequest {
	return IngestRequest{
		Type:    strings.TrimSpace(get("type")),
		Source:  strings.TrimSpace(get("source")),
		Session: strings.TrimSpace(get("session")),
		Task:    strings.TrimSpace(get("task")),
		Format:  strings.TrimSpace(get("format")),
	}
}

func validateLabels(req *IngestRequest, isJSON bool) *apiError {
	req.Type = strings.TrimSpace(req.Type)
	req.Source = strings.TrimSpace(req.Source)
	req.Session = strings.TrimSpace(req.Session)
	req.Task = strings.TrimSpace(req.Task)

	if req.Type == "" {
		return badRequest("missing_label", "type", "'type' is required: one of dialogue, document, log")
	}
	if !config.ValidType(req.Type) {
		return badRequest("invalid_label", "type", fmt.Sprintf("unknown type %q: want dialogue, document or log", req.Type))
	}
	if req.Source == "" {
		return badRequest("missing_label", "source", "'source' is required: where this input came from")
	}
	if req.Session == "" {
		return badRequest("missing_label", "session", "'session' is required: the session this input belongs to")
	}
	if req.Type != config.TypeDialogue {
		if len(req.Turns) > 0 || req.Speaker != "" {
			return badRequest("invalid_label", "turns", "'turns' and 'speaker' are only valid for type dialogue")
		}
		return nil
	}

	if !isJSON {
		return badRequest("invalid_label", "type", "dialogue must be sent as application/json with 'turns'")
	}
	if len(req.Turns) > 0 && (req.Text != "" || req.Speaker != "") {
		return badRequest("invalid_label", "turns", "send either 'turns' or a single 'speaker' + 'text', not both")
	}
	if len(req.Turns) == 0 {
		if strings.TrimSpace(req.Speaker) == "" {
			return badRequest("missing_label", "speaker", "dialogue needs 'turns' (each with a speaker), or 'speaker' + 'text' for one turn")
		}
		req.Turns = []TurnInput{{Speaker: req.Speaker, Text: req.Text}}
		req.Speaker, req.Text = "", ""
	}
	seen := make(map[string]int)
	for i := range req.Turns {
		t := &req.Turns[i]
		t.Speaker = strings.TrimSpace(t.Speaker)
		if t.Speaker == "" {
			return badRequest("missing_label", fmt.Sprintf("turns[%d].speaker", i), fmt.Sprintf("turn %d has no speaker", i))
		}
		if t.TurnID != "" {
			if j, dup := seen[t.TurnID]; dup {
				return badRequest("invalid_label", fmt.Sprintf("turns[%d].turn_id", i), fmt.Sprintf("turn_id %q repeats turns[%d]", t.TurnID, j))
			}
			seen[t.TurnID] = i
		}
	}
	return nil
}

// ingest chunks, floors, scores and stores one labelled input as one memory.
func (s *Server) ingest(r *http.Request, in *ingestInput) (*IngestResponse, *apiError) {
	req := in.req
	var cands []chunker.Candidate
	switch req.Type {
	case config.TypeDialogue:
		turns := make([]chunker.Turn, len(req.Turns))
		for i, t := range req.Turns {
			turns[i] = chunker.Turn{Speaker: t.Speaker, Text: t.Text, TurnID: t.TurnID}
			if t.TS != nil {
				turns[i].TS = t.TS.UTC()
			}
		}
		cands = chunker.Dialogue(turns, s.Cfg.ChunkMaxBytes)
	case config.TypeLog:
		cands = chunker.Log(string(in.data), s.Cfg.ChunkMaxBytes)
	case config.TypeDocument:
		var err error
		cands, err = chunker.Document(in.data, in.format, s.Cfg.ChunkMaxBytes)
		if err != nil {
			return nil, badRequest("unparseable_document", "format", fmt.Sprintf("failed to parse %s document: %v", in.format, err))
		}
	}

	kept, discarded := s.Floor.Apply(req.Type, cands)

	scores, status := s.score(r, req.Task, kept)

	memoryID := newMemoryID()
	recs := make([]buffer.Record, len(kept))
	strong := 0
	unscored := 0
	for i, c := range kept {
		rec := buffer.Record{
			ID:       chunker.ID(c.Text),
			MemoryID: memoryID,
			Text:     c.Text,
			Type:     req.Type,
			Format:   req.Format,
			Source:   req.Source,
			Session:  req.Session,
			Speaker:  c.Speaker,
			TurnID:   c.TurnID,
			ParentID: c.ParentID,
			Part:     c.Part,
			Parts:    c.Parts,
			Heading:  c.Heading,
			TS:       c.TS,
			Task:     req.Task,
		}
		switch {
		case status != ScoreScored:
			rec.ScoreStatus = status
		case scores[i] == nil:
			rec.ScoreStatus = ScoreUnavailable
			unscored++
		default:
			rec.ScoreStatus = ScoreScored
			rec.TaskScore = scores[i]
			rec.Strong = *scores[i] >= s.Cfg.StrongThreshold
		}
		if rec.Strong {
			strong++
		}
		recs[i] = rec
	}
	if status == ScoreUnavailable {
		unscored = len(recs)
	}
	if status == ScoreScored && unscored > 0 {
		status = ScorePartial
		if unscored == len(recs) {
			status = ScoreUnavailable
		}
	}

	stored, err := s.RingBuffer.Append(recs)
	if err != nil {
		s.telemetryMu.Lock()
		s.ingestTele.BackpressureHits++
		s.telemetryMu.Unlock()
		var full *buffer.FullError
		var tooLarge *buffer.TooLargeError
		switch {
		case errors.As(err, &full):
			return nil, &apiError{status: http.StatusServiceUnavailable, Code: "buffer_full",
				Message: err.Error() + "; nothing from this request was stored, retry after Node 2 drains and acknowledges"}
		case errors.As(err, &tooLarge):
			return nil, &apiError{status: http.StatusRequestEntityTooLarge, Code: "exceeds_buffer_capacity",
				Message: err.Error() + "; split the input into smaller requests"}
		default:
			return nil, &apiError{status: http.StatusInternalServerError, Code: "buffer_error", Message: err.Error()}
		}
	}

	s.telemetryMu.Lock()
	s.ingestTele.Requests++
	s.ingestTele.ChunksAccepted += uint64(len(stored))
	s.ingestTele.ChunksStrong += uint64(strong)
	s.ingestTele.ChunksUnscored += uint64(unscored)
	for reason, n := range discarded {
		s.ingestTele.Discarded[reason] += uint64(n)
	}
	s.telemetryMu.Unlock()

	resp := &IngestResponse{
		MemoryID:        memoryID,
		Epoch:           s.RingBuffer.Epoch(),
		Type:            req.Type,
		Format:          req.Format,
		Source:          req.Source,
		Session:         req.Session,
		Task:            req.Task,
		ScoreStatus:     status,
		StrongThreshold: s.Cfg.StrongThreshold,
		Accepted:        len(stored),
		Strong:          strong,
		Weak:            len(stored) - strong,
		Discarded:       discarded,
		Chunks:          stored,
	}
	if len(stored) > 0 {
		resp.FirstSeq = stored[0].Seq
		resp.LastSeq = stored[len(stored)-1].Seq
	}
	return resp, nil
}

// score returns per-candidate task scores and the overall status. When status is not
// ScoreScored, scores is nil. Embedder failure never blocks storage.
func (s *Server) score(r *http.Request, task string, cands []chunker.Candidate) ([]*float64, string) {
	if task == "" {
		return nil, ScoreNoTask
	}
	if _, off := s.Embedder.(embed.None); off || s.Embedder == nil {
		return nil, ScoreDisabled
	}
	if len(cands) == 0 {
		return nil, ScoreScored
	}
	texts := make([]string, len(cands))
	for i, c := range cands {
		texts[i] = c.Text
		if c.Heading != "" {
			texts[i] = c.Heading + "\n\n" + c.Text // heading context improves the score; stored text is unchanged
		}
	}
	scores, err := embed.Scores(r.Context(), s.Embedder, task, texts)
	if err != nil {
		log.Printf("[Sekha Sensory Buffer] embedder unavailable, storing %d chunks unscored: %v", len(cands), err)
		return nil, ScoreUnavailable
	}
	return scores, ScoreScored
}

// newMemoryID returns a UUIDv7: time-ordered, so memory ids sort by ingest time.
func newMemoryID() string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixMilli())<<16)
	if _, err := rand.Read(b[6:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// handleDrain implements GET /api/v1/sensory/drain: the oldest unacknowledged records.
func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	if s.rejectInPushMode(w) {
		return
	}
	q := r.URL.Query()
	max := s.Cfg.DrainMaxDefault
	if v := q.Get("max"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, badRequest("invalid_param", "max", "max must be a positive integer"))
			return
		}
		max = n
	}
	if max > s.Cfg.DrainMaxLimit {
		max = s.Cfg.DrainMaxLimit
	}
	var after uint64
	if v := q.Get("after_seq"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, badRequest("invalid_param", "after_seq", "after_seq must be a non-negative integer"))
			return
		}
		after = n
	}
	writeJSON(w, http.StatusOK, s.RingBuffer.Drain(after, max))
}

// rejectInPushMode refuses /drain and /ack while the push loop delivers to Node 2: a
// second consumer pulling and acking would take chunks the push loop has not sent.
func (s *Server) rejectInPushMode(w http.ResponseWriter) bool {
	if s.Cfg.PushURL == "" {
		return false
	}
	writeError(w, &apiError{status: http.StatusConflict, Code: "push_mode",
		Message: "Node 3 pushes chunks to Node 2 (SEKHA_PUSH_URL is set); /drain and /ack are disabled"})
	return true
}

// AckRequest is the body of POST /api/v1/sensory/ack.
type AckRequest struct {
	Epoch   string  `json:"epoch"`
	UpToSeq *uint64 `json:"up_to_seq"`
}

// handleAck implements POST /api/v1/sensory/ack: cumulative acknowledgement.
func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	if s.rejectInPushMode(w) {
		return
	}
	var req AckRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, badRequest("invalid_json", "", "could not decode ack request: "+err.Error()))
		return
	}
	if req.Epoch == "" {
		writeError(w, badRequest("missing_field", "epoch", "'epoch' is required (from the drain response)"))
		return
	}
	if req.UpToSeq == nil {
		writeError(w, badRequest("missing_field", "up_to_seq", "'up_to_seq' is required"))
		return
	}
	n, err := s.RingBuffer.Ack(req.Epoch, *req.UpToSeq)
	switch {
	case errors.Is(err, buffer.ErrEpochMismatch):
		writeError(w, &apiError{status: http.StatusConflict, Code: "epoch_mismatch", Field: "epoch", Message: err.Error()})
		return
	case errors.Is(err, buffer.ErrAckAhead):
		writeError(w, badRequest("ack_ahead", "up_to_seq", err.Error()))
		return
	case err != nil:
		writeError(w, &apiError{status: http.StatusInternalServerError, Code: "buffer_error", Message: err.Error()})
		return
	}
	stats := s.RingBuffer.Stats()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"epoch":           stats.Epoch,
		"acked":           n,
		"acked_up_to_seq": stats.AckedUpToSeq,
		"pending":         stats.PendingCount,
	})
}
