package api

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/chunker"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/config"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/embed"
)

// topicEmbedder is a deterministic offline stand-in for MiniLM: dimension 0 is the
// "retention" topic, dimension 1 everything else. "benchmark answer" leans slightly toward
// retention so it scores low but non-zero (relevant but weak).
type topicEmbedder struct{ fail bool }

func (e topicEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	if e.fail {
		return nil, errors.New("connection refused")
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		switch {
		case strings.Contains(t, "retention"):
			out[i] = []float32{1, 0}
		case strings.Contains(t, "benchmark answer"):
			out[i] = []float32{0.2, 1}
		default:
			out[i] = []float32{0, 1}
		}
	}
	return out, nil
}

func newTestServer(capacity int64, e embed.Embedder) *Server {
	return NewServer(config.Defaults(), buffer.New(capacity), e)
}

func do(t *testing.T, srv *Server, method, path, contentType string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func ingestJSON(t *testing.T, srv *Server, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	return do(t, srv, http.MethodPost, "/api/v1/sensory/ingest", "application/json", b)
}

func decodeIngest(t *testing.T, rec *httptest.ResponseRecorder) IngestResponse {
	t.Helper()
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder, status int) apiError {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("expected %d, got %d: %s", status, rec.Code, rec.Body.String())
	}
	var e apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatal(err)
	}
	return e
}

// Acceptance 6: labels are required and never guessed.
func TestIngestRejectsMissingLabels(t *testing.T) {
	srv := newTestServer(1<<20, embed.None{})
	cases := []struct {
		name  string
		body  map[string]any
		code  string
		field string
	}{
		{"no type", map[string]any{"source": "s", "session": "x", "text": "hello"}, "missing_label", "type"},
		{"bad type", map[string]any{"type": "chat", "source": "s", "session": "x", "text": "hello"}, "invalid_label", "type"},
		{"no source", map[string]any{"type": "log", "session": "x", "text": "hello"}, "missing_label", "source"},
		{"no session", map[string]any{"type": "log", "source": "s", "text": "hello"}, "missing_label", "session"},
		{"dialogue no speaker", map[string]any{"type": "dialogue", "source": "s", "session": "x", "turns": []map[string]string{{"speaker": "u", "text": "a"}, {"text": "b"}}}, "missing_label", "turns[1].speaker"},
		{"dialogue single no speaker", map[string]any{"type": "dialogue", "source": "s", "session": "x", "text": "hi"}, "missing_label", "speaker"},
		{"legacy origin/data", map[string]any{"origin": "web", "data": "hello"}, "invalid_json", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := decodeError(t, ingestJSON(t, srv, tc.body), http.StatusBadRequest)
			if e.Code != tc.code || e.Field != tc.field || e.Message == "" {
				t.Fatalf("got %+v", e)
			}
		})
	}
	// Raw body without labels in the query string.
	e := decodeError(t, do(t, srv, http.MethodPost, "/api/v1/sensory/ingest?source=s&session=x", "text/plain", []byte("a line")), http.StatusBadRequest)
	if e.Field != "type" {
		t.Fatalf("got %+v", e)
	}
	if srv.RingBuffer.Stats().CurrentItemCount != 0 {
		t.Fatal("rejected requests must store nothing")
	}
}

// Acceptance 1: 20 turns, one of 3,000 words -> 19 turn chunks + paragraph chunks.
func TestIngestDialogueChunking(t *testing.T) {
	srv := newTestServer(8<<20, embed.None{})
	var paras []string
	for p := 0; p < 30; p++ {
		words := make([]string, 100)
		for w := range words {
			words[w] = fmt.Sprintf("word%d", w)
		}
		paras = append(paras, fmt.Sprintf("Paragraph %d: %s.", p, strings.Join(words, " ")))
	}
	long := strings.Join(paras, "\n\n")
	var turns []map[string]string
	for i := 0; i < 20; i++ {
		sp := []string{"user", "assistant"}[i%2]
		text := fmt.Sprintf("Turn %d from %s.", i+1, sp)
		if i == 11 {
			text = long
		}
		turns = append(turns, map[string]string{"speaker": sp, "text": text})
	}
	resp := decodeIngest(t, ingestJSON(t, srv, map[string]any{"type": "dialogue", "source": "chat-ui", "session": "s-1", "turns": turns}))

	whole, parts := 0, 0
	for i, c := range resp.Chunks {
		if c.MemoryID != resp.MemoryID || c.Type != "dialogue" || c.Source != "chat-ui" || c.Session != "s-1" || c.ID != chunker.ID(c.Text) {
			t.Fatalf("chunk %d missing labels: %+v", i, c)
		}
		if c.ParentID == "" {
			whole++
			wantSp := []string{"user", "assistant"}
			n := 0
			fmt.Sscanf(c.TurnID, "t%d", &n)
			if c.Speaker != wantSp[(n-1)%2] || c.Text != fmt.Sprintf("Turn %d from %s.", n, c.Speaker) {
				t.Fatalf("turn chunk mislabelled: %+v", c)
			}
			continue
		}
		parts++
		if c.TurnID != "t12" || c.Speaker != "assistant" || c.ParentID != chunker.ID(long) || c.Parts != 30 || c.Part != parts {
			t.Fatalf("paragraph chunk %d lineage wrong: %+v", parts, c)
		}
	}
	if whole != 19 || parts != 30 || resp.Accepted != 49 {
		t.Fatalf("whole=%d parts=%d accepted=%d", whole, parts, resp.Accepted)
	}
	// Order is preserved: seqs contiguous, the long turn's parts sit between t11 and t13.
	for i := 1; i < len(resp.Chunks); i++ {
		if resp.Chunks[i].Seq != resp.Chunks[i-1].Seq+1 {
			t.Fatal("seqs not contiguous")
		}
	}
	if resp.Chunks[10].TurnID != "t11" || resp.Chunks[11].TurnID != "t12" || resp.Chunks[41].TurnID != "t13" {
		t.Fatal("turn order not preserved")
	}
}

func TestIngestDialogueDedupFollowsTheQuestion(t *testing.T) {
	srv := newTestServer(1<<20, embed.None{})
	resp := decodeIngest(t, ingestJSON(t, srv, map[string]any{"type": "dialogue", "source": "chat", "session": "s", "turns": []map[string]string{
		{"speaker": "assistant", "text": "Shall I delete the logs?"},
		{"speaker": "user", "text": "yes"},
		{"speaker": "assistant", "text": "Shall I restart the service?"},
		{"speaker": "user", "text": "yes"},
		{"speaker": "assistant", "text": "Shall I delete the logs?"},
		{"speaker": "user", "text": "yes"},
	}}))
	// t6 repeats the answer to a question already answered (t1/t2) -> one duplicate.
	// t5 re-asks after a different turn, so it is kept.
	if resp.Accepted != 5 || resp.Discarded["duplicate"] != 1 || resp.Chunks[3].TurnID != "t4" || resp.Chunks[4].TurnID != "t5" {
		t.Fatalf("accepted=%d discarded=%v", resp.Accepted, resp.Discarded)
	}
}

// Acceptance 2 end-to-end on the raw-body log path.
func TestIngestLogFloorCounts(t *testing.T) {
	srv := newTestServer(1<<20, embed.None{})
	body := "INFO a\n\n-----\nheartbeat seq=1 status=ok\nINFO a\nWARN b\n=====\n64 bytes from 10.0.0.1: icmp_seq=4 ttl=64 time=1 ms\n\nERROR c\n"
	resp := decodeIngest(t, do(t, srv, http.MethodPost, "/api/v1/sensory/ingest?type=log&source=syslog&session=boot-7", "text/plain", []byte(body)))
	want := map[string]int{"blank": 2, "separator": 2, "heartbeat": 2, "duplicate": 1}
	for k, v := range want {
		if resp.Discarded[k] != v {
			t.Fatalf("discarded %v, want %v", resp.Discarded, want)
		}
	}
	var texts []string
	for _, c := range resp.Chunks {
		texts = append(texts, c.Text)
	}
	if strings.Join(texts, "|") != "INFO a|WARN b|ERROR c" {
		t.Fatalf("kept %q", texts)
	}
}

// Acceptance 3 (offline): a relevant-but-weak chunk is stored and delivered, never dropped.
func TestIngestNoScoreGating(t *testing.T) {
	srv := newTestServer(1<<20, topicEmbedder{})
	resp := decodeIngest(t, ingestJSON(t, srv, map[string]any{
		"type": "document", "source": "bench", "session": "run-1", "task": "retention",
		"text": "The benchmark answer is 42.\n\nLunch is at noon.",
	}))
	if resp.Accepted != 2 || resp.Strong != 0 || resp.Weak != 2 {
		t.Fatalf("resp %+v", resp)
	}
	weak := resp.Chunks[0]
	if weak.TaskScore == nil || *weak.TaskScore <= 0 || *weak.TaskScore >= srv.Cfg.StrongThreshold || weak.Strong {
		t.Fatalf("expected a low but non-zero score, got %+v", weak)
	}
	d := srv.RingBuffer.Drain(0, 10)
	if len(d.Chunks) != 2 || d.Chunks[0].Text != "The benchmark answer is 42." {
		t.Fatalf("weak chunk not in buffer: %+v", d.Chunks)
	}
}

func TestIngestRoutingAndOutputRecord(t *testing.T) {
	srv := newTestServer(1<<20, topicEmbedder{})
	resp := decodeIngest(t, ingestJSON(t, srv, map[string]any{
		"type": "document", "format": "md", "source": "policy.md", "session": "s-9",
		"task": "what is the log retention period?",
		"text": "# Policy\n\n## Logs\n\nThe retention period is 90 days.\n\nThe office has plants.\n",
	}))
	if resp.ScoreStatus != ScoreScored || resp.Task != "what is the log retention period?" || resp.StrongThreshold != 0.30 {
		t.Fatalf("resp %+v", resp)
	}
	var strong, weak *buffer.Record
	for i := range resp.Chunks {
		c := &resp.Chunks[i]
		if c.Text == "The retention period is 90 days." {
			strong = c
		}
		if c.Text == "The office has plants." {
			weak = c
		}
		if c.Task != resp.Task || c.ScoreStatus != ScoreScored || c.TaskScore == nil {
			t.Fatalf("record missing task/score: %+v", c)
		}
	}
	if strong == nil || weak == nil {
		t.Fatal("chunks missing")
	}
	if !strong.Strong || *strong.TaskScore <= *weak.TaskScore || weak.Strong {
		t.Fatalf("routing wrong: strong=%+v weak=%+v", strong, weak)
	}
	if strong.Heading != "# Policy > ## Logs" || strong.Format != "md" || strong.TS.IsZero() || strong.Seq == 0 {
		t.Fatalf("record fields: %+v", strong)
	}

	// Every documented field is present in the JSON record, task_score even when null.
	raw := do(t, srv, http.MethodGet, "/api/v1/sensory/drain?max=1", "", nil)
	var d struct {
		Chunks []map[string]any `json:"chunks"`
	}
	json.Unmarshal(raw.Body.Bytes(), &d)
	for _, k := range []string{"id", "memory_id", "text", "type", "source", "session", "seq", "ts", "task", "task_score", "strong", "score_status"} {
		if _, ok := d.Chunks[0][k]; !ok {
			t.Fatalf("record JSON missing %q: %v", k, d.Chunks[0])
		}
	}
}

func TestIngestWithoutTaskNothingStrong(t *testing.T) {
	srv := newTestServer(1<<20, topicEmbedder{})
	resp := decodeIngest(t, ingestJSON(t, srv, map[string]any{"type": "log", "source": "s", "session": "x", "text": "retention is 90 days"}))
	c := resp.Chunks[0]
	if resp.ScoreStatus != ScoreNoTask || c.TaskScore != nil || c.Strong || resp.Strong != 0 {
		t.Fatalf("got %+v", resp)
	}
	b, _ := json.Marshal(c)
	if !strings.Contains(string(b), `"task_score":null`) {
		t.Fatalf("task_score must serialise as null: %s", b)
	}
}

func TestIngestStoresEvenWhenEmbedderDown(t *testing.T) {
	srv := newTestServer(1<<20, topicEmbedder{fail: true})
	resp := decodeIngest(t, ingestJSON(t, srv, map[string]any{"type": "log", "source": "s", "session": "x", "task": "retention", "text": "retention is 90 days\nother"}))
	if resp.Accepted != 2 || resp.ScoreStatus != ScoreUnavailable || resp.Chunks[0].ScoreStatus != ScoreUnavailable || resp.Chunks[0].TaskScore != nil {
		t.Fatalf("got %+v", resp)
	}
	if srv.RingBuffer.Pending() != 2 {
		t.Fatal("chunks must be stored even without a score")
	}
}

// Acceptance 5: past capacity with nothing acknowledged -> explicit back-pressure, no loss.
func TestIngestBackPressureAndAck(t *testing.T) {
	srv := newTestServer(4096, embed.None{})
	body := map[string]any{"type": "log", "source": "s", "session": "x"}
	accepted := 0
	var lastErr *httptest.ResponseRecorder
	for i := 0; i < 100; i++ {
		body["text"] = fmt.Sprintf("line %d %s", i, strings.Repeat("x", 200))
		rec := ingestJSON(t, srv, body)
		if rec.Code != http.StatusCreated {
			lastErr = rec
			break
		}
		accepted++
	}
	if lastErr == nil {
		t.Fatal("buffer never pushed back")
	}
	e := decodeError(t, lastErr, http.StatusServiceUnavailable)
	if e.Code != "buffer_full" || lastErr.Header().Get("Retry-After") != "5" {
		t.Fatalf("got %+v retry-after=%q", e, lastErr.Header().Get("Retry-After"))
	}
	// Keep pushing: still rejected, nothing evicted.
	for i := 0; i < 5; i++ {
		decodeError(t, ingestJSON(t, srv, body), http.StatusServiceUnavailable)
	}
	st := srv.RingBuffer.Stats()
	if st.PendingCount != accepted || st.EvictedAckedCount != 0 || st.DroppedPackets != 0 {
		t.Fatalf("accepted=%d stats=%+v", accepted, st)
	}

	// Node 2 drains and acknowledges; ingest works again and nothing unacked was lost.
	var d buffer.DrainResult
	json.Unmarshal(do(t, srv, http.MethodGet, "/api/v1/sensory/drain", "", nil).Body.Bytes(), &d)
	if len(d.Chunks) != accepted || d.Chunks[0].Text[:6] != "line 0" {
		t.Fatalf("drain returned %d of %d", len(d.Chunks), accepted)
	}
	ack, _ := json.Marshal(map[string]any{"epoch": d.Epoch, "up_to_seq": d.LastSeq})
	if rec := do(t, srv, http.MethodPost, "/api/v1/sensory/ack", "application/json", ack); rec.Code != http.StatusOK {
		t.Fatalf("ack: %d %s", rec.Code, rec.Body.String())
	}
	decodeIngest(t, ingestJSON(t, srv, body))
}

func TestDrainAckContractErrors(t *testing.T) {
	srv := newTestServer(1<<20, embed.None{})
	decodeIngest(t, ingestJSON(t, srv, map[string]any{"type": "log", "source": "s", "session": "x", "text": "a\nb\nc"}))
	epoch := srv.RingBuffer.Epoch()

	cases := []struct {
		body   string
		status int
		code   string
	}{
		{`{"epoch":"old","up_to_seq":1}`, http.StatusConflict, "epoch_mismatch"},
		{fmt.Sprintf(`{"epoch":%q,"up_to_seq":99}`, epoch), http.StatusBadRequest, "ack_ahead"},
		{fmt.Sprintf(`{"epoch":%q}`, epoch), http.StatusBadRequest, "missing_field"},
		{`{"up_to_seq":1}`, http.StatusBadRequest, "missing_field"},
	}
	for _, tc := range cases {
		e := decodeError(t, do(t, srv, http.MethodPost, "/api/v1/sensory/ack", "application/json", []byte(tc.body)), tc.status)
		if e.Code != tc.code {
			t.Fatalf("%s: got %+v", tc.body, e)
		}
	}
	var d buffer.DrainResult
	json.Unmarshal(do(t, srv, http.MethodGet, "/api/v1/sensory/drain?max=2", "", nil).Body.Bytes(), &d)
	if len(d.Chunks) != 2 || !d.More || d.Pending != 3 {
		t.Fatalf("drain %+v", d)
	}
	json.Unmarshal(do(t, srv, http.MethodGet, "/api/v1/sensory/drain?after_seq=2", "", nil).Body.Bytes(), &d)
	if len(d.Chunks) != 1 || d.Chunks[0].Text != "c" {
		t.Fatalf("paged drain %+v", d)
	}
	decodeError(t, do(t, srv, http.MethodGet, "/api/v1/sensory/drain?max=0", "", nil), http.StatusBadRequest)
}

func TestIngestDocumentFormats(t *testing.T) {
	srv := newTestServer(1<<20, embed.None{})

	// Markdown via raw body.
	md := decodeIngest(t, do(t, srv, http.MethodPost, "/api/v1/sensory/ingest?type=document&source=notes&session=x", "text/markdown", []byte("# A\n\nalpha\n\n## B\n\nbeta\n")))
	if md.Format != "md" || md.Accepted != 4 || md.Chunks[3].Heading != "# A > ## B" {
		t.Fatalf("md %+v", md)
	}
	// CSV via raw body.
	csv := decodeIngest(t, do(t, srv, http.MethodPost, "/api/v1/sensory/ingest?type=document&source=metrics&session=x", "text/csv", []byte("host,status\nnode3,online\nnode2,online\n")))
	if csv.Format != "csv" || csv.Accepted != 2 || !strings.Contains(csv.Chunks[0].Text, "node3") {
		t.Fatalf("csv %+v", csv)
	}
	// XML via raw body.
	xml := decodeIngest(t, do(t, srv, http.MethodPost, "/api/v1/sensory/ingest?type=document&source=feed&session=x", "application/xml",
		[]byte(`<?xml version="1.0"?><rss><channel><item><title>Kernel update</title></item><item><title>Disk alert</title></item></channel></rss>`)))
	if xml.Format != "xml" || xml.Accepted < 1 {
		t.Fatalf("xml %+v", xml)
	}
	// JSON document.
	js := decodeIngest(t, ingestJSON(t, srv, map[string]any{"type": "document", "format": "json", "source": "api", "session": "x", "text": `[{"a":1},{"b":2}]`}))
	if js.Format != "json" || js.Accepted != 2 {
		t.Fatalf("json %+v", js)
	}
	// PDF via raw body.
	var z bytes.Buffer
	zw := zlib.NewWriter(&z)
	zw.Write([]byte("BT\n(Sekha Whitepaper 01 Abstract) Tj\nET\n"))
	zw.Close()
	var pdf bytes.Buffer
	pdf.WriteString("%PDF-1.5\n1 0 obj\n<< /Length 50 /Filter /FlateDecode >>\nstream\n")
	pdf.Write(z.Bytes())
	pdf.WriteString("\nendstream\nendobj\n%%EOF")
	p := decodeIngest(t, do(t, srv, http.MethodPost, "/api/v1/sensory/ingest?type=document&source=paper&session=x", "application/pdf", pdf.Bytes()))
	if p.Format != "pdf" || p.Accepted < 1 || !strings.Contains(p.Chunks[0].Text, "Whitepaper") {
		t.Fatalf("pdf %+v", p)
	}
	// Multipart upload with labels as form fields.
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	w.WriteField("type", "document")
	w.WriteField("source", "upload")
	w.WriteField("session", "x")
	part, _ := w.CreateFormFile("file", "report.md")
	part.Write([]byte("# R\n\nbody text\n"))
	w.Close()
	mp := decodeIngest(t, do(t, srv, http.MethodPost, "/api/v1/sensory/ingest", w.FormDataContentType(), b.Bytes()))
	if mp.Format != "md" || mp.Accepted != 2 || mp.Chunks[1].Heading != "# R" {
		t.Fatalf("multipart %+v", mp)
	}
	// PDF inside JSON is refused with a clear message.
	decodeError(t, ingestJSON(t, srv, map[string]any{"type": "document", "format": "pdf", "source": "s", "session": "x", "text": "%PDF"}), http.StatusBadRequest)
}

func TestIngestSummaryAndSingleTurn(t *testing.T) {
	srv := newTestServer(1<<20, embed.None{})
	rec := do(t, srv, http.MethodPost, "/api/v1/sensory/ingest?summary=true", "application/json",
		[]byte(`{"type":"dialogue","source":"chat","session":"s","speaker":"user","text":"hello there"}`))
	resp := decodeIngest(t, rec)
	if resp.Chunks != nil || resp.Accepted != 1 || resp.MemoryID == "" || resp.FirstSeq != 1 {
		t.Fatalf("got %+v", resp)
	}
	q := srv.RingBuffer.Query(1, 0, 0)
	if q[0].Speaker != "user" || q[0].TurnID != "t1" {
		t.Fatalf("single turn %+v", q[0])
	}
}

func TestIngestPayloadTooLarge(t *testing.T) {
	cfg := config.Defaults()
	cfg.MaxBodyBytes = 64
	srv := NewServer(cfg, buffer.New(1<<20), embed.None{})
	body := []byte(`{"type":"log","source":"s","session":"x","text":"` + strings.Repeat("a", 200) + `"}`)
	decodeError(t, do(t, srv, http.MethodPost, "/api/v1/sensory/ingest", "application/json", body), http.StatusRequestEntityTooLarge)
}

func TestMemoryIDsAreUniqueUUIDv7(t *testing.T) {
	a, b := newMemoryID(), newMemoryID()
	if a == b || len(a) != 36 || a[14] != '7' {
		t.Fatalf("%s %s", a, b)
	}
}
