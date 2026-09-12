package api

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
)

func setupTestServer() *Server {
	rb := buffer.New(10 * 1024 * 1024) // 10MB
	return NewServer(rb)
}

func TestHandleIngestSingleJSON(t *testing.T) {
	srv := setupTestServer()

	body := `{"origin": "web", "data": "incoming raw log"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/ingest", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.IngestedCount != 1 || resp.FirstSeq != 1 {
		t.Fatalf("unexpected ingest response: %+v", resp)
	}
}

func TestHandleIngestBatchJSON(t *testing.T) {
	srv := setupTestServer()

	body := `{"origin": "telemetry", "items": ["event 1", "event 2", "event 3"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/ingest", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if resp.IngestedCount != 3 || resp.LastSeq != 3 {
		t.Fatalf("unexpected batch response: %+v", resp)
	}
}

func TestHandleIngestMarkdown(t *testing.T) {
	srv := setupTestServer()

	mdBody := `# Heading 1
Content 1

# Heading 2
Content 2`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/ingest?origin=notes", bytes.NewBufferString(mdBody))
	req.Header.Set("Content-Type", "text/markdown")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.IngestedCount != 2 {
		t.Fatalf("expected 2 sections ingested from Markdown, got %d", resp.IngestedCount)
	}
}

func TestHandleIngestCSV(t *testing.T) {
	srv := setupTestServer()

	csvBody := `sensor,temp,voltage
pi5_node3,42.5,5.1
pi5_node2,48.0,5.0`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/ingest?origin=metrics", bytes.NewBufferString(csvBody))
	req.Header.Set("Content-Type", "text/csv")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.IngestedCount != 2 {
		t.Fatalf("expected 2 CSV row chunks ingested, got %d", resp.IngestedCount)
	}
}

func TestHandleIngestXML(t *testing.T) {
	srv := setupTestServer()

	xmlBody := `<telemetry>
  <event><id>1</id><msg>fan spin up</msg></event>
  <event><id>2</id><msg>temp stable</msg></event>
</telemetry>`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/ingest?origin=sysxml", bytes.NewBufferString(xmlBody))
	req.Header.Set("Content-Type", "application/xml")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.IngestedCount != 2 {
		t.Fatalf("expected 2 XML chunks ingested, got %d", resp.IngestedCount)
	}
}

func TestHandleIngestPDF(t *testing.T) {
	srv := setupTestServer()

	// Minimal synthetic PDF
	streamText := "BT\n(Sekha Whitepaper 01 Abstract) Tj\nET\n"
	var zlibBuf bytes.Buffer
	zw := zlib.NewWriter(&zlibBuf)
	zw.Write([]byte(streamText))
	zw.Close()

	var pdfBuf bytes.Buffer
	pdfBuf.WriteString("%PDF-1.5\n")
	pdfBuf.WriteString("1 0 obj\n<< /Length 50 /Filter /FlateDecode >>\nstream\n")
	pdfBuf.Write(zlibBuf.Bytes())
	pdfBuf.WriteString("\nendstream\nendobj\n%%EOF")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/ingest?origin=whitepaper", &pdfBuf)
	req.Header.Set("Content-Type", "application/pdf")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.IngestedCount < 1 {
		t.Fatalf("expected at least 1 PDF chunk ingested, got %d", resp.IngestedCount)
	}
}

func TestHandleMultipartFileUpload(t *testing.T) {
	srv := setupTestServer()

	var b bytes.Buffer
	w := multipart.NewWriter(&b)

	part, err := w.CreateFormFile("file", "telemetry_report.csv")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	part.Write([]byte("host,ip,status\nnode3,192.168.8.183,online\nnode2,192.168.8.175,online\n"))
	w.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/ingest", &b)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var resp IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.IngestedCount != 2 {
		t.Fatalf("expected 2 rows from multipart CSV, got %d", resp.IngestedCount)
	}

	// Verify query returns them with origin telemetry_report.csv
	qReq := httptest.NewRequest(http.MethodGet, "/api/v1/sensory/buffer?limit=2", nil)
	qRec := httptest.NewRecorder()
	srv.ServeHTTP(qRec, qReq)

	var qResp BufferQueryResponse
	json.Unmarshal(qRec.Body.Bytes(), &qResp)
	if len(qResp.Chunks) != 2 || qResp.Chunks[0].Origin != "telemetry_report.csv" {
		t.Fatalf("unexpected query output: %+v", qResp)
	}
}

func TestHandleBufferQueryAndStats(t *testing.T) {
	srv := setupTestServer()

	// Ingest 5 items
	for i := 0; i < 5; i++ {
		srv.RingBuffer.Ingest("agent", "stimulus")
	}

	// Query top 2
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sensory/buffer?limit=2", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var qResp BufferQueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &qResp); err != nil {
		t.Fatalf("failed to decode query response: %v", err)
	}
	if qResp.Count != 2 || len(qResp.Chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", qResp.Count)
	}

	// Query Stats
	sReq := httptest.NewRequest(http.MethodGet, "/api/v1/sensory/stats", nil)
	sRec := httptest.NewRecorder()
	srv.ServeHTTP(sRec, sReq)

	if sRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", sRec.Code)
	}

	var stats buffer.BufferStats
	if err := json.Unmarshal(sRec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}
	if stats.CurrentItemCount != 5 || stats.TotalIngestedCount != 5 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestHandleHealth(t *testing.T) {
	srv := setupTestServer()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
