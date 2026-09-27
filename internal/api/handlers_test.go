package api

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

	var stats map[string]interface{}
	if err := json.Unmarshal(sRec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}
	if stats["current_item_count"].(float64) != 5 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestHandleFilterDirect(t *testing.T) {
	srv := setupTestServer()

	body := `{
		"task": "detect memory leaks",
		"threshold": 0.45,
		"items": [
			{"origin": "syslog", "data": "ping 64 bytes from 192.168.8.1: icmp_seq=1 ttl=64 time=0.4 ms"},
			{"origin": "syslog", "data": "CRITICAL: memory leak detected in working memory allocator, threshold exceeded at 15GB"}
		]
	}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/filter", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to unmarshal filter response: %v", err)
	}

	if res["total_evaluated"].(float64) != 2 {
		t.Fatalf("expected 2 evaluated, got %v", res["total_evaluated"])
	}
	if res["salient_count"].(float64) != 1 {
		t.Fatalf("expected 1 salient chunk, got %v", res["salient_count"])
	}
	if res["discarded_count"].(float64) != 1 {
		t.Fatalf("expected 1 discarded chunk, got %v", res["discarded_count"])
	}
}

func TestHandleFilterFromBuffer(t *testing.T) {
	srv := setupTestServer()

	// Ingest noise and signal into ring buffer
	srv.RingBuffer.Ingest("syslog", "ping 64 bytes from 192.168.8.1: icmp_seq=1 ttl=64 time=0.4 ms")
	srv.RingBuffer.Ingest("syslog", "heartbeat status=ok")
	srv.RingBuffer.Ingest("agent", "African Fractals represent a sophisticated mathematical paradigm observed in traditional African architecture.")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/filter?from_buffer=true&limit=10&threshold=0.45", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if res["total_evaluated"].(float64) != 3 {
		t.Fatalf("expected 3 evaluated, got %v", res["total_evaluated"])
	}
	if res["salient_count"].(float64) != 1 {
		t.Fatalf("expected 1 salient, got %v", res["salient_count"])
	}
	if res["discarded_count"].(float64) != 2 {
		t.Fatalf("expected 2 discarded, got %v", res["discarded_count"])
	}

	// Verify stats telemetry was updated
	sReq := httptest.NewRequest(http.MethodGet, "/api/v1/sensory/stats", nil)
	sRec := httptest.NewRecorder()
	srv.ServeHTTP(sRec, sReq)

	var sResp map[string]interface{}
	json.Unmarshal(sRec.Body.Bytes(), &sResp)
	cTele := sResp["classifier_telemetry"].(map[string]interface{})
	if cTele["total_evaluated"].(float64) != 3 {
		t.Fatalf("expected classifier telemetry 3 evaluated, got %v", cTele["total_evaluated"])
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

func TestHandleFilter_DynamicStreamChunking_RawText(t *testing.T) {
	srv := setupTestServer()

	rawLog := "ping 64 bytes from 192.168.8.1: icmp_seq=1 ttl=64 time=0.4 ms\n" +
		"[DEBUG] 2026-09-13 13:40:00 heartbeat status=ok\n" +
		"GET /healthz 200 OK 127.0.0.1 - 0.2ms\n" +
		"CRITICAL ALERT: Working memory heap utilization on sekha-node2 exceeded 90% (14.6GB / 16.0GB).\n" +
		"--------------------------------------------------\n" +
		"Host sekha-node2 reported thermal throttling at 82C with memory exhaustion in working memory scratchpad."

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/filter?task=cluster+health", strings.NewReader(rawLog))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	totalChunks := int(res["total_chunks"].(float64))
	noiseDiscarded := int(res["noise_discarded"].(float64))
	reductionRate := res["reduction_rate"].(float64)
	salientCount := int(res["salient_count"].(float64))

	if totalChunks <= 1 {
		t.Fatalf("expected total_chunks > 1, got %d (stream was not dynamically partitioned)", totalChunks)
	}
	if noiseDiscarded == 0 {
		t.Fatalf("expected noise_discarded > 0, got %d", noiseDiscarded)
	}
	if reductionRate <= 0.0 {
		t.Fatalf("expected reduction_rate > 0, got %f", reductionRate)
	}
	if salientCount < 2 {
		t.Fatalf("expected at least 2 salient chunks preserved, got %d", salientCount)
	}

	// Verify chunks list in response contract
	chunksList, ok := res["chunks"].([]interface{})
	if !ok || len(chunksList) != salientCount {
		t.Fatalf("expected 'chunks' array with length %d, got %+v", salientCount, res["chunks"])
	}
}

func TestHandleFilter_DynamicStreamChunking_JSONText(t *testing.T) {
	srv := setupTestServer()

	body := map[string]interface{}{
		"text": "[DEBUG] heartbeat probe seq=1 status=ok\n" +
			"[DEBUG] heartbeat probe seq=2 status=ok\n" +
			"CRITICAL ALERT: BCM2712 SoC on Node 3 reached 82.4°C\n" +
			"ping 64 bytes from 192.168.8.1: icmp_seq=1 ttl=64 time=0.4 ms",
		"task_directive": "thermal alerts",
		"threshold":      0.75,
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/filter", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if int(res["total_chunks"].(float64)) != 4 {
		t.Fatalf("expected 4 total_chunks, got %v", res["total_chunks"])
	}
	if int(res["noise_discarded"].(float64)) != 3 {
		t.Fatalf("expected 3 noise_discarded, got %v", res["noise_discarded"])
	}
	if int(res["salient_count"].(float64)) != 1 {
		t.Fatalf("expected 1 salient_count, got %v", res["salient_count"])
	}
	if res["reduction_rate"].(float64) < 0.70 {
		t.Fatalf("expected reduction_rate >= 0.70, got %v", res["reduction_rate"])
	}
}

func TestHandleFilter_90KB_NoisyLogWithPlantedSignal(t *testing.T) {
	srv := setupTestServer()

	// Build a realistic ~90 KB+ log with ~1,500 lines of noise and 5 planted signal events
	var sb strings.Builder
	plantedSignals := []string{
		"CRITICAL ALERT: Working memory heap utilization on sekha-node2 exceeded 90% (14.6GB / 16.0GB). SLM context window compression activated.",
		"Fatal error: kernel panic on node 3 - unable to handle kernel paging request at virtual address 0000000000000010.",
		"TASK DIRECTIVE: Query long-term relational knowledge graph on Node 1 for historical decisions regarding cluster power capping.",
		"Thermal Warning: BCM2712 SoC on Node 3 reached 78.4°C. Active cooler fan RPM scaled to 100%.",
		"Host sekha-node2 reported thermal throttling at 82C with memory exhaustion in working memory scratchpad.",
	}

	signalInterval := 250
	totalNoiseLines := 1500

	for i := 0; i < totalNoiseLines; i++ {
		if i > 0 && i%signalInterval == 0 && (i/signalInterval)-1 < len(plantedSignals) {
			sig := plantedSignals[(i/signalInterval)-1]
			sb.WriteString(sig + "\n")
		}

		// Noise patterns
		switch i % 5 {
		case 0:
			sb.WriteString(fmt.Sprintf("ping 64 bytes from 192.168.8.1: icmp_seq=%d ttl=64 time=0.421 ms\n", i))
		case 1:
			sb.WriteString(fmt.Sprintf("[DEBUG] 2026-09-13 14:30:%02d heartbeat status=ok node=node3 load=0.08\n", i%60))
		case 2:
			sb.WriteString(fmt.Sprintf("GET /healthz 200 OK 127.0.0.1 - 180µs request_id=%06d\n", i))
		case 3:
			sb.WriteString("kernel: [ 102.481920] eth0: link up, 1000Mbps, full-duplex\n")
		case 4:
			sb.WriteString("--------------------------------------------------------------------------------\n")
		}
	}

	payload := sb.String()
	payloadSizeKB := float64(len(payload)) / 1024.0
	if payloadSizeKB < 80.0 {
		t.Fatalf("payload size too small for 90KB test: %.1f KB", payloadSizeKB)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/filter?task=monitor+thermal+and+memory+leaks", strings.NewReader(payload))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	totalChunks := int(res["total_chunks"].(float64))
	noiseDiscarded := int(res["noise_discarded"].(float64))
	reductionRate := res["reduction_rate"].(float64)
	salientCount := int(res["salient_count"].(float64))

	// Verify empirical gating criteria
	if totalChunks <= 1 {
		t.Fatalf("expected total_chunks > 1, got %d", totalChunks)
	}
	if noiseDiscarded <= 0 {
		t.Fatalf("expected noise_discarded > 0, got %d", noiseDiscarded)
	}
	if reductionRate < 0.90 {
		t.Fatalf("expected reduction_rate >= 0.90 for noisy log, got %f", reductionRate)
	}
	if salientCount < len(plantedSignals) {
		t.Fatalf("expected all %d planted signals to be preserved, but got %d", len(plantedSignals), salientCount)
	}

	// Verify all planted signals are present in surviving chunks
	survivingChunks, ok := res["chunks"].([]interface{})
	if !ok {
		t.Fatalf("missing or invalid 'chunks' in response: %+v", res)
	}

	var survivingTexts []string
	for _, sc := range survivingChunks {
		chunkMap := sc.(map[string]interface{})
		survivingTexts = append(survivingTexts, chunkMap["text"].(string))
	}

	for _, expectedSig := range plantedSignals {
		found := false
		for _, st := range survivingTexts {
			if strings.Contains(st, expectedSig) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("planted signal line lost during filtering: %s", expectedSig)
		}
	}
}

func TestHandleFilter_LargePayload_250KB_and_500KB(t *testing.T) {
	srv := setupTestServer()

	sizes := []int{250 * 1024, 500 * 1024}

	for _, targetBytes := range sizes {
		var sb strings.Builder
		lineIdx := 0
		for sb.Len() < targetBytes {
			sb.WriteString(fmt.Sprintf("[DEBUG] 2026-09-13 14:30:00 node=node3 sensor_event_%05d status=ok metric=ping_stat_val time=0.4ms\n", lineIdx))
			lineIdx++
		}
		// Plant a critical alert
		sb.WriteString("CRITICAL ALERT: High temperature threshold breached on BCM2712 SoC\n")

		payload := sb.String()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/filter?task=thermal+alert", strings.NewReader(payload))
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()

		start := time.Now()
		srv.ServeHTTP(rec, req)
		elapsed := time.Since(start)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for %d KB payload, got %d. Body: %s", targetBytes/1024, rec.Code, rec.Body.String())
		}

		var res map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		totalChunks := int(res["total_chunks"].(float64))
		if totalChunks <= 1 {
			t.Fatalf("expected total_chunks > 1 for %d KB payload, got %d", targetBytes/1024, totalChunks)
		}
		if res["reduction_rate"].(float64) < 0.95 {
			t.Fatalf("expected reduction_rate >= 0.95, got %v", res["reduction_rate"])
		}

		// Latency check: should complete well under 2 seconds even for 500KB
		if elapsed > 2*time.Second {
			t.Fatalf("processing time too slow for %d KB payload: %v", targetBytes/1024, elapsed)
		}
	}
}


func TestHandleFilter_OversizedItemIsPartitioned(t *testing.T) {
	srv := setupTestServer()

	var sb strings.Builder
	for i := 0; sb.Len() < 90*1024; i++ {
		sb.WriteString(fmt.Sprintf("[DEBUG] 2026-09-13 14:30:%02d heartbeat status=ok node=node3 load=0.08\n", i%60))
		if i == 400 {
			sb.WriteString("CRITICAL ALERT: Working memory heap utilization on sekha-node2 exceeded 90% (14.6GB / 16.0GB).\n")
		}
	}

	body, _ := json.Marshal(map[string]interface{}{
		"task":  "monitor memory",
		"items": []map[string]string{{"origin": "syslog", "data": sb.String()}},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/filter", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var res struct {
		TotalChunks    int     `json:"total_chunks"`
		NoiseDiscarded int     `json:"noise_discarded"`
		ReductionRate  float64 `json:"reduction_rate"`
		Chunks         []struct {
			ID     string `json:"id"`
			Text   string `json:"text"`
			Source string `json:"source"`
		} `json:"chunks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if res.TotalChunks <= 1 || res.NoiseDiscarded <= 0 || res.ReductionRate <= 0 {
		t.Fatalf("expected oversized item to be partitioned and gated, got total=%d discarded=%d rate=%f",
			res.TotalChunks, res.NoiseDiscarded, res.ReductionRate)
	}

	found := false
	for _, c := range res.Chunks {
		if c.Source != "syslog" {
			t.Errorf("expected partitioned chunk to keep origin 'syslog', got %q", c.Source)
		}
		if strings.Contains(c.Text, "CRITICAL ALERT") {
			found = true
		}
	}
	if !found {
		t.Fatalf("planted signal was not preserved in surviving chunks")
	}
}

func TestHandleFilter_OversizedChunkedBodyRejected(t *testing.T) {
	srv := setupTestServer()

	// Hide the length so the request looks like a chunked transfer with no Content-Length.
	body := struct{ io.Reader }{bytes.NewReader(bytes.Repeat([]byte("a"), 16*1024*1024+1))}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sensory/filter", body)
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized chunked body, got %d", rec.Code)
	}
}
