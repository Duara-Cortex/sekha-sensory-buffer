package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type StatsResponse struct {
	CapacityBytes      int64   `json:"capacity_bytes"`
	UsedBytes          int64   `json:"used_bytes"`
	FillPercent        float64 `json:"fill_percent"`
	CurrentItemCount   int     `json:"current_item_count"`
	TotalIngestedCount uint64  `json:"total_ingested_count"`
	TotalIngestedBytes uint64  `json:"total_ingested_bytes"`
	DroppedPackets     uint64  `json:"dropped_packets"`
	DroppedBytes       uint64  `json:"dropped_bytes"`
	IngestionRateKBps  float64 `json:"ingestion_rate_kbps"`
	UptimeSeconds      float64 `json:"uptime_seconds"`
	PendingCount       int     `json:"pending_count"`
	Backpressure       uint64  `json:"backpressure_rejections"`
	Ingest             struct {
		ChunksStrong   uint64 `json:"chunks_strong"`
		ChunksUnscored uint64 `json:"chunks_embedder_unavailable"`
	} `json:"ingest_telemetry"`
}

func fetchStats(client *http.Client, url string) (*StatsResponse, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var stats StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, err
	}
	return &stats, nil
}

func main() {
	targetURL := flag.String("url", "http://127.0.0.1:8081/api/v1/sensory/ingest", "Ingest endpoint URL")
	statsURL := flag.String("stats-url", "http://127.0.0.1:8081/api/v1/sensory/stats", "Stats endpoint URL")
	session := flag.String("session", "benchmark-"+time.Now().UTC().Format("20060102T150405Z"), "Session label sent with every ingest")
	targetLinesPerSec := flag.Int("lines-per-sec", 10000, "Target text lines to ingest per second")
	duration := flag.Duration("duration", 10*time.Second, "Duration of the benchmark test")
	batchSize := flag.Int("batch-size", 50, "Number of text lines per HTTP request batch")
	concurrency := flag.Int("concurrency", 16, "Number of concurrent worker goroutines")
	task := flag.String("task", "", "Task sent with every ingest; when set, every line is MiniLM-scored")
	drain := flag.Bool("drain", false, "Act as the Node 2 consumer: drain and acknowledge during the run")
	timeout := flag.Duration("timeout", 5*time.Second, "Per-request client timeout (raise it for -task runs)")
	flag.Parse()

	tr := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   *timeout,
	}

	log.Printf("==========================================================")
	log.Printf(" SEKHA SENSORY BUFFER INGESTION LOAD TEST")
	log.Printf("==========================================================")
	log.Printf(" Target Ingest URL:    %s", *targetURL)
	log.Printf(" Target Throughput:    %d lines/sec", *targetLinesPerSec)
	log.Printf(" Batch Size:           %d lines/request", *batchSize)
	log.Printf(" Concurrency Workers:  %d", *concurrency)
	log.Printf(" Duration:             %v", *duration)
	log.Printf(" Task (scoring):       %q", *task)
	log.Printf(" Drain + ack:          %v", *drain)
	log.Printf("==========================================================")

	statsBefore, err := fetchStats(client, *statsURL)
	if err != nil {
		log.Printf("Warning: Could not fetch initial stats from %s: %v", *statsURL, err)
	}

	// Prepare reusable batch payload
	var lines []string
	for i := 0; i < *batchSize; i++ {
		lines = append(lines, fmt.Sprintf("sensory_stimulus_event_%04d timestamp=%s payload=\"sample telemetry trace data from node3 sensor input\"", i, time.Now().Format(time.RFC3339Nano)))
	}
	// Unique per line so the floor's duplicate rule does not drop repeated batches' lines
	// within one request (each request is its own input).
	payloadMap := map[string]interface{}{
		"type":    "log",
		"source":  "benchmark-agent",
		"session": *session,
		"text":    strings.Join(lines, "\n"),
	}
	if *task != "" {
		payloadMap["task"] = *task
	}
	payloadBytes, _ := json.Marshal(payloadMap)

	reqsPerSec := *targetLinesPerSec / *batchSize
	if reqsPerSec <= 0 {
		reqsPerSec = 1
	}

	tickerInterval := time.Second / time.Duration(reqsPerSec)
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	stopTime := time.Now().Add(*duration)

	var totalReqsSent int64
	var totalLinesSent int64
	var totalReqsSuccess int64
	var totalReqsFailed int64
	var totalReqsBackpressure int64
	var totalReqsSkipped int64

	var latenciesMu sync.Mutex
	var latencies []time.Duration

	workChan := make(chan struct{}, reqsPerSec*2)

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localLatencies := make([]time.Duration, 0, 1000)

			for range workChan {
				req, err := http.NewRequest(http.MethodPost, *targetURL, bytes.NewReader(payloadBytes))
				if err != nil {
					atomic.AddInt64(&totalReqsFailed, 1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				start := time.Now()
				resp, err := client.Do(req)
				elapsed := time.Since(start)

				if err != nil {
					atomic.AddInt64(&totalReqsFailed, 1)
					continue
				}

				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode == http.StatusCreated {
					atomic.AddInt64(&totalReqsSuccess, 1)
					localLatencies = append(localLatencies, elapsed)
				} else if resp.StatusCode == http.StatusServiceUnavailable {
					atomic.AddInt64(&totalReqsBackpressure, 1)
				} else {
					atomic.AddInt64(&totalReqsFailed, 1)
				}
			}

			latenciesMu.Lock()
			latencies = append(latencies, localLatencies...)
			latenciesMu.Unlock()
		}()
	}

	if *drain {
		go drainLoop(client, strings.TrimSuffix(*targetURL, "/api/v1/sensory/ingest"))
	}

	testStart := time.Now()
generatorLoop:
	for {
		select {
		case <-ticker.C:
			if time.Now().After(stopTime) {
				break generatorLoop
			}
			atomic.AddInt64(&totalReqsSent, 1)
			atomic.AddInt64(&totalLinesSent, int64(*batchSize))
			select {
			case workChan <- struct{}{}:
			default:
				// Worker queue saturated: this request is never sent.
				atomic.AddInt64(&totalReqsSkipped, 1)
			}
		}
	}

	close(workChan)
	wg.Wait()
	totalTestDuration := time.Since(testStart)

	statsAfter, err := fetchStats(client, *statsURL)
	if err != nil {
		log.Printf("Warning: Could not fetch final stats from %s: %v", *statsURL, err)
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var p50, p90, p95, p99, minLat, maxLat, avgLat time.Duration
	if len(latencies) > 0 {
		minLat = latencies[0]
		maxLat = latencies[len(latencies)-1]
		p50 = latencies[len(latencies)*50/100]
		p90 = latencies[len(latencies)*90/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]

		var totalDuration time.Duration
		for _, l := range latencies {
			totalDuration += l
		}
		avgLat = totalDuration / time.Duration(len(latencies))
	}

	actualLinesPerSec := float64(totalReqsSuccess*int64(*batchSize)) / totalTestDuration.Seconds()
	actualReqsPerSec := float64(totalReqsSuccess) / totalTestDuration.Seconds()

	fmt.Println()
	fmt.Println("================== BENCHMARK RESULTS ==================")
	fmt.Printf(" Duration:             %.2f seconds\n", totalTestDuration.Seconds())
	fmt.Printf(" Successful Requests:  %d\n", totalReqsSuccess)
	fmt.Printf(" Back-pressure (503):  %d\n", totalReqsBackpressure)
	fmt.Printf(" Failed Requests:      %d (errors, timeouts, other statuses)\n", totalReqsFailed)
	fmt.Printf(" Skipped (client queue saturated): %d\n", totalReqsSkipped)
	fmt.Printf(" Total Ingested Lines: %d\n", totalReqsSuccess*int64(*batchSize))
	fmt.Printf(" Effective Ingest Rate:%.0f lines/sec (%.1f reqs/sec)\n", actualLinesPerSec, actualReqsPerSec)
	fmt.Println("----------------- Request Latency ---------------------")
	fmt.Printf(" Min Latency:          %v\n", minLat)
	fmt.Printf(" Mean Latency:         %v\n", avgLat)
	fmt.Printf(" p50 (Median):         %v\n", p50)
	fmt.Printf(" p90:                  %v\n", p90)
	fmt.Printf(" p95:                  %v\n", p95)
	fmt.Printf(" p99:                  %v\n", p99)
	fmt.Printf(" Max Latency:          %v\n", maxLat)
	fmt.Println("----------------- Telemetry Verification --------------")
	if statsBefore != nil && statsAfter != nil {
		deltaIngested := statsAfter.TotalIngestedCount - statsBefore.TotalIngestedCount
		deltaDropped := statsAfter.DroppedPackets - statsBefore.DroppedPackets
		fmt.Printf(" Total Chunks Added:   %d\n", deltaIngested)
		fmt.Printf(" Unacked Chunks Lost:  %d (must be 0)\n", deltaDropped)
		fmt.Printf(" Pending (unacked):    %d\n", statsAfter.PendingCount)
		if *task != "" {
			fmt.Printf(" Strong / Unscored:    %d / %d\n",
				statsAfter.Ingest.ChunksStrong-statsBefore.Ingest.ChunksStrong,
				statsAfter.Ingest.ChunksUnscored-statsBefore.Ingest.ChunksUnscored)
		}
		fmt.Printf(" Buffer Fill %%:        %.2f%%\n", statsAfter.FillPercent)
		fmt.Printf(" Buffer Memory Used:   %.2f MB / %.2f MB\n", float64(statsAfter.UsedBytes)/(1024*1024), float64(statsAfter.CapacityBytes)/(1024*1024))
		fmt.Printf(" Daemon Measured Rate: %.2f KB/s\n", statsAfter.IngestionRateKBps)
	}
	fmt.Println("=======================================================")

	switch {
	case totalReqsFailed > 0:
		fmt.Printf(">> RESULT: WARN - %d failed requests (errors or client timeouts).\n", totalReqsFailed)
	case totalReqsBackpressure > 0:
		fmt.Printf(">> RESULT: BACK-PRESSURE - %d requests refused with 503 (buffer full of unacked chunks; nothing lost).\n", totalReqsBackpressure)
	case totalReqsSkipped > 0:
		fmt.Printf(">> RESULT: SATURATED - target rate not reached; %d requests never sent. Lower -lines-per-sec.\n", totalReqsSkipped)
	case *task != "":
		fmt.Printf(">> RESULT: OK (scored run) - %.0f lines/sec sustained with MiniLM scoring, mean latency %v.\n", actualLinesPerSec, avgLat)
	case avgLat < 10*time.Millisecond:
		fmt.Println(">> RESULT: PASS - High throughput & sub-millisecond per-line latency confirmed.")
	default:
		fmt.Printf(">> RESULT: OK - mean latency %v.\n", avgLat)
	}
}

// drainLoop repeatedly drains and acknowledges the buffer, mimicking Node 2.
func drainLoop(client *http.Client, baseURL string) {
	for {
		resp, err := client.Get(baseURL + "/api/v1/sensory/drain?max=4096")
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		var d struct {
			Epoch   string            `json:"epoch"`
			Chunks  []json.RawMessage `json:"chunks"`
			LastSeq uint64            `json:"last_seq"`
		}
		err = json.NewDecoder(resp.Body).Decode(&d)
		resp.Body.Close()
		if err != nil || len(d.Chunks) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		ack, _ := json.Marshal(map[string]interface{}{"epoch": d.Epoch, "up_to_seq": d.LastSeq})
		if resp, err := client.Post(baseURL+"/api/v1/sensory/ack", "application/json", bytes.NewReader(ack)); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}
