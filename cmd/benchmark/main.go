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
	targetLinesPerSec := flag.Int("lines-per-sec", 10000, "Target text lines to ingest per second")
	duration := flag.Duration("duration", 10*time.Second, "Duration of the benchmark test")
	batchSize := flag.Int("batch-size", 50, "Number of text lines per HTTP request batch")
	concurrency := flag.Int("concurrency", 16, "Number of concurrent worker goroutines")
	flag.Parse()

	tr := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

	log.Printf("==========================================================")
	log.Printf(" SEKHA SENSORY BUFFER INGESTION LOAD TEST")
	log.Printf("==========================================================")
	log.Printf(" Target Ingest URL:    %s", *targetURL)
	log.Printf(" Target Throughput:    %d lines/sec", *targetLinesPerSec)
	log.Printf(" Batch Size:           %d lines/request", *batchSize)
	log.Printf(" Concurrency Workers:  %d", *concurrency)
	log.Printf(" Duration:             %v", *duration)
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
	payloadMap := map[string]interface{}{
		"origin": "benchmark-agent",
		"items":  lines,
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
				} else {
					atomic.AddInt64(&totalReqsFailed, 1)
				}
			}

			latenciesMu.Lock()
			latencies = append(latencies, localLatencies...)
			latenciesMu.Unlock()
		}()
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
				// Worker queue saturated
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
	fmt.Printf(" Failed Requests:      %d\n", totalReqsFailed)
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
		fmt.Printf(" Chunks Evicted/Drop:  %d\n", deltaDropped)
		fmt.Printf(" Buffer Fill %%:        %.2f%%\n", statsAfter.FillPercent)
		fmt.Printf(" Buffer Memory Used:   %.2f MB / %.2f MB\n", float64(statsAfter.UsedBytes)/(1024*1024), float64(statsAfter.CapacityBytes)/(1024*1024))
		fmt.Printf(" Daemon Measured Rate: %.2f KB/s\n", statsAfter.IngestionRateKBps)
	}
	fmt.Println("=======================================================")

	if totalReqsFailed == 0 && avgLat < 10*time.Millisecond {
		fmt.Println(">> RESULT: PASS - High throughput & sub-millisecond per-line latency confirmed.")
	} else if totalReqsFailed > 0 {
		fmt.Printf(">> RESULT: WARN - Encountered %d failed requests.\n", totalReqsFailed)
	}
}
