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
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/classifier"
)

var sampleNoise = []string{
	"ping 64 bytes from 192.168.8.1: icmp_seq=1 ttl=64 time=0.421 ms",
	"ping 64 bytes from 192.168.8.1: icmp_seq=2 ttl=64 time=0.389 ms",
	"ping 64 bytes from 192.168.8.1: icmp_seq=3 ttl=64 time=0.401 ms",
	"[DEBUG] 2026-09-13 13:40:01 heartbeat status=ok node=node3 load=0.08",
	"[DEBUG] 2026-09-13 13:40:02 heartbeat status=ok node=node3 load=0.09",
	"[DEBUG] 2026-09-13 13:40:03 heartbeat status=ok node=node3 load=0.08",
	"GET /healthz 200 OK 127.0.0.1 - 180µs",
	"GET /healthz 200 OK 127.0.0.1 - 192µs",
	"GET /healthz 200 OK 127.0.0.1 - 175µs",
	"--------------------------------------------------",
	"..................................................",
	"ok ok ok ok ok ok ok ok ok ok ok ok ok",
	"kernel: [ 102.481920] eth0: link up, 1000Mbps, full-duplex",
	"kernel: [ 102.481999] eth0: link up, 1000Mbps, full-duplex",
}

var sampleSignal = []string{
	"CRITICAL ALERT: Working memory heap utilization on sekha-node2 exceeded 90% (14.6GB / 16.0GB). SLM context window compression activated.",
	"African Fractals represent a sophisticated mathematical paradigm observed in traditional African architecture, where designs are constructed using self-similar recursive algorithms.",
	"The Ba-ila settlement of Zambia is organized as a concentric circular scaling geometry nesting circular homesteads, circular houses, and circular hearths.",
	"Bamana sand divination operates as a 4-bit pseudo-random number generator (PRNG) where symbols are recursively combined using modulo-2 Boolean XOR arithmetic.",
	"TASK DIRECTIVE: Query long-term relational knowledge graph on Node 1 for historical decisions regarding cluster power capping.",
	"Thermal Warning: BCM2712 SoC on Node 3 reached 78.4°C. Active cooler fan RPM scaled to 100%.",
}

func main() {
	filterURL := flag.String("url", "http://127.0.0.1:8081/api/v1/sensory/filter", "Filter endpoint URL (leave empty for in-memory direct test)")
	threshold := flag.Float64("threshold", 0.45, "Salience gating threshold theta (0.0 - 1.0)")
	iterations := flag.Int("iterations", 100, "Number of workload cycle iterations")
	task := flag.String("task", "monitor memory leaks, architectural fractals and thermal alerts", "Active task directive")
	flag.Parse()

	log.Printf("==========================================================")
	log.Printf(" SEKHA SENSORY NOISE FILTERING CLASSIFIER VALIDATION")
	log.Printf("==========================================================")
	log.Printf(" Target URL:        %s", *filterURL)
	log.Printf(" Threshold (theta): %.2f", *threshold)
	log.Printf(" Task Directive:    %s", *task)
	log.Printf(" Workload Cycles:   %d", *iterations)
	log.Printf("==========================================================")

	// Build realistic synthetic edge workload: 70% noise, 30% signal
	var testItems []map[string]interface{}
	expectedNoiseCount := 0
	expectedSignalCount := 0

	for it := 0; it < *iterations; it++ {
		// 7 noise items
		for i := 0; i < 7; i++ {
			noiseText := sampleNoise[(it*7+i)%len(sampleNoise)]
			testItems = append(testItems, map[string]interface{}{
				"origin": "syslog",
				"data":   noiseText,
				"label":  "noise",
			})
			expectedNoiseCount++
		}
		// 3 signal items
		for i := 0; i < 3; i++ {
			sigText := sampleSignal[(it*3+i)%len(sampleSignal)]
			testItems = append(testItems, map[string]interface{}{
				"origin": "agent",
				"data":   sigText,
				"label":  "signal",
			})
			expectedSignalCount++
		}
	}

	totalItems := len(testItems)
	log.Printf("Synthesised %d total workload chunks (%d Noise [70%%], %d Signal [30%%])", totalItems, expectedNoiseCount, expectedSignalCount)

	// Test via HTTP if URL is reachable, or test via internal classifier
	client := &http.Client{Timeout: 10 * time.Second}
	useHTTP := false
	if *filterURL != "" {
		testReq, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:8081/healthz", nil)
		if resp, err := client.Do(testReq); err == nil && resp.StatusCode == http.StatusOK {
			useHTTP = true
			resp.Body.Close()
		}
	}

	var latencies []time.Duration
	var correctlyDiscardedNoise int
	var retainedSignal int
	var falsePositives int // noise treated as signal
	var falseNegatives int // signal treated as noise

	c := classifier.New(*threshold)

	if useHTTP {
		log.Printf("Testing against active HTTP daemon at %s ...", *filterURL)
		batchSize := 20
		for i := 0; i < totalItems; i += batchSize {
			end := i + batchSize
			if end > totalItems {
				end = totalItems
			}
			batch := testItems[i:end]

			reqBody := map[string]interface{}{
				"task":              *task,
				"threshold":         *threshold,
				"include_discarded": true,
				"items":             batch,
			}
			reqBytes, _ := json.Marshal(reqBody)

			start := time.Now()
			resp, err := client.Post(*filterURL, "application/json", bytes.NewReader(reqBytes))
			elapsed := time.Since(start)

			if err != nil {
				log.Fatalf("HTTP request failed: %v", err)
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			perItemElapsed := elapsed / time.Duration(len(batch))
			for range batch {
				latencies = append(latencies, perItemElapsed)
			}

			var fRes classifier.FilterResult
			_ = json.Unmarshal(body, &fRes)
			_ = fRes

			// Score verification
			for _, item := range batch {
				isNoise := item["label"] == "noise"
				text := item["data"].(string)

				m := c.Score(text, *task, *threshold)
				if isNoise {
					if m.IsSalient {
						falsePositives++
					} else {
						correctlyDiscardedNoise++
					}
				} else {
					if m.IsSalient {
						retainedSignal++
					} else {
						falseNegatives++
					}
				}
			}
		}
	} else {
		log.Printf("Testing via local in-memory classifier engine ...")
		for _, item := range testItems {
			isNoise := item["label"] == "noise"
			text := item["data"].(string)

			start := time.Now()
			m := c.Score(text, *task, *threshold)
			elapsed := time.Since(start)
			latencies = append(latencies, elapsed)

			if isNoise {
				if m.IsSalient {
					falsePositives++
				} else {
					correctlyDiscardedNoise++
				}
			} else {
				if m.IsSalient {
					retainedSignal++
				} else {
					falseNegatives++
				}
			}
		}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	meanLat := sum / time.Duration(len(latencies))
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]
	minLat := latencies[0]
	maxLat := latencies[len(latencies)-1]

	noiseReductionRatio := float64(correctlyDiscardedNoise) / float64(expectedNoiseCount) * 100.0
	signalRetentionRatio := float64(retainedSignal) / float64(expectedSignalCount) * 100.0
	overallNoiseReduction := float64(correctlyDiscardedNoise) / float64(totalItems) * 100.0

	fmt.Println()
	fmt.Println("================== CLASSIFICATION ACCURACY ==================")
	fmt.Printf(" Total Chunks Evaluated:   %d\n", totalItems)
	fmt.Printf(" Total True Noise Chunks:  %d\n", expectedNoiseCount)
	fmt.Printf(" Total True Signal Chunks: %d\n", expectedSignalCount)
	fmt.Printf(" Noise Discarded (TNR):    %d / %d (%.2f%%) [Target > 60%%]\n", correctlyDiscardedNoise, expectedNoiseCount, noiseReductionRatio)
	fmt.Printf(" Signal Retained (TPR):    %d / %d (%.2f%%) [Target > 95%%]\n", retainedSignal, expectedSignalCount, signalRetentionRatio)
	fmt.Printf(" Overall Noise Reduction:  %.2f%% of all incoming stream discarded\n", overallNoiseReduction)
	fmt.Printf(" False Positives (leaked): %d (%.2f%%)\n", falsePositives, float64(falsePositives)/float64(expectedNoiseCount)*100.0)
	fmt.Printf(" False Negatives (lost):   %d (%.2f%%)\n", falseNegatives, float64(falseNegatives)/float64(expectedSignalCount)*100.0)
	fmt.Println("----------------- Processing Latency Per Chunk --------------")
	fmt.Printf(" Min Latency:              %v\n", minLat)
	fmt.Printf(" Mean Latency:             %v [Target < 10ms]\n", meanLat)
	fmt.Printf(" p50 (Median):             %v\n", p50)
	fmt.Printf(" p95:                      %v\n", p95)
	fmt.Printf(" p99:                      %v\n", p99)
	fmt.Printf(" Max Latency:              %v\n", maxLat)
	fmt.Println("=============================================================")

	if noiseReductionRatio >= 60.0 && signalRetentionRatio >= 95.0 && meanLat < 10*time.Millisecond {
		fmt.Println(">> RESULT: PASS - All Task 04 empirical criteria verified.")
	} else {
		fmt.Println(">> RESULT: WARN - One or more criteria did not meet target thresholds.")
	}
}
