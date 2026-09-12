package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/api"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
)

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func main() {
	defaultHost := getEnv("SEKHA_HOST", "0.0.0.0")
	defaultPort := getEnvInt("SEKHA_PORT", 8081)
	defaultCapacityMB := getEnvInt("SEKHA_CAPACITY_MB", 64)

	host := flag.String("host", defaultHost, "Bind address for HTTP server")
	port := flag.Int("port", defaultPort, "Listening port for HTTP server")
	capacityMB := flag.Int("capacity-mb", defaultCapacityMB, "Ring buffer capacity in Megabytes")
	flag.Parse()

	if *capacityMB <= 0 {
		log.Fatalf("Invalid capacity-mb: %d (must be > 0)", *capacityMB)
	}

	capacityBytes := int64(*capacityMB) * 1024 * 1024
	log.Printf("[Sekha Sensory Buffer] Initialising circular ring buffer (Capacity: %d MB / %d bytes)", *capacityMB, capacityBytes)

	rb := buffer.New(capacityBytes)
	server := api.NewServer(rb)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Channel to listen for interrupt or termination signals
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[Sekha Sensory Buffer] Daemon running and listening on http://%s", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Sekha Sensory Buffer] HTTP server error: %v", err)
		}
	}()

	sig := <-stopChan
	log.Printf("[Sekha Sensory Buffer] Received signal %v; initiating graceful shutdown...", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("[Sekha Sensory Buffer] Graceful shutdown encountered error: %v", err)
	} else {
		log.Println("[Sekha Sensory Buffer] Server gracefully stopped.")
	}
}
