package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/api"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/config"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/embed"
	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/version"
)

func main() {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		log.Fatalf("[Sekha Sensory Buffer] %v", err)
	}

	// Flags override the environment (useful for local runs); systemd relies on the env file.
	host := flag.String("host", cfg.Host, "Bind address for HTTP server (SEKHA_HOST)")
	port := flag.Int("port", cfg.Port, "Listening port for HTTP server (SEKHA_PORT)")
	capacityMB := flag.Int("capacity-mb", cfg.CapacityMB, "Ring buffer capacity in Megabytes (SEKHA_CAPACITY_MB)")
	flag.Parse()

	if *capacityMB <= 0 {
		log.Fatalf("Invalid capacity-mb: %d (must be > 0)", *capacityMB)
	}

	capacityBytes := int64(*capacityMB) * 1024 * 1024
	rb := buffer.New(capacityBytes)
	log.Printf("[Sekha Sensory Buffer] v%s starting (epoch %s); buffer capacity %d MB, unacknowledged chunks are never evicted", version.Version, rb.Epoch(), *capacityMB)

	var embedder embed.Embedder = embed.None{}
	if cfg.EmbedProvider == config.ProviderOpenAI {
		embedder = embed.NewOpenAI(cfg.EmbedURL, cfg.EmbedAPIKey, cfg.EmbedModel, cfg.EmbedDim, cfg.EmbedBatchSize, cfg.EmbedTimeout)
		log.Printf("[Sekha Sensory Buffer] task scoring via %s (model %s, %d-D); strong threshold %.2f", cfg.EmbedURL, cfg.EmbedModel, cfg.EmbedDim, cfg.StrongThreshold)
	} else {
		log.Printf("[Sekha Sensory Buffer] SEKHA_EMBED_PROVIDER=none: task scoring disabled, no chunk will be marked strong")
	}

	server := api.NewServer(cfg, rb, embedder)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       60 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
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
	log.Printf("[Sekha Sensory Buffer] Received signal %v; initiating graceful shutdown (%d unacknowledged chunks will be lost)...", sig, rb.Pending())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("[Sekha Sensory Buffer] Graceful shutdown encountered error: %v", err)
	} else {
		log.Println("[Sekha Sensory Buffer] Server gracefully stopped.")
	}
}
