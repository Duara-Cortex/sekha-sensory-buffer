package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) LookupFunc {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestDefaults(t *testing.T) {
	c := Defaults()
	if c.Port != 8081 || c.ChunkMaxBytes != 1024 || c.StrongThreshold != 0.30 || c.EmbedDim != 384 ||
		c.EmbedURL != "http://127.0.0.1:8086/v1/embeddings" || c.EmbedTimeout != 10*time.Second ||
		!c.HeartbeatTypes[TypeLog] || c.HeartbeatTypes[TypeDialogue] || !c.DedupTypes[TypeDialogue] ||
		c.LegacyFilterThreshold != 0.75 || c.EmbedAPIKey != "" {
		c.LegacyFilterThreshold != 0.75 || c.EmbedAPIKey != "" || c.PushURL != "" || c.PushBatch != 256 ||
		c.PushInterval != time.Second || c.PushMaxBackoff != 30*time.Second || c.PushTimeout != 10*time.Second || c.PushMaxBody != 8<<20 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestOverrides(t *testing.T) {
	c, err := Load(env(map[string]string{
		"SEKHA_STRONG_THRESHOLD":      "0.42",
		"SEKHA_CHUNK_MAX_BYTES":       "2048",
		"SEKHA_EMBED_PROVIDER":        "none",
		"SEKHA_FLOOR_DEDUP_TYPES":     "",
		"SEKHA_FLOOR_HEARTBEAT_TYPES": "log, document",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.StrongThreshold != 0.42 || c.ChunkMaxBytes != 2048 || c.EmbedProvider != ProviderNone ||
		len(c.DedupTypes) != 0 || !c.HeartbeatTypes[TypeDocument] {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestInvalidValuesAreErrors(t *testing.T) {
	_, err := Load(env(map[string]string{
		"SEKHA_STRONG_THRESHOLD":        "high",
		"SEKHA_CHUNK_MAX_BYTES":         "10",
		"SEKHA_FLOOR_SEPARATOR_PATTERN": "([",
		"SEKHA_FLOOR_DEDUP_TYPES":       "chat",
		"SEKHA_EMBED_PROVIDER":          "magic",
		"SEKHA_PUSH_URL":                "node2:9000/receive",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{"SEKHA_STRONG_THRESHOLD", "SEKHA_CHUNK_MAX_BYTES", "SEKHA_FLOOR_SEPARATOR_PATTERN", "SEKHA_FLOOR_DEDUP_TYPES", "SEKHA_EMBED_PROVIDER"} {
	for _, key := range []string{"SEKHA_STRONG_THRESHOLD", "SEKHA_CHUNK_MAX_BYTES", "SEKHA_FLOOR_SEPARATOR_PATTERN", "SEKHA_FLOOR_DEDUP_TYPES", "SEKHA_EMBED_PROVIDER", "SEKHA_PUSH_URL"} {
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("error does not mention %s: %v", key, err)
		}
	}
}

func TestPushBackoffMustCoverInterval(t *testing.T) {
	_, err := Load(env(map[string]string{"SEKHA_PUSH_INTERVAL_MS": "5000", "SEKHA_PUSH_MAX_BACKOFF_MS": "1000"}))
	if err == nil || !strings.Contains(err.Error(), "SEKHA_PUSH_MAX_BACKOFF_MS") {
		t.Fatalf("expected a backoff error, got %v", err)
	}
}
