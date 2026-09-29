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
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{"SEKHA_STRONG_THRESHOLD", "SEKHA_CHUNK_MAX_BYTES", "SEKHA_FLOOR_SEPARATOR_PATTERN", "SEKHA_FLOOR_DEDUP_TYPES", "SEKHA_EMBED_PROVIDER"} {
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("error does not mention %s: %v", key, err)
		}
	}
}
