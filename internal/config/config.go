// Package config loads every tunable of the sensory buffer from the environment.
// Each key has a documented default (see .env.example and README). Invalid values
// are a startup error rather than a silent fallback.
package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Input types accepted by the labelled ingest API.
const (
	TypeDialogue = "dialogue"
	TypeDocument = "document"
	TypeLog      = "log"
)

// Embedder providers.
const (
	ProviderOpenAI = "openai"
	ProviderNone   = "none"
)

// Default values. These are the documented defaults in .env.example and the README.
const (
	DefaultHost                  = "0.0.0.0"
	DefaultPort                  = 8081
	DefaultCapacityMB            = 64
	DefaultMaxBodyBytes          = 16 * 1024 * 1024
	DefaultHTTPWriteTimeoutS     = 120
	DefaultChunkMaxBytes         = 1024
	DefaultStrongThreshold       = 0.30
	DefaultEmbedProvider         = ProviderOpenAI
	DefaultEmbedURL              = "http://127.0.0.1:8086/v1/embeddings"
	DefaultEmbedModel            = "all-MiniLM-L6-v2"
	DefaultEmbedDim              = 384
	DefaultEmbedTimeoutMS        = 10000
	DefaultEmbedBatchSize        = 32
	DefaultSeparatorPattern      = `^\s*(?:-{3,}|={3,}|_{3,}|\*{3,}|~{3,}|#{3,}|\+{3,})\s*$`
	DefaultHeartbeatPattern      = `(?i)^\s*(?:ping|pong|ping\s+\S+\s+\(\S+\).*|\d+\s+bytes\s+from\s+.*\bicmp_seq=\d+.*|.*\bheartbeat\b.*\b(?:ok|alive|healthy)\s*)$`
	DefaultHeartbeatExclude      = `(?i)\b(?:error|fail|failed|failure|miss|missed|missing|timeout|timed\s+out|lost|unreachable|not|down|critical|fatal|warn|warning)\b`
	DefaultHeartbeatTypes        = TypeLog
	DefaultDedupTypes            = TypeLog + "," + TypeDocument + "," + TypeDialogue
	DefaultDrainMaxDefault       = 256
	DefaultDrainMaxLimit         = 4096
	DefaultBackpressureRetryS    = 5
	DefaultLegacyFilterThreshold = 0.75
	DefaultPushBatch             = 256
	DefaultPushIntervalMS        = 1000
	DefaultPushMaxBackoffMS      = 30000
	DefaultPushTimeoutMS         = 10000
)

// Config is the fully resolved service configuration.
type Config struct {
	Host             string
	Port             int
	CapacityMB       int
	MaxBodyBytes     int64
	HTTPWriteTimeout time.Duration

	ChunkMaxBytes   int
	StrongThreshold float64

	EmbedProvider  string
	EmbedURL       string
	EmbedAPIKey    string
	EmbedModel     string
	EmbedDim       int
	EmbedTimeout   time.Duration
	EmbedBatchSize int

	SeparatorPattern *regexp.Regexp
	HeartbeatPattern *regexp.Regexp
	HeartbeatExclude *regexp.Regexp
	HeartbeatTypes   map[string]bool
	DedupTypes       map[string]bool

	DrainMaxDefault    int
	DrainMaxLimit      int
	BackpressureRetryS int

	LegacyFilterThreshold float64

	// PushURL is Node 2's receive endpoint. Empty disables the push loop (pull mode).
	PushURL        string
	PushAPIKey     string
	PushBatch      int
	PushInterval   time.Duration
	PushMaxBackoff time.Duration
	PushTimeout    time.Duration
}

// LookupFunc matches os.LookupEnv.
type LookupFunc func(key string) (string, bool)

type loader struct {
	lookup LookupFunc
	errs   []string
}

func (l *loader) raw(key string) (string, bool) {
	v, ok := l.lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return "", false
	}
	return strings.TrimSpace(v), true
}

func (l *loader) str(key, def string) string {
	if v, ok := l.raw(key); ok {
		return v
	}
	return def
}

func (l *loader) intVal(key string, def, min int) int {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil || i < min {
		l.errs = append(l.errs, fmt.Sprintf("%s=%q: must be an integer >= %d", key, v, min))
		return def
	}
	return i
}

func (l *loader) floatVal(key string, def, min, max float64) float64 {
	v, ok := l.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < min || f > max {
		l.errs = append(l.errs, fmt.Sprintf("%s=%q: must be a number in [%g, %g]", key, v, min, max))
		return def
	}
	return f
}

func (l *loader) regex(key, def string) *regexp.Regexp {
	pattern := l.str(key, def)
	re, err := regexp.Compile(pattern)
	if err != nil {
		l.errs = append(l.errs, fmt.Sprintf("%s: invalid regular expression: %v", key, err))
		return regexp.MustCompile(def)
	}
	return re
}

func (l *loader) typeSet(key, def string) map[string]bool {
	v, ok := l.lookup(key)
	if !ok {
		v = def
	}
	set := make(map[string]bool)
	for _, t := range strings.Split(v, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if !ValidType(t) {
			l.errs = append(l.errs, fmt.Sprintf("%s: unknown type %q (want dialogue, document or log)", key, t))
			continue
		}
		set[t] = true
	}
	return set
}

// ValidType reports whether t is one of the accepted input types.
func ValidType(t string) bool {
	return t == TypeDialogue || t == TypeDocument || t == TypeLog
}

// Load resolves the configuration from lookup (normally os.LookupEnv).
func Load(lookup LookupFunc) (Config, error) {
	l := &loader{lookup: lookup}
	c := Config{
		Host:             l.str("SEKHA_HOST", DefaultHost),
		Port:             l.intVal("SEKHA_PORT", DefaultPort, 1),
		CapacityMB:       l.intVal("SEKHA_CAPACITY_MB", DefaultCapacityMB, 1),
		MaxBodyBytes:     int64(l.intVal("SEKHA_MAX_BODY_BYTES", DefaultMaxBodyBytes, 1)),
		HTTPWriteTimeout: time.Duration(l.intVal("SEKHA_HTTP_WRITE_TIMEOUT_S", DefaultHTTPWriteTimeoutS, 1)) * time.Second,

		ChunkMaxBytes:   l.intVal("SEKHA_CHUNK_MAX_BYTES", DefaultChunkMaxBytes, 64),
		StrongThreshold: l.floatVal("SEKHA_STRONG_THRESHOLD", DefaultStrongThreshold, -1, 1),

		EmbedProvider:  l.str("SEKHA_EMBED_PROVIDER", DefaultEmbedProvider),
		EmbedURL:       l.str("SEKHA_EMBED_URL", DefaultEmbedURL),
		EmbedAPIKey:    l.str("SEKHA_EMBED_API_KEY", ""),
		EmbedModel:     l.str("SEKHA_EMBED_MODEL", DefaultEmbedModel),
		EmbedDim:       l.intVal("SEKHA_EMBED_DIM", DefaultEmbedDim, 1),
		EmbedTimeout:   time.Duration(l.intVal("SEKHA_EMBED_TIMEOUT_MS", DefaultEmbedTimeoutMS, 1)) * time.Millisecond,
		EmbedBatchSize: l.intVal("SEKHA_EMBED_BATCH_SIZE", DefaultEmbedBatchSize, 1),

		SeparatorPattern: l.regex("SEKHA_FLOOR_SEPARATOR_PATTERN", DefaultSeparatorPattern),
		HeartbeatPattern: l.regex("SEKHA_FLOOR_HEARTBEAT_PATTERN", DefaultHeartbeatPattern),
		HeartbeatExclude: l.regex("SEKHA_FLOOR_HEARTBEAT_EXCLUDE_PATTERN", DefaultHeartbeatExclude),
		HeartbeatTypes:   l.typeSet("SEKHA_FLOOR_HEARTBEAT_TYPES", DefaultHeartbeatTypes),
		DedupTypes:       l.typeSet("SEKHA_FLOOR_DEDUP_TYPES", DefaultDedupTypes),

		DrainMaxDefault:    l.intVal("SEKHA_DRAIN_MAX_DEFAULT", DefaultDrainMaxDefault, 1),
		DrainMaxLimit:      l.intVal("SEKHA_DRAIN_MAX_LIMIT", DefaultDrainMaxLimit, 1),
		BackpressureRetryS: l.intVal("SEKHA_BACKPRESSURE_RETRY_AFTER_S", DefaultBackpressureRetryS, 0),

		LegacyFilterThreshold: l.floatVal("SEKHA_FILTER_THRESHOLD", DefaultLegacyFilterThreshold, 0.01, 0.99),

		PushURL:        l.str("SEKHA_PUSH_URL", ""),
		PushAPIKey:     l.str("SEKHA_PUSH_API_KEY", ""),
		PushBatch:      l.intVal("SEKHA_PUSH_BATCH", DefaultPushBatch, 1),
		PushInterval:   time.Duration(l.intVal("SEKHA_PUSH_INTERVAL_MS", DefaultPushIntervalMS, 10)) * time.Millisecond,
		PushMaxBackoff: time.Duration(l.intVal("SEKHA_PUSH_MAX_BACKOFF_MS", DefaultPushMaxBackoffMS, 10)) * time.Millisecond,
		PushTimeout:    time.Duration(l.intVal("SEKHA_PUSH_TIMEOUT_MS", DefaultPushTimeoutMS, 1)) * time.Millisecond,
	}

	if c.EmbedProvider != ProviderOpenAI && c.EmbedProvider != ProviderNone {
		l.errs = append(l.errs, fmt.Sprintf("SEKHA_EMBED_PROVIDER=%q: want %q or %q", c.EmbedProvider, ProviderOpenAI, ProviderNone))
	}
	if c.DrainMaxDefault > c.DrainMaxLimit {
		l.errs = append(l.errs, fmt.Sprintf("SEKHA_DRAIN_MAX_DEFAULT (%d) exceeds SEKHA_DRAIN_MAX_LIMIT (%d)", c.DrainMaxDefault, c.DrainMaxLimit))
	}

	if c.PushURL != "" {
		if u, err := url.Parse(c.PushURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			l.errs = append(l.errs, fmt.Sprintf("SEKHA_PUSH_URL=%q: want an http:// or https:// URL", c.PushURL))
		}
	}
	if c.PushMaxBackoff < c.PushInterval {
		l.errs = append(l.errs, fmt.Sprintf("SEKHA_PUSH_MAX_BACKOFF_MS (%v) is less than SEKHA_PUSH_INTERVAL_MS (%v)", c.PushMaxBackoff, c.PushInterval))
	}

	if len(l.errs) > 0 {
		return c, fmt.Errorf("invalid configuration:\n  %s", strings.Join(l.errs, "\n  "))
	}
	return c, nil
}

// Defaults returns the configuration with every key at its default value.
func Defaults() Config {
	c, err := Load(func(string) (string, bool) { return "", false })
	if err != nil {
		panic(err)
	}
	return c
}
