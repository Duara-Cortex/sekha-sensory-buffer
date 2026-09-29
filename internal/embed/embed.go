// Package embed computes the semantic task score: cosine similarity between the MiniLM
// embedding of a chunk and the embedding of the task.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"time"
)

// Embedder turns texts into vectors, one per input, in input order.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// ErrDisabled is returned by the none provider.
var ErrDisabled = errors.New("embedder disabled")

// None is the embedder used when SEKHA_EMBED_PROVIDER=none: nothing is scored.
type None struct{}

// Embed always returns ErrDisabled.
func (None) Embed(context.Context, []string) ([][]float32, error) { return nil, ErrDisabled }

// StatusError is a non-200 reply from the embedding server: the server is up but rejected
// the request (for example an input too long for its batch size).
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("embedding server returned %d: %s", e.Code, e.Body)
}

// OpenAI calls an OpenAI-compatible /v1/embeddings endpoint (e.g. llama-server --embeddings).
type OpenAI struct {
	URL       string
	APIKey    string // sent as a Bearer token when set; never logged
	Model     string
	Dim       int
	BatchSize int
	Client    *http.Client
}

// NewOpenAI builds an OpenAI-compatible client with a per-request timeout.
func NewOpenAI(url, apiKey, model string, dim, batchSize int, timeout time.Duration) *OpenAI {
	return &OpenAI{URL: url, APIKey: apiKey, Model: model, Dim: dim, BatchSize: batchSize, Client: &http.Client{Timeout: timeout}}
}

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed sends texts in batches of BatchSize.
func (o *OpenAI) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += o.BatchSize {
		end := start + o.BatchSize
		if end > len(texts) {
			end = len(texts)
		}
		vecs, err := o.embedBatch(ctx, texts[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, vecs...)
	}
	return out, nil
}

func (o *OpenAI) embedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(embedRequest{Model: o.Model, Input: texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if o.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+o.APIKey)
	}
	resp, err := o.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("reading embedding response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		snippet := raw
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return nil, &StatusError{Code: resp.StatusCode, Body: string(snippet)}
	}
	var er embedResponse
	if err := json.Unmarshal(raw, &er); err != nil {
		return nil, fmt.Errorf("decoding embedding response: %w", err)
	}
	if len(er.Data) != len(texts) {
		return nil, fmt.Errorf("embedding server returned %d vectors for %d inputs", len(er.Data), len(texts))
	}
	sort.Slice(er.Data, func(i, j int) bool { return er.Data[i].Index < er.Data[j].Index })
	vecs := make([][]float32, len(texts))
	for i, d := range er.Data {
		if len(d.Embedding) != o.Dim {
			return nil, fmt.Errorf("embedding has %d dimensions, want %d (check SEKHA_EMBED_MODEL/SEKHA_EMBED_DIM)", len(d.Embedding), o.Dim)
		}
		vecs[i] = d.Embedding
	}
	return vecs, nil
}

// Cosine returns the cosine similarity of a and b (0 if either is a zero vector).
func Cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// Scores embeds the task once and every text, returning one cosine score per text.
// A nil entry means that text could not be scored. If a whole batch fails (for example one
// input is longer than the server's batch size), texts are retried one at a time so a single
// bad input does not leave the rest unscored. err is non-nil only if the task itself could
// not be embedded, in which case nothing is scored.
func Scores(ctx context.Context, e Embedder, task string, texts []string) ([]*float64, error) {
	tv, err := e.Embed(ctx, []string{task})
	if err != nil {
		return nil, err
	}
	scores := make([]*float64, len(texts))
	vecs, err := e.Embed(ctx, texts)
	if err == nil {
		for i, v := range vecs {
			s := round4(Cosine(tv[0], v))
			scores[i] = &s
		}
		return scores, nil
	}
	var se *StatusError
	if !errors.As(err, &se) || len(texts) == 1 {
		return scores, nil // server unreachable or timing out: retrying per text would only multiply the wait
	}
	for i, t := range texts {
		v, err := e.Embed(ctx, []string{t})
		if err != nil {
			continue
		}
		s := round4(Cosine(tv[0], v[0]))
		scores[i] = &s
	}
	return scores, nil
}

func round4(f float64) float64 { return math.Round(f*10000) / 10000 }
