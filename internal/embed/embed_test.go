package embed

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeServer answers like llama-server /v1/embeddings with 3-D vectors keyed on content.
func fakeServer(t *testing.T, calls *int32, reject func(string) bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(calls, 1)
		if r.Header.Get("Authorization") != "Bearer k" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req embedRequest
		json.NewDecoder(r.Body).Decode(&req)
		type item struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		}
		var data []item
		for i := len(req.Input) - 1; i >= 0; i-- { // reversed order: client must sort by index
			in := req.Input[i]
			if reject != nil && reject(in) {
				http.Error(w, "input is too large to process", http.StatusInternalServerError)
				return
			}
			v := []float32{0, 0, 1}
			if strings.Contains(in, "retention") {
				v = []float32{1, 0, 0}
			}
			data = append(data, item{Index: i, Embedding: v})
		}
		json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
}

func TestScoresAgainstOpenAICompatibleServer(t *testing.T) {
	var calls int32
	srv := fakeServer(t, &calls, nil)
	defer srv.Close()
	e := NewOpenAI(srv.URL, "k", "all-MiniLM-L6-v2", 3, 2, time.Second)

	scores, err := Scores(context.Background(), e, "what is the retention period", []string{"a", "retention is 90 days", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if *scores[0] != 0 || *scores[1] != 1 || *scores[2] != 0 {
		t.Fatalf("scores %v %v %v", *scores[0], *scores[1], *scores[2])
	}
	if calls != 3 { // 1 task + 2 batches of size 2
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestWrongDimensionIsAnError(t *testing.T) {
	var calls int32
	srv := fakeServer(t, &calls, nil)
	defer srv.Close()
	e := NewOpenAI(srv.URL, "k", "m", 384, 8, time.Second)
	if _, err := Scores(context.Background(), e, "task", []string{"x"}); err == nil {
		t.Fatal("expected dimension error")
	}
}

func TestBadInputOnlyUnscoresThatInput(t *testing.T) {
	var calls int32
	srv := fakeServer(t, &calls, func(s string) bool { return s == "too long" })
	defer srv.Close()
	e := NewOpenAI(srv.URL, "k", "m", 3, 8, time.Second)
	scores, err := Scores(context.Background(), e, "retention", []string{"retention", "too long", "other"})
	if err != nil {
		t.Fatal(err)
	}
	if scores[0] == nil || scores[1] != nil || scores[2] == nil {
		t.Fatalf("want only the rejected input unscored, got %v", scores)
	}
}

func TestTimeoutIsNotRetriedPerItem(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n > 1 { // the task embeds fine; every later call stalls past the client timeout
			time.Sleep(300 * time.Millisecond)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"index": 0, "embedding": []float32{1, 0, 0}}}})
	}))
	defer srv.Close()
	e := NewOpenAI(srv.URL, "", "m", 3, 8, 100*time.Millisecond)
	scores, err := Scores(context.Background(), e, "task", []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range scores {
		if s != nil {
			t.Fatalf("score %d should be nil", i)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2 (task + one batch, no per-item retry)", got)
	}
}

func TestUnreachableServerIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	e := NewOpenAI(url, "", "m", 3, 8, 200*time.Millisecond)
	if _, err := Scores(context.Background(), e, "task", []string{"a"}); err == nil {
		t.Fatal("expected error when embedder is down")
	}
}

func TestCosine(t *testing.T) {
	if c := Cosine([]float32{1, 1}, []float32{2, 2}); math.Abs(c-1) > 1e-9 {
		t.Fatal(c)
	}
	if Cosine([]float32{0, 0}, []float32{1, 0}) != 0 {
		t.Fatal("zero vector must score 0")
	}
}
