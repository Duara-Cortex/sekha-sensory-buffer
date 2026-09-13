package classifier

import (
	"math"
	"regexp"
	"strings"
	"unicode"

	"github.com/Duara-Cortex/sekha-sensory-buffer/internal/buffer"
)

var (
	numberRegex = regexp.MustCompile(`\b\d+(\.\d+)?(ms|s|%|MB|GB|KB|kbps|Mbps|°C|C|F|V|A|Hz|MHz|GHz)?\b`)
	keyValueRegex = regexp.MustCompile(`[a-zA-Z0-9_\-\.]+\s*[:=]\s*[^\s]+`)
	identifierRegex = regexp.MustCompile(`\b[A-Z0-9_]{3,}\b|\b[A-Z][a-z]+[A-Z][a-z]+\b|/[a-zA-Z0-9_\-\./]+`)
)

// Common English and log stopwords that carry low standalone semantic information
var standardStopwords = map[string]struct{}{
	"a": {}, "an": {}, "the": {}, "and": {}, "or": {}, "but": {}, "if": {}, "then": {}, "else": {},
	"when": {}, "at": {}, "from": {}, "by": {}, "on": {}, "off": {}, "for": {}, "in": {}, "out": {},
	"over": {}, "to": {}, "into": {}, "with": {}, "about": {}, "against": {}, "between": {},
	"through": {}, "during": {}, "before": {}, "after": {}, "above": {}, "below": {}, "up": {}, "down": {},
	"is": {}, "are": {}, "was": {}, "were": {}, "be": {}, "been": {}, "being": {}, "have": {}, "has": {},
	"had": {}, "do": {}, "does": {}, "did": {}, "will": {}, "would": {}, "shall": {}, "should": {},
	"can": {}, "could": {}, "may": {}, "might": {}, "must": {}, "it": {}, "its": {}, "this": {}, "that": {},
	"these": {}, "those": {}, "i": {}, "you": {}, "he": {}, "she": {}, "we": {}, "they": {},
}

// SalienceMetrics encapsulates information density and gating scores for a text chunk.
type SalienceMetrics struct {
	Entropy            float64 `json:"entropy"`
	EntropyNorm        float64 `json:"entropy_norm"`
	LexicalDensity     float64 `json:"lexical_density"`
	EntityDensity      float64 `json:"entity_density"`
	TaskRelevance      float64 `json:"task_relevance"`
	RepetitionPenalty  float64 `json:"repetition_penalty"`
	SalienceScore      float64 `json:"salience_score"`
	IsSalient          bool    `json:"is_salient"`
	DiscardReason      string  `json:"discard_reason,omitempty"`
}

// FilteredChunk wraps a buffer.Chunk with its salience metrics.
type FilteredChunk struct {
	Chunk   buffer.Chunk    `json:"chunk"`
	Metrics SalienceMetrics `json:"metrics"`
}

// FilterResult represents the outcome of filtering a batch or window of text chunks.
type FilterResult struct {
	TotalEvaluated      int             `json:"total_evaluated"`
	SalientCount        int             `json:"salient_count"`
	DiscardedCount      int             `json:"discarded_count"`
	NoiseReductionRatio float64         `json:"noise_reduction_ratio"`
	Threshold           float64         `json:"threshold"`
	TaskDirective       string          `json:"task_directive,omitempty"`
	SalientChunks       []FilteredChunk `json:"salient_chunks"`
	DiscardedChunks     []FilteredChunk `json:"discarded_chunks,omitempty"`
}

// Classifier implements CPU-efficient information-density and semantic salience gating.
type Classifier struct {
	DefaultThreshold float64
}

// New creates a new Classifier with a given default threshold (e.g. 0.45).
func New(defaultThreshold float64) *Classifier {
	if defaultThreshold <= 0.0 || defaultThreshold >= 1.0 {
		defaultThreshold = 0.45
	}
	return &Classifier{
		DefaultThreshold: defaultThreshold,
	}
}

// CalculateEntropy computes normalized Shannon character entropy [0.0, 1.0].
func CalculateEntropy(s string) (float64, float64) {
	if len(s) == 0 {
		return 0, 0
	}

	freq := make(map[rune]int)
	totalRunes := 0
	for _, r := range s {
		freq[r]++
		totalRunes++
	}

	var entropy float64
	for _, count := range freq {
		p := float64(count) / float64(totalRunes)
		entropy -= p * math.Log2(p)
	}

	// Normalise against typical natural language / code ceiling (~4.5 bits)
	norm := entropy / 4.5
	if norm > 1.0 {
		norm = 1.0
	} else if norm < 0.0 {
		norm = 0.0
	}

	return entropy, norm
}

// Score evaluates a text string against optional task directive and salience threshold.
func (c *Classifier) Score(text string, taskDirective string, threshold float64) SalienceMetrics {
	if threshold <= 0.0 || threshold >= 1.0 {
		threshold = c.DefaultThreshold
	}

	trimmed := strings.TrimSpace(text)
	if len(trimmed) == 0 {
		return SalienceMetrics{
			SalienceScore: 0.0,
			IsSalient:     false,
			DiscardReason: "empty_text",
		}
	}

	// 1. Calculate Shannon Entropy
	entropy, entropyNorm := CalculateEntropy(trimmed)

	// 2. Tokenize and calculate Lexical Density
	words := tokenize(trimmed)
	totalWords := len(words)
	if totalWords == 0 {
		return SalienceMetrics{
			Entropy:       entropy,
			EntropyNorm:   entropyNorm,
			SalienceScore: 0.0,
			IsSalient:     false,
			DiscardReason: "no_alphanumeric_tokens",
		}
	}

	uniqueWords := make(map[string]struct{})
	contentWordsCount := 0
	for _, w := range words {
		lower := strings.ToLower(w)
		uniqueWords[lower] = struct{}{}
		if _, isStop := standardStopwords[lower]; !isStop {
			contentWordsCount++
		}
	}

	uniqueRatio := float64(len(uniqueWords)) / float64(totalWords)
	contentRatio := float64(contentWordsCount) / float64(totalWords)
	lexicalDensity := (uniqueRatio*0.6 + contentRatio*0.4)
	if lexicalDensity > 1.0 {
		lexicalDensity = 1.0
	}

	// 3. Entity & Information Density (numbers, symbols, identifiers, key-values)
	numMatches := len(numberRegex.FindAllString(trimmed, -1))
	kvMatches := len(keyValueRegex.FindAllString(trimmed, -1))
	idMatches := len(identifierRegex.FindAllString(trimmed, -1))

	entityFeatures := float64(numMatches*2 + kvMatches*3 + idMatches*2)
	entityDensity := entityFeatures / (float64(totalWords)*0.5 + 5.0)
	if entityDensity > 1.0 {
		entityDensity = 1.0
	}

	// 4. Repetition Penalty
	repetitionPenalty := 0.0
	if hasRepeatedRunes(trimmed, 5) {
		repetitionPenalty += 0.35
	}
	if uniqueRatio < 0.35 && totalWords >= 6 {
		repetitionPenalty += 0.35
	}

	// 5. Semantic Characterization: Boilerplate vs Actionable vs Prose
	lowerText := strings.ToLower(trimmed)
	upperText := strings.ToUpper(trimmed)

	isActionable := isActionableAlert(upperText)
	isBoilerplate := isBoilerplateLog(lowerText)
	isProse := isProseDocumentation(trimmed, totalWords, lexicalDensity)

	actionableBoost := 0.0
	if isActionable {
		actionableBoost = 0.25
	}

	proseBoost := 0.0
	if isProse {
		proseBoost = 0.20
	}

	boilerplatePenalty := 0.0
	if isBoilerplate && !isActionable {
		boilerplatePenalty = 0.40
	}

	// 6. Task Directive Relevance
	taskRelevance := 0.0
	hasTask := strings.TrimSpace(taskDirective) != ""
	if hasTask {
		taskWords := tokenize(taskDirective)
		taskWordSet := make(map[string]struct{})
		for _, tw := range taskWords {
			lower := strings.ToLower(tw)
			if _, isStop := standardStopwords[lower]; !isStop && len(lower) > 2 {
				taskWordSet[lower] = struct{}{}
			}
		}

		if len(taskWordSet) > 0 {
			matches := 0
			for w := range uniqueWords {
				if _, found := taskWordSet[w]; found {
					matches++
				}
			}
			denom := math.Min(2.0, float64(len(taskWordSet)))
			taskRelevance = float64(matches) / denom
			if taskRelevance > 1.0 {
				taskRelevance = 1.0
			}
		} else {
			taskRelevance = 0.5
		}
	} else {
		taskRelevance = 0.5
	}

	// 7. Composite Salience Score
	var salienceScore float64
	if hasTask {
		salienceScore = (0.20 * entropyNorm) + (0.25 * lexicalDensity) + (0.20 * entityDensity) + (0.25 * taskRelevance) + proseBoost + actionableBoost - repetitionPenalty - boilerplatePenalty
	} else {
		salienceScore = (0.35 * entropyNorm) + (0.35 * lexicalDensity) + (0.30 * entityDensity) + proseBoost + actionableBoost - repetitionPenalty - boilerplatePenalty
	}

	if salienceScore < 0.0 {
		salienceScore = 0.0
	} else if salienceScore > 1.0 {
		salienceScore = 1.0
	}

	// 8. Gating Decision
	isSalient := salienceScore >= threshold
	var discardReason string
	if !isSalient {
		if boilerplatePenalty > 0.0 {
			discardReason = "boilerplate_system_log"
		} else if repetitionPenalty > 0.2 {
			discardReason = "high_repetition_low_entropy"
		} else if entropyNorm < 0.4 {
			discardReason = "low_shannon_entropy"
		} else if hasTask && taskRelevance < 0.2 && !isProse {
			discardReason = "low_task_relevance"
		} else if lexicalDensity < 0.3 {
			discardReason = "low_lexical_density_boilerplate"
		} else {
			discardReason = "below_salience_threshold"
		}
	}

	return SalienceMetrics{
		Entropy:           entropy,
		EntropyNorm:       entropyNorm,
		LexicalDensity:    lexicalDensity,
		EntityDensity:     entityDensity,
		TaskRelevance:     taskRelevance,
		RepetitionPenalty: repetitionPenalty + boilerplatePenalty,
		SalienceScore:     math.Round(salienceScore*1000) / 1000,
		IsSalient:         isSalient,
		DiscardReason:     discardReason,
	}
}

// FilterChunks evaluates a list of chunks, returning filtered salient and discarded sets.
func (c *Classifier) FilterChunks(chunks []buffer.Chunk, taskDirective string, threshold float64, includeDiscarded bool) FilterResult {
	if threshold <= 0.0 || threshold >= 1.0 {
		threshold = c.DefaultThreshold
	}

	var salient []FilteredChunk
	var discarded []FilteredChunk

	for _, chunk := range chunks {
		metrics := c.Score(chunk.Data, taskDirective, threshold)
		fc := FilteredChunk{
			Chunk:   chunk,
			Metrics: metrics,
		}

		if metrics.IsSalient {
			salient = append(salient, fc)
		} else {
			if includeDiscarded {
				discarded = append(discarded, fc)
			}
		}
	}

	total := len(chunks)
	discardedCount := total - len(salient)
	var noiseReductionRatio float64
	if total > 0 {
		noiseReductionRatio = float64(discardedCount) / float64(total)
	}

	return FilterResult{
		TotalEvaluated:      total,
		SalientCount:        len(salient),
		DiscardedCount:      discardedCount,
		NoiseReductionRatio: math.Round(noiseReductionRatio*1000) / 1000,
		Threshold:           threshold,
		TaskDirective:       taskDirective,
		SalientChunks:       salient,
		DiscardedChunks:     discarded,
	}
}

func tokenize(s string) []string {
	var words []string
	var curr strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' {
			curr.WriteRune(r)
		} else {
			if curr.Len() > 0 {
				words = append(words, curr.String())
				curr.Reset()
			}
		}
	}
	if curr.Len() > 0 {
		words = append(words, curr.String())
	}
	return words
}

func hasRepeatedRunes(s string, minRepeats int) bool {
	if len(s) < minRepeats {
		return false
	}
	var last rune
	count := 0
	for _, r := range s {
		if r == last {
			count++
			if count >= minRepeats {
				return true
			}
		} else {
			last = r
			count = 1
		}
	}
	return false
}

func isBoilerplateLog(lower string) bool {
	if strings.Contains(lower, "ping ") && strings.Contains(lower, "icmp_seq=") {
		return true
	}
	if strings.Contains(lower, "heartbeat") && (strings.Contains(lower, "status=ok") || strings.Contains(lower, "load=")) {
		return true
	}
	if (strings.Contains(lower, "get /health") || strings.Contains(lower, "/healthz") || strings.Contains(lower, "get /metrics")) && strings.Contains(lower, "200") {
		return true
	}
	if strings.Contains(lower, "link up") && strings.Contains(lower, "full-duplex") {
		return true
	}
	if strings.HasPrefix(lower, "[debug]") && (strings.Contains(lower, "probe") || strings.Contains(lower, "ping") || strings.Contains(lower, "ok")) {
		return true
	}
	return false
}

func isActionableAlert(upper string) bool {
	alertKeywords := []string{
		"CRITICAL", "ALERT", "ERROR", "PANIC", "FATAL", "WARNING",
		"EXCEPTION", "FAIL", "FAILURE", "OOM", "TASK DIRECTIVE", "INSTRUCTION",
	}
	for _, kw := range alertKeywords {
		if strings.Contains(upper, kw) {
			return true
		}
	}
	return false
}

func isProseDocumentation(s string, totalWords int, lexicalDensity float64) bool {
	if len(s) >= 70 && totalWords >= 10 && lexicalDensity >= 0.50 {
		if strings.ContainsAny(s, ".,;:") {
			return true
		}
	}
	return false
}


