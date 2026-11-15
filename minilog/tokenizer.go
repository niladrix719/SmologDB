package minilog

// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run TestStage2Tokenizer -v
//
// Reference: lib/logstorage/tokenizer.go in VictoriaLogs.
// ---------------------------------------------------------------------------

// Tokenize splits s into word tokens and appends them to dst.
//
// A token is a maximal run of letters and digits. Everything else is a
// separator. So:
//
//	"GET /api/v1/users?id=42 200"  ->  [GET api v1 users id 42 200]
//	"2024-01-15T10:30:00Z"         ->  [2024 01 15T10 30 00Z]
//
// Note the second example: the naive rule produces tokens that a human would
// not choose. That is fine and it is worth understanding why VictoriaLogs
// accepts it -- the filter always re-verifies against the real values, so a
// coarse tokenizer costs recall on the bloom (more false "maybe"s) but never
// correctness.
//
// Decisions you have to make, and should record here once you have made them:
//   - Case: do you lowercase? What does that cost you on case-sensitive
//     filters, and what does it buy on bloom hit rate?
//   - Minimum token length: is a 1-char token worth a bloom slot?
//   - Do you cap the number of tokens per value? An 8KB stack trace can
//     produce hundreds and blow up your filter size.
func Tokenize(dst []string, s string) []string {
	st := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9') {
			if st < i {
				dst = append(dst, s[st:i])
			}
			st = i + 1
		}
	}
	if st < len(s) {
		dst = append(dst, s[st:])
	}
	return dst
}

// TokenizeValues tokenizes every value and returns the DEDUPED token set.
//
// Dedupe matters twice over:
//   - Bloom sizing uses len(tokens) as n. Duplicates inflate n, oversize the
//     filter, and make your measured FP rate come out better than theory --
//     which the harness will flag as suspicious, not as success.
//   - Inserting the same token twice is pure wasted work.
//
// The caller passes a whole column block's worth of values here, so expect
// heavy duplication (think a `level` column with 5 distinct values across
// 8192 rows).
//
// Optimisation to notice later: VictoriaLogs skips re-tokenizing a value that
// is identical to the previous one, because values arrive sorted-ish and
// runs are common. Measure whether that helps you before you add it.
func TokenizeValues(dst []string, values []string) []string {
	seen := make(map[string]bool)
	var scratch []string
	for _, val := range values {
		scratch = Tokenize(scratch[:0], val)
		for _, tok := range scratch {
			if !seen[tok] {
				dst = append(dst, tok)
				seen[tok] = true
			}
		}
	}
	return dst
}
