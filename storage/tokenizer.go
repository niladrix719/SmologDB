package storage

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
