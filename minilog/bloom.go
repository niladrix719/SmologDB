package minilog

import "github.com/cespare/xxhash/v2"

// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run TestStage2 -v
//
// The FP-rate harness compares your measured false-positive rate against the
// closed-form prediction (1 - e^(-k/bitsPerItem))^k. It does not know anything
// about your implementation, so it cannot be satisfied by a bug that happens
// to agree with itself.
//
// Reference: lib/logstorage/bloomfilter.go in VictoriaLogs.
// ---------------------------------------------------------------------------

const (
	// BloomHashesCount is the number of hash functions (k).
	// Same value VictoriaLogs uses.
	BloomHashesCount = 6

	// BloomBitsPerItem is m/n -- bits allocated per inserted token.
	// Same value VictoriaLogs uses.
	BloomBitsPerItem = 16
)

// Bloom is a bloom filter over a set of string tokens.
//
// Layout note: store the bits as []uint64 rather than []byte. You will be
// doing (hash % nbits) then a word index + bit index, and uint64 words keep
// that cheap. VictoriaLogs does the same.
type Bloom struct {
	// bits is the bit array, as 64-bit words.
	bits []uint64
}

// Init builds a filter sized for the given tokens and inserts them all.
//
// Sizing: nbits = len(tokens) * BloomBitsPerItem, rounded up to a whole number
// of uint64 words, with a sane minimum (a filter with 0 bits must not divide
// by zero, and a filter that is too small makes the FP math meaningless).
//
// Duplicate tokens must not be counted twice for sizing -- dedupe first or
// require the caller to pass a deduped slice. Decide which, and write it down
// here, because the FP harness depends on knowing n.
//
// HINT on generating k hashes from one hash function: do NOT call xxhash k
// times with k different seeds -- that is slow and easy to get wrong. Use
// Kirsch-Mitzenmacher double hashing: split one 64-bit hash into h1 (low 32)
// and h2 (high 32), then hash_i = h1 + i*h2. If your measured FP rate comes
// out much WORSE than theory, your k hashes are correlated and this is why.
func (bf *Bloom) Init(tokens []string) {
	nbits := max(uint64(len(tokens)*BloomBitsPerItem), 64)
	nbits = ((nbits + 63) / 64) * 64

	nwords := nbits / 64
	bf.bits = make([]uint64, nwords)
	for _, tok := range tokens {
		hash := xxhash.Sum64String(tok)
		h1 := uint32(hash)
		h2 := uint32(hash >> 32)
		for i := range BloomHashesCount {
			pos := (uint64(h1) + uint64(i)*uint64(h2)) % nbits
			bf.bits[pos/64] |= 1 << (pos % 64)
		}
	}
}

// Contains reports whether the token may be present.
//
// false  => definitely not present  (this is the useful answer)
// true   => maybe present, must verify by decoding the actual values
//
// It must use exactly the same hashing scheme as Init.
func (bf *Bloom) Contains(token string) bool {
	hash := xxhash.Sum64String(token)
	h1 := uint32(hash)
	h2 := uint32(hash >> 32)
	for i := range BloomHashesCount {
		pos := (uint64(h1) + uint64(i)*uint64(h2)) % uint64((len(bf.bits) * 64))
		if bf.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// SizeBytes is the marshaled size of the filter. Used by the harness to report
// bloom overhead as a fraction of total part size.
func (bf *Bloom) SizeBytes() int {
	return len(bf.bits) * 8
}

// Reset clears bf for reuse.
//
// Once you get to stage 5, this is where sync.Pool enters the picture: the
// real repo never allocates a bloom filter per block, it leases one. Leave a
// note here about what you measured before and after.
func (bf *Bloom) Reset() {
	bf.bits = nil
}

// Marshal appends the serialized filter to dst and returns the result.
//
// Format is yours to choose, but it must be self-describing enough that
// Unmarshal can recover the bit count. Prefer a varint length prefix over a
// fixed-width one -- you will be writing one of these per column per block and
// the overhead adds up.
func (bf *Bloom) Marshal(dst []byte) []byte {
	dst = MarshalVarUint64(dst, uint64(len(bf.bits)))
	for _, in := range bf.bits {
		dst = MarshalVarUint64(dst, in)
	}
	return dst
}

// Unmarshal parses a filter from src.
//
// It must return an error rather than panicking on malformed input: the crash
// test in stage 3 will feed you truncated files on purpose.
func (bf *Bloom) Unmarshal(src []byte) error {
	var u uint64
	var count uint64
	var err error
	count, src, err = UnmarshalVarUint64(src)
	if err != nil {
		return err
	}
	for i := 0; i < int(count); i++ {
		u, src, err = UnmarshalVarUint64(src)
		if err != nil {
			return err
		}
		bf.bits = append(bf.bits, u)
	}
	return err
}
