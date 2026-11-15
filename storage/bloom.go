package storage

import "github.com/cespare/xxhash/v2"

const (
	BloomHashesCount = 6

	BloomBitsPerItem = 16
)

type Bloom struct {
	bits []uint64
}

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

func (bf *Bloom) SizeBytes() int {
	return len(bf.bits) * 8
}

func (bf *Bloom) Reset() {
	bf.bits = nil
}

func (bf *Bloom) Marshal(dst []byte) []byte {
	dst = MarshalVarUint64(dst, uint64(len(bf.bits)))
	for _, in := range bf.bits {
		dst = MarshalVarUint64(dst, in)
	}
	return dst
}

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
