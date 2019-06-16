package newdb

import "fmt"

// forked from https://github.com/HcashOrg/bitset/blob/master/bitset.go

var (
	// byteModMask is the maximum number of indexes a 1 may be left
	// shifted by before the value overflows a byte.  It is equal to one
	// less than the number of bits per byte.
	//
	// This package uses this value to calculate all bit indexes within
	// a byte as it is quite a bit more efficient to perform a bitwise
	// AND with this rather than using the modulus operator (n&7 == n%8).
	byteModMask = uint(7) // 0b0000111

	// byteShift is the number of bits to perform a right shift of a bit
	// index to get the byte index in a bitset.  It is functionally
	// equivalent to integer dividing by 8 bits per byte, but is a bit
	// more efficient to calculate.
	byteShift = uint(3)
)

// BitSet the bitset interface
type BitSet interface {
	Get(i uint) bool
	Set(i uint)
	Unset(i uint)
	SetBool(i uint, b bool)
}

var _ BitSet = (*Bytes)(nil)

// Bytes the bitset []byte
type Bytes []byte

// NewBytes returns a new bitset that is capable of holding numBits number
// of binary values.  All bytes in the bitset are zeroed and each bit is
// therefore considered unset.
func NewBytes(numBits uint) Bytes {
	return make(Bytes, (numBits+byteModMask)>>byteShift)
}

// Get returns whether the bit at index i is set or not.  This method will
// panic if the index results in a byte index that exceeds the number of
// bytes held by the bitset.
func (s Bytes) Get(i uint) bool {
	return s[uint(i)>>byteShift]&(1<<(uint(i)&byteModMask)) != 0
}

// Set sets the bit at index i.  This method will panic if the index results
// in a byte index that exceeds the number of a bytes held by the bitset.
func (s Bytes) Set(i uint) {
	s[uint(i)>>byteShift] |= 1 << (uint(i) & byteModMask)
}

// Unset unsets the bit at index i.  This method will panc if the index
// results in a byte index that exceeds the number of bytes held by the
// bitset.
func (s Bytes) Unset(i uint) {
	s[uint(i)>>byteShift] &^= 1 << (uint(i) & byteModMask)
}

// SetBool sets or unsets the bit at index i depending on the value of b.
// This method will panic if the index results in a byte index that exceeds
// the nubmer of bytes held by the bitset.
func (s Bytes) SetBool(i uint, b bool) {
	if b {
		s.Set(i)
		return
	}
	s.Unset(i)
}

// Grow ensures that the bitset s is large enough to hold numBits number of
// bits, potentially appending to and/or reallocating the slice if the
// current length is not sufficient.
func (s *Bytes) Grow(numBits uint) {
	bytes := *s
	targetLen := (numBits + byteModMask) >> byteShift
	missing := targetLen - uint(len(bytes))
	if missing > 0 && missing <= targetLen {
		*s = append(bytes, make(Bytes, missing)...)
	}
}

// String eq: `[4 0] bit(00100000 00000000)`
func (s Bytes) String() string {
	var ret string
	for i := 0; i < len([]byte(s))*8; i++ {
		res := s.Get(uint(i))
		if i > 0 && i%8 == 0 {
			ret += " "
		}
		ret += func(b bool) string {
			if b {
				return "1"
			}
			return "0"
		}(res)
	}
	return fmt.Sprintf("%v bit(%v)", []byte(s), ret)
}
