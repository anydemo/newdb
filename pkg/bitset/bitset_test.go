package bitset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type namedBitSet struct {
	name   string
	bitset BitSet
}

func standardBitsets(numBits uint) []namedBitSet {
	return []namedBitSet{
		{"Bytes", NewBytes(numBits)},
	}
}

func TestInRange(t *testing.T) {
	tests := []struct {
		name        string
		bitsToSet   []bool
		bitsToUnset []bool // must be same length as bitsToSet
	}{
		{
			bitsToSet:   nil,
			bitsToUnset: nil,
		},
		{
			bitsToSet:   []bool{0: false},
			bitsToUnset: []bool{0: true},
		},
		{
			bitsToSet:   []bool{1: true},
			bitsToUnset: []bool{0: true, 1: true},
		},
		{
			bitsToSet:   []bool{7: true},
			bitsToUnset: []bool{7: true},
		},
		{
			bitsToSet:   []bool{0: true, 7: true},
			bitsToUnset: []bool{1: true, 7: true},
		},
		{
			bitsToSet: []bool{0: true, 1: true, 2: true, 3: true,
				4: true, 5: true, 6: true, 7: false},
			bitsToUnset: []bool{7: true},
		},
		{
			bitsToSet:   []bool{63: true},
			bitsToUnset: []bool{63: true},
		},
		{
			bitsToSet:   []bool{0: true, 8: true, 16: true, 63: true},
			bitsToUnset: []bool{1: true, 9: true, 17: true, 63: true},
		},
		{
			bitsToSet: []bool{56: true, 57: true, 58: true, 59: true,
				60: true, 61: true, 62: true, 63: false},
			bitsToUnset: []bool{63: true},
		},
	}

	for testNum, test := range tests {
	nextBitSet:
		for _, nbs := range standardBitsets(uint(len(test.bitsToSet))) {
			// Set all bits in the bitsToSet field and compare
			// against the expected values.
			for bit, testVal := range test.bitsToSet {
				nbs.bitset.SetBool(uint(bit), testVal)
				got := nbs.bitset.Get(uint(bit))
				if got != testVal {
					t.Errorf("Test %d bitset %s failed: bit %d got %v expected %v",
						testNum, nbs.name, bit, got, testVal)
					continue nextBitSet
				}
			}

			// Unset each bit in the bitsToUnset field and check
			// that 1) the value was never set and 2) if unset,
			// the value is now unset.
			for bit, unset := range test.bitsToUnset {
				bit := uint(bit)
				exp := test.bitsToSet[bit] && !unset
				nbs.bitset.SetBool(bit, exp)
				got := nbs.bitset.Get(bit)
				if got != exp {
					t.Errorf("Test %d bitset %s unset failed: bit %d got %v expected %v",
						testNum, nbs.name, bit, got, exp)
					continue nextBitSet
				}
			}
		}
	}
}

type namedGrower struct {
	name   string
	bitset interface {
		BitSet
		Grow(uint)
	}
}

func standardGrowers(numBits int) []namedGrower {
	bytes := NewBytes(uint(numBits))
	return []namedGrower{
		{"Bytes", &bytes},
	}
}

func TestGrowing(t *testing.T) {
	tests := []struct {
		initialBits []bool
		newNumBits  int
		bitSets     []bool
	}{
		{
			initialBits: nil,
			newNumBits:  1,
			bitSets:     []bool{0: true},
		},
		{
			initialBits: nil,
			newNumBits:  64,
			bitSets:     []bool{0: true, 7: true, 31: true, 63: true},
		},
		{
			initialBits: []bool{0: true, 15: true},
			newNumBits:  64,
			bitSets:     []bool{},
		},
		{
			initialBits: []bool{0: true, 15: true},
			newNumBits:  8,
			bitSets:     []bool{7: true, 15: true},
		},
	}

	for testNum, test := range tests {
	nextBitSet:
		for _, nbs := range standardGrowers(len(test.initialBits)) {
			for bit, val := range test.initialBits {
				bit := uint(bit)
				nbs.bitset.SetBool(bit, val)
			}

			nbs.bitset.Grow(uint(test.newNumBits))

			for bit, val := range test.bitSets {
				bit := uint(bit)
				nbs.bitset.SetBool(bit, val)
			}

			for bit, exp := range test.bitSets {
				bit := uint(bit)
				got := nbs.bitset.Get(bit)
				if exp != got {
					t.Errorf("Growing %d bitset %s: bit %d: got %v expected %v",
						testNum, nbs.name, bit, got, exp)
					continue nextBitSet
				}
			}
		}
	}
}

func TestNoSets(t *testing.T) {
	tests := []int{0, 1, 8, 16, 32, 64, 128, 1024}
	for testNum, test := range tests {
	nextBitSet:
		for _, nbs := range standardBitsets(uint(test)) {
			for i := 0; i < test; i++ {
				i := uint(i)
				if nbs.bitset.Get(i) {
					t.Errorf("%d: bitset %s: zero value caused set bit %d",
						testNum, nbs.name, i)
					continue nextBitSet
				}
			}
		}
	}
}

func TestByteSlice(t *testing.T) {

	bs1 := NewBytes(8)
	assert.Len(t, []byte(bs1), 1)
	bs1.SetBool(0, true)
	assert.Equal(t, []byte{0x1}, []byte(bs1))
	bs1.SetBool(1, true)
	assert.Equal(t, []byte{0x3}, []byte(bs1))

	bs2 := NewBytes(9)
	assert.Len(t, []byte(bs2), 2)
	bs2.SetBool(0, true)
	assert.Equal(t, []byte{0x1, 0x0}, []byte(bs2))
	bs2.SetBool(1, true)
	assert.Equal(t, []byte{0x3, 0x0}, []byte(bs2))
}

func TestBytes_String(t *testing.T) {
	bs := NewBytes(9)
	bs.SetBool(2, true)
	assert.Equal(t, "[4 0] bit(00100000 00000000)", bs.String())
}
