package cas

import (
	"encoding/hex"
)

const stageDir = ".stage"
const cacheDir = ".cache"

// Size of the CAS keys in bytes.
const KeySize = 64
const KeySizeHex = KeySize*2
const ChunkSize = 5*1024*1024

// A Key that identifies data stored in the CAS. Keys are immutable.
type Key struct {
	object [KeySize]byte
}

// BadKeySizeError is passed to panic if NewKey is called with input
// that is not KeySize long.
type BadKeySizeError struct {
	Key []byte
}

func newKey(b []byte) Key {
	k := Key{}
	n := copy(k.object[:], b)
	if n != KeySize {
		panic(BadKeySizeError{Key: b})
	}
	return k
}

// NewKey makes a new Key with the given byte contents.
//
// This function is intended for use when unmarshaling keys from
// storage.
func NewKey(b []byte) Key {
	k := newKey(b)
	//	if bytes.HasPrefix(k.object[:], specialPrefix) &&
	//	k != Empty {
	//		return Invalid
	//	}
	return k
}

// String returns a hex encoding of the key.
func (k Key) String() string {
	return hex.EncodeToString(k.object[:])
}
