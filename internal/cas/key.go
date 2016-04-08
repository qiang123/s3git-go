/*
 * Copyright 2016 Frank Wessels <fwessels@xs4all.nl>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cas

import (
	"encoding/hex"
)

const stageDir = "stage"
const cacheDir = "cache"

const BLOB = "blob"

var (
	//
	// With the additional constraint that objects are a multiple of 64 in size, this
	// adds 6 bits to the chance of getting a regular blob in the range, see table:
	//
	// PREFIX-SIZE | NR BITS | OCCURRENCE (MLD)
	//           5 |    20+6 |             0.1
	//           6 |    24+6 |             1.1
	//           7 |    28+6 |            17.2
	//           8 |    32+6 |             275
	//
	prefixChar  = '0'
	prefixNum   = 7
	prefixCheat = 3		// Number that is cheated in the prefix, like 0000xxx00000 -- will fail in file check mode
)


// Size of the CAS keys in bytes.
const KeySize = 64
const KeySizeHex = KeySize*2

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
