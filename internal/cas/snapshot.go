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
	"os"
	"encoding/hex"
	"io/ioutil"
	"github.com/s3git/s3git-go/internal/kv"
)

//
// Deduped Storage Format for Snapshots
// ====================================
//
// Implementation of 'deduped' storage format for use within snapshot directories
//
// In contrast to external backend storage where the name is the root level hash and the
// contents of the object is the concatenation of the leaf hashes
// (see https://github.com/s3git/s3git/blob/master/BLAKE2.md#deduplicated
// this does not work for snapshots since the file name is (well...) the file name used
// within the snapshot.
//
// Therefore we concatenate the root level hash after the list of leaf hashes, like so
//
// /-----\  /-----\  /-----\  /-----\      /=====\  /=====\
// | 0:0 |  | 0:1 |  | 0:2 |  | 0:3 |  ... | 0:N |  | 1:0 |
// \-----/  \-----/  \-----/  \-----/      \=====/  \=====/
//
// This format allows for conclusive detection of this structure (hash the concatenation
// of the leaf hashes and it has to equal the root level hash, ie. last 64 bytes)
//
// This way it also becomes possible to detect files (when not checked out hydrated)
// that are either moved or copied within a snapshot directory (irrespective of the
// file name)
//

// Write all leaf hashes as contents  followed by final 64 bytes with level 1 root hash
func WriteLevel1HashFollowedByLeafHashes(hash, filename string, mode os.FileMode) {
	hex, _ := hex.DecodeString(hash)
	leafHashes, _, err := kv.GetLevel1(hex)
	if len(leafHashes) == 0 {
		// TODO: Make sure we fetch from upstream
		panic("")
	}

	b := make([]byte, len(hex)+len(leafHashes))
	copy(b[0:len(leafHashes)], leafHashes[:]) // Copy all leaves
	copy(b[len(leafHashes):], hex[:]) // Add level 1 hash

	err = ioutil.WriteFile(filename, b, mode)
	if err != nil {
		return
	}
}

// Check for all leaf hashes as contents followed by final 64 bytes with level 1 root hash
func CheckLevel1HashFollowedByLeafHashes(filename string) (bool, string, []Key, error) {

	stat, err := os.Stat(filename)
	if err != nil {
		return false, "", nil, err
	}

	// Check whether the size contains at least two keys or is a multiple of 64 bytes
	if stat.Size() < KeySize*2 || stat.Size() & (KeySize-1) != 0 {
		return false, "", nil, nil
	}

	// Otherwise compute the level 1 hash on the contents
	hashes, err := ioutil.ReadFile(filename)
	if err != nil {
		return false, "", nil, err
	}

	// Read leaf hashes (skipping first hash)
	leaves := make([]Key, 0, len(hashes)/KeySize - 1)
	for i := 0; i < len(hashes) - KeySize; i += KeySize {
		leaves = append(leaves, NewKey(hashes[i:i+KeySize]))
	}

	// Compute root hash
	rootStr, err := computeRootBlake2(leaves)
	if err != nil {
		return false, "", nil, err
	}

	// Now if hashes equal it must be (local) deduped format
	return hex.EncodeToString(hashes[len(hashes)-KeySize:]) == rootStr, rootStr, leaves, nil
}
