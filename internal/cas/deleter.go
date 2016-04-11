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
	"github.com/s3git/s3git-go/internal/kv"
	"os"
)

// Delete the leaves for a given blob
func DeleteLeavesForBlob(hash string) error {

	key, _ := hex.DecodeString(hash)
	leafHashes, _, err := kv.GetLevel1(key)
	if err != nil {
		return err
	}

	for i := 0; i < len(leafHashes); i += KeySize {
		leafKey := hex.EncodeToString(leafHashes[i : i+KeySize])

		err = DeleteLeafNodeFromCache(leafKey)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete an individual leaf node from the caching area
func DeleteLeafNodeFromCache(leafKey string) error {

	// Test if available in cache directory
	nameInCache := getBlobPathWithinArea(leafKey, cacheDir)
	if _, err := os.Stat(nameInCache); err == nil {
		if err := os.Remove(nameInCache); err != nil {
			return err
		}

		err := kv.RemoveLevel0FromCache(leafKey)
		if err != nil {
			return err
		}
	}

	return nil
}