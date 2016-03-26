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

package s3git

import (
	"encoding/hex"
	"github.com/s3git/s3git-go/internal/backend"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/s3git/s3git-go/internal/kv"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"io/ioutil"
	"os"
)

// Pull updates for the repository
func (repo Repository) Pull(progress func(maxTicks int64)) error {

	return pull(progress)
}

func pull(progress func(maxTicks int64)) error {

	client, err := backend.GetDefaultClient()
	if err != nil {
		return err
	}

	// Get map of prefixes already in store
	prefixesInBackend, err := listPrefixes(client)
	if err != nil {
		return err
	}

	prefixesToFetch := []string{}

	// Iterate over prefixes and find out if they are already available locally
	for prefix, _ := range prefixesInBackend {
		key, _ := hex.DecodeString(prefix)
		_, _, err := kv.GetLevel1(key)
		if err != nil && !lmdb.IsNotFound(err) {
			return err
		}

		if lmdb.IsNotFound(err) {
			// Prefix is not available locally  --> add to get fetched
			prefixesToFetch = append(prefixesToFetch, prefix)
		}
	}

	if len(prefixesToFetch) == 0 {
		return nil
	}

	progress(int64(len(prefixesToFetch)))

	// TODO: [perf] Speed up by running parallel in multiple go routines
	for _, prefix := range prefixesToFetch {

		// Fetch Prefix object and all objects directly and indirectly referenced by it
		err = fetchPrefix(prefix, client)
		if err != nil {
			return err
		}

		progress(int64(len(prefixesToFetch)))
	}

	return nil
}

// Fetch Prefix object and all objects directly and indirectly referenced by it
func fetchPrefix(prefix string, client backend.Backend) error {

	// Make sure that we do the storage of the object in the cas in
	// reverse order, that is store prefix object in the cas as the
	// *last* action, just before that the commit object, etc.
	// This ensures that, if the pull action is prematurely terminated
	// (either by user/application, or some sort of error/crash) that
	// a subsequent pull will still miss the prefix object and pull it
	// again

	// First fetch blob for prefix object
	prefixName, prefixBytes, err := fetchBlobTempFileAndContents(prefix, client)
	if err != nil {
		return err
	}
	defer os.Remove(prefixName)

	po, err := core.GetPrefixObjectFromString(string(prefixBytes))
	if err != nil {
		return err
	}

	{
		// Now pull down commit object
		commitName, commitBytes, err := fetchBlobTempFileAndContents(po.S3gitFollowMe, client)
		if err != nil {
			return err
		}
		defer os.Remove(commitName)

		co, err := core.GetCommitObjectFromString(string(commitBytes))
		if err != nil {
			return err
		}

		{
			// Now pull down tree object
			treeName, treeBytes, err := fetchBlobTempFileAndContents(co.S3gitTree, client)
			if err != nil {
				return err
			}
			defer os.Remove(treeName)

			to, err := core.GetTreeObjectFromString(string(treeBytes))
			if err != nil {
				return err
			}

			// Cache root keys for all added blobs in this commit ...
			err = cacheKeysForBlobs(to.S3gitAdded)
			if err != nil {
				return err
			}

			// Add tree object to cas
			_, err = cas.StoreBlobInCache(treeName, kv.TREE)
			if err != nil {
				return err
			}

			// Delete the chunks for the tree object since we are unlikely the need it again
			err = cas.DeleteChunksForBlob(co.S3gitTree)
			if err != nil {
				return err
			}
		}

		// Add commit object to cas
		_, err = cas.StoreBlobInCache(commitName, kv.COMMIT)
		if err != nil {
			return err
		}

		// Mark warm and cold parents as parents
		err = co.MarkWarmAndColdParents()
		if err != nil {
			return err
		}
	}

	// Add prefix object to cas
	_, err = cas.StoreBlobInCache(prefixName, kv.PREFIX)
	if err != nil {
		return err
	}

	return nil
}

// Fetch blob down to temp file along with contents
func fetchBlobTempFileAndContents(prefix string, client backend.Backend) (tempFile string, contents []byte, err error) {

	name, err := cas.FetchBlobToTempFile(prefix, client)
	if err != nil {
		return "", nil, err
	}

	contents, err = ioutil.ReadFile(name)
	if err != nil {
		return "", nil, err
	}

	return name, contents, nil
}

const treeBatchSize = 0x4000

// Just cache the key for BLOBs, content will be pulled down later when needed
// For performance do not write per key to the KV but write in larger batches
func cacheKeysForBlobs(added []string) error {

	// Create arrays to be able to batch the operation
	keys := make([][]byte, 0, treeBatchSize)
	values := make([][]byte, 0, treeBatchSize)

	count := 0
	for _, add := range added {
		key, _ := hex.DecodeString(add)

		keys = append(keys, key)
		values = append(values, nil)

		if len(keys) == cap(keys) {
			err := kv.AddMultiToLevel1(keys, values, kv.BLOB)
			if err != nil {
				return err
			}

			keys = keys[:0]
			values = values[:0]
		}
		count++
	}

	if len(keys) > 0 {
		err := kv.AddMultiToLevel1(keys, values, kv.BLOB)
		if err != nil {
			return err
		}
		count++
	}

	return nil
}
