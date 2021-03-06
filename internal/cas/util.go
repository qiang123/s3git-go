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
	"github.com/s3git/s3git-go/internal/backend"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/kv"
	"os"
	"path"
)

// Upon writing, make sure the size of the repository does not exceed the max local size,
// prune stale chunks otherwise
func checkRepoSize() error {

	// TODO: [perf] Maybe cache sizes of stage & cache area as they may be expensive for large repositories

	stageSize, err := kv.GetLevel0StageSize()
	if err != nil {
		return err
	}

	cacheSize, err := kv.GetLevel0CacheSize()
	if err != nil {
		return err
	}

	if stageSize+cacheSize > config.Config.MaxRepoSize {

		threshold95Pct := uint64(float64(config.Config.MaxRepoSize) * 0.95)

		// Prune leaf nodes in caching area in a number of iterations...
		for {
			if cacheSize < 100 || // Stop when less than 100 leaves are left in cache area to delete
				stageSize+cacheSize < threshold95Pct { // Or when we've gone under the threshold
				break
			}

			// Get random list of leaf nodes in cache
			leaves, err := kv.GetLevel0RandomListFromCache()
			if err != nil {
				return err
			}

			// Delete these leaves
			for _, l := range leaves {
				err = DeleteLeafNodeFromCache(l)
				if err != nil {
					return err
				}
			}

			cacheSize, err = kv.GetLevel0CacheSize()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Get the filepath for a given hash
func getBlobPath(hash string) string {

	// Check if in stage directory
	nameInStage := getBlobPathWithinArea(hash, stageDir)
	if _, err := os.Stat(nameInStage); err == nil {
		return nameInStage
	}

	// Check if in cache directory
	nameInCache := getBlobPathWithinArea(hash, cacheDir)
	if _, err := os.Stat(nameInCache); err == nil {
		return nameInCache
	}

	// Check size of repo, prune stale chunks if necessary
	checkRepoSize()

	// TODO: Chunk is not in cache, download from upstream

	return nameInCache
}

// Push a low level leaf node to a remote back end
func PushLeafBlob(hash string, client backend.Backend) error {

	path := getBlobPath(hash)

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	err = client.UploadWithReader(hash, file)
	if err != nil {
		return err
	}

	return nil
}

// Fetch a low level leaf node from a remote back end
func FetchLeafBlob(hash string, client backend.Backend) error {

	filename := getBlobPathWithinArea(hash, cacheDir)

	if _, err := os.Stat(filename); err == nil {
		// File already exists --> no need to download again
		return nil
	}

	// Otherwise create file
	file, err := createLeafNodeFile(hash, cacheDir)
	if err != nil {
		return err
	}
	defer file.Close()

	// And download
	err = client.DownloadWithWriter(hash, file)
	if err != nil {
		return err
	}

	// Get file size
	fi, err := os.Stat(filename)
	if err != nil {
		return err
	}

	// Add size of leaf to KV store
	err = addLeafBlobFileToKV(hash, cacheDir, uint32(fi.Size()))
	if err != nil {
		return err
	}

	return nil
}

// Get the filepath for a given hash in either the .stage or .cache area
func getBlobPathWithinArea(hash, area string) string {
	return path.Join(config.Config.BasePath, config.S3GIT_DIR, area, hash[0:2], hash[2:4], hash[4:])
}

// Move underlying chunks for a hash from .stage area to .cache area
func MoveBlobToCache(hash string) error {

	leaves, err := openRoot(hash)
	if err != nil {
		return err
	}

	for _, l := range leaves {
		leaveHash := l.String()

		oldPath := getBlobPathWithinArea(leaveHash, stageDir)
		if _, err := os.Stat(oldPath); os.IsNotExist(err) {
			// Chunk is not available in staging dir, verify that it is already in cache or return error otherwise
			cachePath := getBlobPathWithinArea(leaveHash, cacheDir)
			if _, errCache := os.Stat(cachePath); os.IsNotExist(errCache) {
				// Return error as chunk is also not in caching dir
				return err
			} else {
				return nil
			}
		}

		hashDir := path.Join(config.Config.BasePath, config.S3GIT_DIR, cacheDir, leaveHash[0:2], leaveHash[2:4]) + "/"
		err := os.MkdirAll(hashDir, os.ModePerm)
		if err != nil {
			return err
		}
		newPath := hashDir + leaveHash[4:]

		// Rename file in order to move from stage to cache area
		err = os.Rename(oldPath, newPath)
		if err != nil {
			return err
		}

		// Move size in KV store from stage to cache
		err = kv.MoveLevel0FromStageToCache(leaveHash)
		if err != nil {
			return err
		}
	}

	// TODO: [impr] Consider removing directories that are left empty in the .stage area
	//   However be aware of files moving in at potentially the same time (in a seperate process)
	//   Maybe do in separate maintenance step???
	return nil
}
