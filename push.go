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
	"errors"
	"fmt"

	"github.com/s3git/s3git-go/internal/backend"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/s3git/s3git-go/internal/kv"
	"github.com/s3git/s3git-go/internal/util"

	"encoding/hex"
	"github.com/dustin/go-humanize"
	"sync"
	"bytes"
)

// Perform a push to the back end for the repository
func (repo Repository) Push(hydrated bool, progress func(maxTicks int64)) error {

	list, err := kv.ListLevel1Prefixes()
	if err != nil {
		return err
	}

	return push(list, hydrated, progress)
}

// Push any new commit objects including all added objects to the back end store
func push(prefixChan <-chan []byte, hydrated bool, progress func(maxTicks int64)) error {

	client, err := backend.GetDefaultClient()
	if err != nil {
		return err
	}

	// Get map of prefixes already in store
	prefixesInBackend, err := listPrefixes(client)
	if err != nil {
		return err
	}

	prefixesToPush := []string{}

	for prefixByte := range prefixChan {

		prefix := hex.EncodeToString(prefixByte)

		_, verified := prefixesInBackend[prefix]

		// We can safely skip in case a prefix object is verified (pushed as last object)
		if !verified {

			prefixesToPush = append(prefixesToPush, prefix)
		}
	}

	if len(prefixesToPush) == 0 {
		return nil
	}

	progress(int64(len(prefixesToPush)))

	for _, prefix := range prefixesToPush {

		// Get prefix object
		po, err := core.GetPrefixObject(prefix)
		if err != nil {
			return err
		}

		// Get commit object
		co, err := core.GetCommitObject(po.S3gitFollowMe)
		if err != nil {
			return err
		}

		// Get tree object
		to, err := core.GetTreeObject(co.S3gitTree)
		if err != nil {
			return err
		}

		// first push all added blobs in this commit ...
		err = pushBlobRange(to.S3gitAdded, nil, hydrated, client)
		if err != nil {
			return err
		}

		// then push tree object
		_, err = pushBlob(co.S3gitTree, nil, client)
		if err != nil {
			return err
		}

		// then push commit object
		_, err = pushBlob(po.S3gitFollowMe, nil, client)
		if err != nil {
			return err
		}

		// ...  finally push prefix object itself
		// (if something goes in chain above, the prefix object will be missing so
		//  will be (attempted to) uploaded again during the next push)
		_, err = pushBlob(prefix, nil, client)
		if err != nil {
			return err
		}

		progress(int64(len(prefixesToPush)))
	}

	return nil
}

// Push a blob to the back end store
func pushBlob(hash string, size *uint64, client backend.Backend) (newlyUploaded bool, err error) {

	startOfLine := ""
	if size != nil {
		startOfLine = fmt.Sprintf("Uploading %s (%s)", util.FriendlyHash(hash), humanize.Bytes(*size))
	} else {
		startOfLine = fmt.Sprintf("Uploading %s", util.FriendlyHash(hash))
	}

	// TODO: Consider whether we want to verify again...
	if false {
		verified, err := client.VerifyHash(hash)
		if err != nil {
			fmt.Println(startOfLine, "verification failed", err)
			return false, err
		}

		if verified { // Resource already in back-end
			fmt.Println(startOfLine, "already in store")

			return false, nil
		}
	}

	// TODO: for back ends storing whole files: consider multipart upload?

	cr := cas.MakeReader(hash)
	if cr == nil {
		panic(errors.New("Failed to create cas reader"))
	}

	err = client.UploadWithReader(hash, cr)
	if err != nil {
		fmt.Println(startOfLine, "failed to upload to store", err)
		return false, err
	}

	// Move blob from .stage to .cache directory upon successful upload
	err = cas.MoveBlobToCache(hash)
	if err != nil {
		fmt.Println(startOfLine, "failed to move underlying blobs to cache", err)
		return false, err
	}

	//fmt.Println(startOfLine, "successfully uploaded to store")

	return true, nil
}

// Push a blob to the back end store in deduplicated format
func PushBlobDeduped(hash string, size *uint64, client backend.Backend) (newlyUploaded bool, err error) {

	// TODO: for back ends storing chunks: consider uploading chunks in parallel

	hx, err := hex.DecodeString(hash)
	if err != nil {
		return false, err
	}

	// Get hashes for leaves
	leafHashes, _, err := kv.GetLevel1(hx)
	if err != nil {
		return false, err
	} else if len(leafHashes) == 0 {
		return false, errors.New(fmt.Sprintf("Unable to push an empty blob: %s", hash))
	}

	// Iterate over the leaves and push up to remote
	for i := 0; i < len(leafHashes); i += cas.KeySize {

		// TODO: verify whether leaf blob is already in back end, and skip if so
		err := cas.PushLeafBlob(hex.EncodeToString(leafHashes[i:i+cas.KeySize]), client)
		if err != nil {
			return false, err
		}
	}

	// Finally upload root hash
	b := bytes.NewBuffer(leafHashes)
	err = client.UploadWithReader(hash, b)
	if err != nil {
		return false, err
	}

	// TODO: Duplicate code with function above -- consider merging functions/common code
	// Move blob from .stage to .cache directory upon successful upload
	err = cas.MoveBlobToCache(hash)
	if err != nil {
		//fmt.Println(startOfLine, "failed to move underlying blobs to cache", err)
		return false, err
	}

	return true, nil
}

func minu64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

// Push a range of blobs to the back end store in parallel
//
// See https://github.com/adonovan/gopl.io/blob/master/ch8/thumbnail/thumbnail_test.go
//
func pushBlobRange(hashes []string, size *uint64, hydrated bool, client backend.Backend) error {

	var wg sync.WaitGroup
	var msgs = make(chan string)
	var results = make(chan error)

	for i := 0; i < min(len(hashes), 100); i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for hash := range msgs {

				pushHydratedToRemote := hydrated
				if !checkIfLeavesAreEqualSize(hash) {
					pushHydratedToRemote = false	// Cannot push hydrated to remote back end when eg rolling hash is used (as we do not know where the boundaries are)
				}
				var err error
				if pushHydratedToRemote {
					_, err = pushBlob(hash, size, client)
				} else {
					_, err = PushBlobDeduped(hash, size, client)
				}
				results <- err
			}
		}()
	}

	go func() {
		for _, hash := range hashes {
			msgs <- hash
		}
		close(msgs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var err error
	for e := range results {
		if e != nil {
			err = e
		}
	}

	return err
}

func checkIfLeavesAreEqualSize(hash string) bool {
	// TODO: Implement: iterate over all leaves, check whether (except for last node) all sizes are equal
	return true
}

// List prefixes at back end store, doing 16 lists in parallel
func listPrefixes(client backend.Backend) (map[string]bool, error) {

	var wg sync.WaitGroup
	var results = make(chan []string)

	for i := 0x0; i <= 0xf; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			result := make([]string, 0, 1000)

			client.List(fmt.Sprintf("%s%x", core.Prefix(), i), func(key string) {
				result = append(result, key)

				// TODO: WE NEED TO wg.Done() HERE WHEN LAST KEY HAS BEEN RECEIVED
				//       -- SEE /Users/frankw/golang/src/github.com/fwessels/listperf/listperf.go
				// IMPORTANT: WE WILL BE MISSING OBJECTS HERE
			})

			results <- result
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	prefixHash := make(map[string]bool)
	for result := range results {
		for _, r := range result {
			prefixHash[r] = true
		}
	}

	return prefixHash, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
