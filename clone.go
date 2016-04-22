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
	"fmt"
	"github.com/s3git/s3git-go/internal/backend"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/s3git/s3git-go/internal/kv"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"sync"
)

type treeInput struct {
	hash   string
	client backend.Backend
}

type treeOutput struct {
	added []string
}

type cloneOptions struct {
	leafSize            uint32
	maxRepoSize         uint64
	accessKey           string
	secretKey           string
	endpoint            string
	progressDownloading func(maxTicks int64)
	progressProcessing  func(maxTicks int64)
}

func CloneOptionSetLeafSize(leafSize uint32) func(optns *cloneOptions) {
	return func(optns *cloneOptions) {
		optns.leafSize = leafSize
	}
}

func CloneOptionSetMaxRepoSize(maxRepoSize uint64) func(optns *cloneOptions) {
	return func(optns *cloneOptions) {
		optns.maxRepoSize = maxRepoSize
	}
}

func CloneOptionSetAccessKey(accessKey string) func(optns *cloneOptions) {
	return func(optns *cloneOptions) {
		optns.accessKey = accessKey
	}
}

func CloneOptionSetSecretKey(secretKey string) func(optns *cloneOptions) {
	return func(optns *cloneOptions) {
		optns.secretKey = secretKey
	}
}

func CloneOptionSetEndpoint(endpoint string) func(optns *cloneOptions) {
	return func(optns *cloneOptions) {
		optns.endpoint = endpoint
	}
}

func CloneOptionSetDownloadProgress(progressDownloading func(maxTicks int64)) func(optns *cloneOptions) {
	return func(optns *cloneOptions) {
		optns.progressDownloading = progressDownloading
	}
}

func CloneOptionSetProcessingProgress(progressProcessing func(maxTicks int64)) func(optns *cloneOptions) {
	return func(optns *cloneOptions) {
		optns.progressProcessing = progressProcessing
	}
}

type CloneOptions func(*cloneOptions)

// Clone a remote repository
func Clone(url, path string, options ...CloneOptions) (*Repository, error) {

	optns := &cloneOptions{}
	for _, op := range options {
		op(optns)
	}

	err := config.SaveConfigFromUrl(url, path, optns.accessKey, optns.secretKey, optns.endpoint, optns.leafSize, optns.maxRepoSize)
	if err != nil {
		return nil, err
	}

	repo, err := OpenRepository(path)
	if err != nil {
		return nil, err
	}

	client, err := backend.GetDefaultClient()
	if err != nil {
		return nil, err
	}

	progressDummy := func(total int64) {}

	// Plug in dummy progress calls if unset
	if optns.progressDownloading == nil {
		optns.progressDownloading = progressDummy
	}
	if optns.progressProcessing == nil {
		optns.progressProcessing = progressDummy
	}

	err = clone(client, optns.progressDownloading, optns.progressProcessing)
	if err != nil {
		return nil, err
	}

	return repo, nil
}

func treeDownloader(trees <-chan treeInput, results chan<- treeOutput, errs chan<- error) {

	for t := range trees {
		// Pull down tree object
		cas.PullBlobDownToLocalDisk(t.hash, kv.TREE, t.client)

		to, err := core.GetTreeObject(t.hash)
		if err != nil {
			errs <- fmt.Errorf("core.GetTreeObject", err)
			return
		}

		results <- treeOutput{added: to.S3gitAdded}

		// Delete the chunks for the tree object since we are unlikely the need it again
		err = cas.DeleteLeavesForBlob(t.hash)
		if err != nil {
			errs <- fmt.Errorf("DeleteChunksForBlob error: ", err)
			return
		}
	}
}

func clone(client backend.Backend, progressDownloading, progressProcessing func(maxTicks int64)) error {

	// Get map of prefixes already in store
	prefixesInBackend, err := listPrefixes(client)
	if err != nil {
		return err
	}
	if len(prefixesInBackend) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	trees := make(chan treeInput)
	results := make(chan treeOutput)
	// TODO: Handle error(s) read from error channel
	errs := make(chan error)

	// Start multiple downloaders in parallel
	for i := 0; i <= 16; i++ {

		wg.Add(1)
		go func() {
			defer wg.Done()
			treeDownloader(trees, results, errs)
		}()
	}

	// Push trees onto input channel
	go func() {

		progressDownloading(int64(len(prefixesInBackend)))

		for prefix, _ := range prefixesInBackend {

			// TODO: Make resistant to crashes/interrupts, e.g. first save blobs, then trees, then commits, and finally prefix objects
			cas.PullBlobDownToLocalDisk(prefix, kv.PREFIX, client)
			po, err := core.GetPrefixObject(prefix)
			if err != nil {
				errs <- fmt.Errorf("core.GetCommitObject error: ", err)
				return
			}

			// Now pull down commit object
			cas.PullBlobDownToLocalDisk(po.S3gitFollowMe, kv.COMMIT, client)
			co, err := core.GetCommitObject(po.S3gitFollowMe)
			if err != nil {
				errs <- fmt.Errorf("core.GetCommitObject error: ", err)
				return
			}

			// Mark warm and cold parents as parents
			err = co.MarkWarmAndColdParents()
			if err != nil {
				errs <- fmt.Errorf("co.MarkWarmAndColdParents error: ", err)
				return
			}

			if co.S3gitTree != "" {
				trees <- treeInput{hash: co.S3gitTree, client: client}
			}

			if co.S3gitSnapshot != "" {
				// TODO: We don't necessarily have to fetch the snapshot objects upon pulling, we could fetch them on demand
				err = pullSnapshotWithChildren(co.S3gitSnapshot, client)
				if err != nil {
					return
				}
			}

			progressDownloading(int64(len(prefixesInBackend)))
		}

		// Close input channel
		close(trees)
	}()

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(results) // Close output channel
	}()

	for r := range results {
		// Cache root hash for all added blobs in this commit ...
		err := cacheKeyForBlobsToLocalDiskFirst(r.added)
		if err != nil {
			return err
		}
	}

	// As last step first stop the keys and import sorted list into KV
	if err := sortAndImportKeys(progressProcessing); err != nil {
		return err
	}

	return nil
}

// Name for temp files storing blob hashes
func hiddenKeyFilename(index int) string {
	return fmt.Sprintf("%s/.keys-0x%02x.dat", path.Join(config.Config.BasePath, config.S3GIT_DIR), index)
}

func appendBytes(slice []byte, data []byte) []byte {
	m := len(slice)
	n := m + len(data)
	if n > cap(slice) { // if necessary, reallocate
		// allocate double what's needed, for future growth.
		newSlice := make([]byte, (n+1)*2)
		copy(newSlice, slice)
		slice = newSlice
	}
	slice = slice[0:n]
	copy(slice[m:n], data)
	return slice
}

func cacheKeyForBlobsToLocalDiskFirst(added []string) error {

	keyArray := make([][]byte, 256)

	for index, _ := range keyArray {
		keyArray[index] = make([]byte, 0, treeBatchSize*cas.KeySize)
	}

	count := 0
	for _, add := range added {
		key, _ := hex.DecodeString(add)

		index := uint(key[0])
		keyArray[index] = appendBytes(keyArray[index], key[:])
		count++
	}

	// First create (empty) files if they do not already exist
	for i := 0; i < 0x100; i++ {
		filename := hiddenKeyFilename(i)
		if _, err := os.Stat(filename); os.IsNotExist(err) {

			f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC, 0666)
			if err != nil {
				return err
			}
			f.Close()
		}
	}

	// Append new keys to file
	for i := 0; i < 0x100; i++ {
		filename := hiddenKeyFilename(i)
		f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}

		if _, err = f.Write(keyArray[i]); err != nil {
			return err
		}
		f.Close()
	}

	return nil
}

type sortKeys [][]byte

func (s sortKeys) Less(i, j int) bool {
	for index := 0; index < cas.KeySize; index++ {
		if s[i][index] < s[j][index] {
			return true
		} else if s[i][index] > s[j][index] {
			return false
		}
	}
	return false
}

func (s sortKeys) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortKeys) Len() int {
	return len(s)
}

func sortKeysFunc(s [][]byte) [][]byte {
	sort.Sort(sortKeys(s))
	return s
}

func sortAndImportKeys(progressProcessing func(maxTicks int64)) error {

	keyFiles := []string{}

	// Find out number of keys in use
	for keyNr := 0; keyNr < 0x100; keyNr++ {

		keyfilename := hiddenKeyFilename(keyNr)

		if stat, err := os.Stat(keyfilename); err == nil {
			if stat.Size() > 0 {
				keyFiles = append(keyFiles, keyfilename)
			} else {
				// File is empty, nothing to process, so remove file already
				os.Remove(keyfilename)
			}
		}
	}

	progressProcessing(int64(len(keyFiles)))

	for _, keyfilename := range keyFiles {

		w1, _ := ioutil.ReadFile(keyfilename)
		array := make([][]byte, len(w1)/cas.KeySize)
		for i := 0; i < len(array); i++ {
			array[i] = w1[cas.KeySize*i : cas.KeySize*(i+1)]
		}
		w2 := sortKeysFunc(array)

		// Create arrays to be able to batch the operation
		keys := make([][]byte, 0, treeBatchSize)
		values := make([][]byte, 0, treeBatchSize)

		for i := 0; i < len(w2); i++ {
			keys = append(keys, w2[i])
			values = append(values, nil)

			if len(keys) == cap(keys) {
				err := kv.AddMultiToLevel1(keys, values, kv.BLOB)
				if err != nil {
					return err
				}

				keys = keys[:0]
				values = values[:0]
			}
		}

		if len(keys) > 0 {
			err := kv.AddMultiToLevel1(keys, values, kv.BLOB)
			if err != nil {
				return err
			}
		}

		// We are done, remove the file
		os.Remove(keyfilename)

		progressProcessing(int64(len(keyFiles)))
	}

	return nil
}
