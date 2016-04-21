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
	"os"
	"fmt"
	"path/filepath"
	"github.com/s3git/s3git-go/internal/kv"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/s3git/s3git-go/internal/backend"
	"github.com/s3git/s3git-go/internal/backend/s3"
	"io"
	"errors"
	"encoding/hex"
)

// Create a snapshot for the repository
func (repo Repository) SnapshotCreate(path, message string) (hash string, empty bool, err error) {

	// Create snapshot
	snapshot, err := core.StoreSnapshotObject(path, func(filename string) (string, error) {

		// Test for deduped storage of file, then we can safely skip it
		deduped, key, _, err := cas.CheckLevel1HashFollowedByLeafHashes(filename)
		if err != nil {
			return "", err
		}
		if deduped { // It is stored in deduped format, so return key immediately
			return key, nil
		}

		// Otherwise compute the hash based on the contents of the file
		file, err := os.Open(filename)
		if err != nil {
			return "", err
		}
		defer file.Close()

		key, _, err = repo.Add(file)
		return key, err
	})
	if err != nil {
		return "", false, err
	}

	// TODO: Make sure we commit a new object, even when no new blobs added (ListStage is empty)
	return repo.commit(message, "master", snapshot, []string{})
}

// Checkout a snapshot for the repository
func (repo Repository) SnapshotCheckout(path, commit string, dedupe bool) error {

	snapshot, err := getSnapshotFromCommit(commit)
	if err != nil {
		return err
	}

	// TODO: Check that status is clean (create 'stashing' like behaviour for temp changes?)

	// For dedupe is false  --> store full contents
	fWriteHydrate := func(hash, filename string, mode os.FileMode) {

		// Compute hash in order to prevent rewriting the content when file already exists
		if _, err := os.Stat(filename); err == nil {

			digest, err := cas.Sum(filename)
			if err != nil {
				return
			}

			if digest == hash {	// Contents unchanged --> exit out early
				return
			}
		}

		r, err := repo.Get(hash)
		if err != nil {
			return
		}

		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
		if err != nil {
			return
		}

		io.Copy(f, r)
	}

	// For dedupe is true --> store all leaf hashes as contents followed by final 64 bytes with level 1 hash
	fWriteDeduped := func(hash, filename string, mode os.FileMode) {

		// TODO: Can we prevent (re)writing the content (run a check when file exists)?

		cas.WriteLevel1HashFollowedByLeafHashes(hash, filename, mode)
	}

	var fWrite func(hash, filename string, perm os.FileMode)
	if dedupe {
		fWrite = fWriteDeduped
	} else {
		fWrite = fWriteHydrate
	}

	return core.SnapshotCheckout(path, snapshot, fWrite)
}

type snapshotListOptions struct {
	showHash bool
	presignedUrls bool
	jsonOutput bool
}

func SnapshotListOptionSetShowHash(showHash bool) func(optns *snapshotListOptions) {
	return func(optns *snapshotListOptions) {
		optns.showHash = showHash
	}
}

func SnapshotListOptionSetPresignedUrls(presignedUrls bool) func(optns *snapshotListOptions) {
	return func(optns *snapshotListOptions) {
		optns.presignedUrls = presignedUrls
	}
}

func SnapshotListOptionSetJsonOutput(jsonOutput bool) func(optns *snapshotListOptions) {
	return func(optns *snapshotListOptions) {
		optns.jsonOutput = jsonOutput
	}
}

type SnapshotListOptions func(*snapshotListOptions)

// List a snapshot for the repository
func (repo Repository) SnapshotList(commit string, options ...SnapshotListOptions) error {

	optns := &snapshotListOptions{}
	for _, op := range options {
		op(optns)
	}

	snapshot, err := getSnapshotFromCommit(commit)
	if err != nil {
		return err
	}

	funcPresignedUrl := func(hash string) (string, error) { return "", nil }
	if optns.presignedUrls {
		client, err := backend.GetDefaultClient()
		if err != nil {
			return err
		}

		// Look for S3 back end
		s3, ok := client.(*s3.Client)
		if ok {
			// And if so generate function to generate presigned url
			funcPresignedUrl = func(hash string) (string, error) {
				return s3.GetPresignedUrl(hash)
			}
		}
	}

	// List snapshot
	err = core.SnapshotList(snapshot, func(entry core.SnapshotEntry, base string) {

		// TODO: Dump result in JSON format if requested

		url, _ := funcPresignedUrl(entry.Blob)
		if url != "" {
			fmt.Printf("%s --> %s\n", filepath.Join(base, entry.Name), url)
		} else {
			fmt.Println(filepath.Join(base, entry.Name))
		}
	}, func(base string) {}, func(base string, entries []core.SnapshotEntry) {})

	return err
}

// Show status for a snapshot of the repository
func (repo Repository) SnapshotStatus(path, commit string) error {

	snapshot, err := getSnapshotFromCommit(commit)
	if err != nil {
		return err
	}

	// Get status of snapshot
	err = core.SnapshotStatus(path, snapshot)

	return err
}

func getSnapshotFromCommit(commit string) (string, error) {

	if commit == "" {	// Unspecified, so default to last commit
		commits, err := kv.ListTopMostCommits()
		if err != nil {
			return "", err
		}
		parents := []string{}
		for c := range commits {
			parents = append(parents, hex.EncodeToString(c))
		}
		if len(parents) == 1 {
			commit = parents[0]
		} else {
			// TODO: Do extra check whether the trees are the same, in that case we can safely ignore the warning
			return "", errors.New("Multiple top most commits founds as parents")
		}
	} else {

		result := make(chan string)

		go func() {
			// make sure we always close the channel
			defer close(result)

			keys, err := kv.ListLevel1Commits(commit)
			if err != nil {
				return
			}

			for key := range keys {
				result <- hex.EncodeToString(key)
			}
		}()

		var err error
		commit, err = getUnique(result)
		if err != nil {
			return "", err
		}
	}

	co, err := core.GetCommitObject(commit)
	if err != nil {
		return "", err
	}

	if co.S3gitSnapshot == "" {
		return "", errors.New(fmt.Sprintf("Commit %s does not contain snapshot", commit))
	}

	return co.S3gitSnapshot, nil
}