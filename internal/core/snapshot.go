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

package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/kv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// TODO: Implement gitignore like filtering

const FileModeNoPerm = "100000" // Permissions still need to be ORred in
const DirectoryMode = "040000"

type snapshotObject struct {
	coreObject
	S3gitEntries []SnapshotEntry `json:"s3gitEntries"`
	S3gitPadding string          `json:"s3gitPadding"`
}

type SnapshotEntry struct {
	Mode string `json:"mode"` // *nix filemode
	Name string `json:"name"` // filename
	Blob string `json:"blob"` // pointer to blob
}

func (sse *SnapshotEntry) IsDirectory() bool {
	return sse.Mode[0:3] == DirectoryMode[0:3]
}

func StoreSnapshotObject(path string, addFn func(filename string) (string, error)) (hash string, err error) {

	// Create snapshot object
	snapshotObject := makeSnapshotObject(path, addFn)
	if snapshotObject == nil {
		return "", errors.New("Failed to create snapshot object")
	}

	buf := new(bytes.Buffer)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(snapshotObject); err != nil {
		return "", err
	}

	// Write to disk
	return snapshotObject.write(buf, kv.SNAPSHOT)
}

func SnapshotCheckout(path, hash string, fWrite func(hash, filename string, perm os.FileMode)) error {

	err := warmCacheForCheckout(hash)
	if err != nil {
		return err
	}

	return iterateSnapshots(hash, path, func(entry SnapshotEntry, base string) {

		perm, _ := strconv.ParseUint(entry.Mode[3:len(entry.Mode)], 8, 0)
		fWrite(entry.Blob, filepath.Join(base, entry.Name), os.FileMode(perm))

	}, func(base string) {
		err := os.MkdirAll(filepath.Join(base), os.ModePerm)
		if err != nil {
			return
		}
	}, func(base string, entries []SnapshotEntry) {

		fileList, err := getFileList(base)
		if err != nil {
			return
		}

		fileRemaining, entryRemaining := getDifferenceForEntries(fileList, entries)

		for _, remove := range fileRemaining {
			err := os.RemoveAll(filepath.Join(base, remove))
			if err != nil {
				return
			}
		}

		if len(entryRemaining) > 0 {
			// TODO: Log error, this should not occur as any 'to be added' files should already have been created (upon leaving the directory)
			return
		}
	})
}

// Warm the cache for a given snapshot by pulling blobs in parallel
func warmCacheForCheckout(hash string) error {

	if len(config.Config.Remotes) == 0 {
		return nil
	}
	const pullBlobsRoutines = 50

	var wgDirs, wgBlobs sync.WaitGroup
	var chanDirs = make(chan string, pullBlobsRoutines*4)
	var chanBlobs = make(chan string, pullBlobsRoutines*4)
	var chanErrors = make(chan error, pullBlobsRoutines*4)

	for i := 0; i < 10; i++ {

		// Start routines for iterating over the snapshot hierarchy
		go func() {
			for hash := range chanDirs {

				func () {
					defer wgDirs.Done()

					so, err := GetSnapshotObject(hash)
					if err != nil {
						chanErrors <- err
						return
					}

					for _, entry := range so.S3gitEntries {
						if entry.IsDirectory() {
							// Push new snapshot object
							wgDirs.Add(1)
							chanDirs <- entry.Blob
						} else {
							// Push new blob to pull down
							wgBlobs.Add(1)
							chanBlobs <- entry.Blob
						}
					}
				}()
			}
		}()
	}

	for i := 0; i < pullBlobsRoutines; i++ {

		// Start routines for pulling down the blobs
		go func() {
			for hashBlob := range chanBlobs {

				func () {
					defer wgBlobs.Done()

					_, err := cas.PullDownOnDemand(hashBlob)
					if err != nil {
						chanErrors <- err
						return
					}
				}()
			}
		}()
	}

	// Push hash for root snapshot object to start processing
	wgDirs.Add(1)
	wgBlobs.Add(1)	// 'Fake' blobs waitgroup into minimally staying open until Dirs are all processed (see below)
	chanDirs <- hash

	go func() {
		wgDirs.Wait()
		wgBlobs.Done()	// Signal to blobs waitgroup that it can close
		close(chanDirs)
	}()

	go func() {
		wgBlobs.Wait()
		close(chanBlobs)
		close(chanErrors)
	}()

	var err error
	for e := range chanErrors {
		if e != nil {
			err = e
		}
	}

	return err
}

func SnapshotStatus(path, hash string) error {

	modified, added, removed := []string{}, []string{}, []string{}

	err := iterateSnapshots(hash, path, func(entry SnapshotEntry, base string) {

		deduped, _, _, err := cas.CheckLevel1HashFollowedByLeafHashes(filepath.Join(base, entry.Name))
		if err != nil {
			return
		}
		if deduped {
			// Nothing to do, file has not been modified
		} else {

			// Compute sum over contents of file...
			digest, err := cas.Sum(filepath.Join(base, entry.Name))
			if err != nil {
				return
			}

			// ... and check to entry.Blob
			if digest != entry.Blob {
				modified = append(modified, filepath.Join(base, entry.Name))
			}
		}

	}, func(base string) {
	}, func(base string, entries []SnapshotEntry) {

		fileList, err := getFileList(base)
		if err != nil {
			return
		}

		fileAdded, fileRemoved := getDifferenceForEntries(fileList, entries)

		for _, fileNew := range fileAdded {
			added = append(added, filepath.Join(base, fileNew))
		}
		for _, fileRemoved := range fileRemoved {
			removed = append(removed, filepath.Join(base, fileRemoved))
		}
	})
	if err != nil {
		return err
	}

	for _, fileNew := range added {
		fmt.Println("     New:", fileNew)
	}
	for _, fileModified := range modified {
		fmt.Println("Modified:", fileModified)
	}
	for _, fileRemoved := range removed {
		fmt.Println(" Removed:", fileRemoved)
	}

	return nil
}

func SnapshotList(hash string, fFile func(entry SnapshotEntry, base string), fEnteringDir func(base string), fLeavingDir func(base string, entries []SnapshotEntry)) error {

	return iterateSnapshots(hash, "", fFile, fEnteringDir, fLeavingDir)
}

func iterateSnapshots(hash, path string, fFile func(entry SnapshotEntry, base string), fEnteringDir func(base string), fLeavingDir func(base string, entries []SnapshotEntry)) error {

	// TODO: [perf] See if we can speed this up by running in parallel

	so, err := GetSnapshotObject(hash)
	if err != nil {
		return err
	}

	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}

	fEnteringDir(path)

	for _, entry := range so.S3gitEntries {
		if entry.Mode == DirectoryMode {
			iterateSnapshots(entry.Blob, filepath.Join(path, entry.Name), fFile, fEnteringDir, fLeavingDir)
		} else {
			fFile(entry, path)
		}
	}
	fLeavingDir(path, so.S3gitEntries)

	return nil
}

func makeSnapshotObject(path string, addFn func(filename string) (string, error)) *snapshotObject {

	// Create snapshot object
	so := snapshotObject{coreObject: coreObject{S3gitVersion: 1, S3gitType: kv.SNAPSHOT}}

	path, err := filepath.Abs(path)
	if err != nil {
		return nil
	}

	// Get list of files to add to snapshot object
	fileList, err := getFileList(path)
	if err != nil {
		return nil
	}

	entries := []SnapshotEntry{}

	// Iterate over list of files
	for _, filename := range fileList {

		// Get details of file
		stat, err := os.Stat(filename)
		if err != nil {
			return nil
		}

		var key string
		fmi, _ := strconv.ParseUint(FileModeNoPerm, 8, 0)
		filemode := fmt.Sprintf("%06o", os.FileMode(fmi)|stat.Mode().Perm())

		if stat.IsDir() { // for directories

			filemode = DirectoryMode
			var err error
			// Create snapshot object for (sub)directory
			key, err = StoreSnapshotObject(filename, addFn)
			if err != nil {
				return nil
			}

		} else { // for files

			var err error
			key, err = addFn(filename)
			if err != nil {
				return nil
			}
		}

		// Add entry to table of snapshot object
		filenameLeaf, err := filepath.Rel(path, filename)
		if err != nil {
			return nil
		}

		entries = append(entries, SnapshotEntry{Mode: filemode, Name: filenameLeaf, Blob: key})
	}

	so.S3gitEntries = entries

	return &so
}

func (so *snapshotObject) writeToDisk() (string, error) {

	buf := new(bytes.Buffer)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(so); err != nil {
		return "", err
	}

	return so.write(buf, kv.SNAPSHOT)
}

// Return snapshot object based on hash
func GetSnapshotObject(hash string) (*snapshotObject, error) {

	cr := cas.MakeReader(hash)
	if cr == nil {
		return nil, errors.New(fmt.Sprint("Failed to read hash", hash))
	}

	buf := bytes.NewBuffer(nil)
	io.Copy(buf, cr)

	s := string(buf.Bytes())

	return GetSnapshotObjectFromString(s)
}

// Return snapshot object from string contents
func GetSnapshotObjectFromString(s string) (*snapshotObject, error) {

	dec := json.NewDecoder(strings.NewReader(s))
	var so snapshotObject
	if err := dec.Decode(&so); err != nil {
		return nil, err
	}

	return &so, nil
}

func getDifferenceForEntries(filelist []string, entries []SnapshotEntry) ([]string, []string) {

	fileOnly, entryOnly := []string{}, []string{}
	m := map[string]int{}

	for _, filePath := range filelist {

		_, file := filepath.Split(filePath)
		m[file] = 1
	}
	for _, entry := range entries {
		m[entry.Name] = m[entry.Name] + 2
	}

	for key, val := range m {
		if val == 1 {
			fileOnly = append(fileOnly, key)
		} else if val == 2 {
			entryOnly = append(entryOnly, key)
		}
	}

	return fileOnly, entryOnly
}

func getFileList(path string) ([]string, error) {

	fileList, err := filepath.Glob(filepath.Join(path, "*"))
	if err != nil {
		return []string{}, err
	}
	return filterIgnoresFromFilelist(fileList), nil
}

func filterIgnoresFromFilelist(filelist []string) []string {

	// Iterate backwards to avoid deleting from while iterating over the slice
	for i := len(filelist) - 1; i >= 0; i-- {
		_, f := filepath.Split(filelist[i])
		if f == ".s3git" || f == ".s3git.config" {
			filelist = append(filelist[:i], filelist[i+1:]...)
		}
	}

	return filelist
}
