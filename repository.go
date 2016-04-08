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
	"bytes"
	"encoding/hex"
	"errors"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/kv"
	"io"
)

type Repository struct {
	Remotes []Remote
}

// Initialize a new repository
func InitRepository(path string) (*Repository, error) {

	config.SaveConfig(path)

	return OpenRepository(path)
}

// Open an existing repository
func OpenRepository(path string) (*Repository, error) {

	success, err := config.LoadConfig(path)
	if err != nil {
		return nil, err
	}
	if !success {
		return nil, errors.New("Not an s3git repository -- did you 's3git init' this directory?")
	}
	kv.OpenDatabase()

	return &Repository{}, nil
}

// Get the list of changes for a repository
func (repo Repository) Status() (<-chan string, error) {

	list, err := kv.ListStage()
	if err != nil {
		return nil, err
	}

	result := make(chan string)

	go func() {
		defer close(result)

		for l := range list {
			result <- hex.EncodeToString(l)
		}
	}()

	return result, nil
}

// List the contents of a repository
func (repo Repository) List(prefix string) (<-chan string, error) {

	result := make(chan string)

	go func() {
		// make sure we always close the channel
		defer close(result)

		keys, err := kv.ListLevel1Blobs(prefix)
		if err != nil {
			return
		}

		for key := range keys {
			result <- hex.EncodeToString(key)
		}
	}()

	return result, nil
}

// Add a stream to the repository
func (repo Repository) Add(r io.Reader) (string, bool, error) {

	cw := cas.MakeWriter(cas.BLOB)
	defer cw.Close()

	_, err := io.Copy(cw, r)
	if err != nil {
		return "", false, nil
	}

	rootKeyStr, _, newBlob, err := cw.Flush()
	if err != nil {
		return "", false, nil
	}

	// Check whether object is not already in repository
	if newBlob {
		// Add root key stage
		kv.AddToStage(rootKeyStr)
	}

	return rootKeyStr, newBlob, err
}

// Get a stream from the repository
func (repo Repository) Get(hash string) (io.Reader, error) {

	cr := cas.MakeReader(hash)
	if cr == nil {
		return nil, errors.New("Failed to create cas reader")
	}

	r, w := io.Pipe()

	go func(cr *cas.Reader, w io.WriteCloser) {

		defer w.Close()

		size := 0
		array := make([]byte, config.Config.ChunkSize)
		for {
			read, err := cr.Read(array)
			size += read
			if read > 0 {
				_, err := io.Copy(w, bytes.NewBuffer(array[:read]))
				if err != nil {
					panic(err)
				}
			}
			if err == io.EOF {
				break
			}
		}

	}(cr, w)

	return r, nil
}

type Statistics struct {
	Objects   uint64
	TotalSize uint64
}

// Get statistics for the repository
func (repo Repository) Statistics() (*Statistics, error) {

	entries, err := kv.GetLevel1BlobsStats()
	if err != nil {
		return nil, err
	}

	return &Statistics{Objects: entries, TotalSize: 0}, nil
}
