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
	"bytes"
	"io"
	"os"
	"sync"

	"encoding/hex"
	"github.com/codahale/blake2"
	"github.com/s3git/s3git-go/internal/config"
)

// Worker routine for computing hash for a chunk
func calcChunkWorkers(chunks <-chan chunkInput, results chan<- chunkOutput) {

	for c := range chunks {

		blake := blake2.New(&blake2.Config{Size: KeySize, Tree: &blake2.Tree{Fanout: 0, MaxDepth: 2, LeafSize: config.Config.LeafSize, NodeOffset: uint64(c.part), NodeDepth: 0, InnerHashSize: KeySize, IsLastNode: c.lastChunk}})

		_, err := io.Copy(blake, bytes.NewBuffer(c.partBuffer))
		if err != nil {
			// TODO: fmt.Println("Failing to compute hash: ", err)
			results <- chunkOutput{digest: []byte(""), part: c.part}
		} else {
			digest := blake.Sum(nil)
			results <- chunkOutput{digest: digest, part: c.part}
		}
	}
}

type chunkInput struct {
	part       int
	partBuffer []byte
	lastChunk  bool
	leafSize   uint32
	level      int
}

type chunkOutput struct {
	digest []byte
	part   int
}

func calcStream(r io.Reader, fileSize int64) (digest []byte, err error) {

	var wg sync.WaitGroup
	chunks := make(chan chunkInput)
	results := make(chan chunkOutput)

	// Start one go routine per CPU
	// TODO: Get nr of cpus
	for i := 0; i < 8; /**cpu*/ i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			calcChunkWorkers(chunks, results)
		}()
	}

	// Push chunks onto input channel
	go func() {
		for part, totalSize := 0, int64(0); ; part++ {
			partBuffer := make([]byte, config.Config.LeafSize)
			n, err := r.Read(partBuffer)
			if err != nil {
				return
			}
			partBuffer = partBuffer[:n]

			totalSize += int64(n)
			lastChunk := uint32(n) < config.Config.LeafSize || uint32(n) == config.Config.LeafSize && totalSize == fileSize

			chunks <- chunkInput{part: part, partBuffer: partBuffer, lastChunk: lastChunk, leafSize: config.Config.LeafSize, level: 0}

			if lastChunk {
				break
			}
		}

		// Close input channel
		close(chunks)
	}()

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(results) // Close output channel
	}()

	// Create hash based on chunk number with digest of chunk
	// (number of chunks upfront is unknown for stdin stream)
	digestHash := make(map[int][]byte)
	for r := range results {
		digestHash[r.part] = r.digest
	}

	// Concatenate digests of chunks
	b := make([]byte, len(digestHash)*KeySize)
	for index, val := range digestHash {
		offset := KeySize * index
		copy(b[offset:offset+KeySize], val[:])
	}

	rootBlake := blake2.New(&blake2.Config{Size: KeySize, Tree: &blake2.Tree{Fanout: 0, MaxDepth: 2, LeafSize: config.Config.LeafSize, NodeOffset: 0, NodeDepth: 1, InnerHashSize: KeySize, IsLastNode: true}})

	// Compute top level digest
	rootBlake.Reset()
	_, err = io.Copy(rootBlake, bytes.NewBuffer(b))
	digest = rootBlake.Sum(nil)

	return digest, nil
}

func Sum(filename string) (string, error) {

	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fileInfo, err := f.Stat()
	if err != nil {
		return "", err
	}
	fileSize := fileInfo.Size()

	digest, err := calcStream(f, fileSize)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(digest), nil
}
