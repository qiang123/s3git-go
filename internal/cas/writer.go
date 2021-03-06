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
	"github.com/codahale/blake2"
	"path"
	"errors"
	"strings"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/kv"
	"encoding/hex"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"fmt"
)

func MakeWriter(objType string) *Writer {
	cw := Writer{areaDir: stageDir, objType: objType}
	cw.chunkBuf = make([]byte, config.Config.LeafSize)
	return &cw
}

// Use cheat mode for prefix objects
func MakeWriterInCheatMode(objType string) *Writer {
	cw := MakeWriter(objType)
	cw.cheatMode = true
	return cw
}

type Writer struct {
	cheatMode   bool
	leaves      []Key
	chunkBuf    []byte
	chunkOffset uint32
	objType     string
	areaDir		string
	flushed		bool
}

func (cw *Writer) setAreaDir(dir string) {
	cw.areaDir = dir
}

func (cw *Writer) Write(p []byte) (nn int, err error) {

	for bytesToWrite := uint32(len(p)); bytesToWrite > 0; {

		if cw.chunkOffset == config.Config.LeafSize {
			// Write out full chunk (without last chunk marker)
			cw.flush(false)
		}

		if cw.chunkOffset + bytesToWrite < config.Config.LeafSize {
			copy(cw.chunkBuf[cw.chunkOffset:], p[uint32(len(p))-bytesToWrite:])
			cw.chunkOffset += bytesToWrite
			bytesToWrite = 0
		} else {
			bytesWritten := config.Config.LeafSize - cw.chunkOffset
			copy(cw.chunkBuf[cw.chunkOffset:], p[uint32(len(p))-bytesToWrite:uint32(len(p))-bytesToWrite+bytesWritten])
			bytesToWrite -= bytesWritten
			cw.chunkOffset += bytesWritten
		}
	}

	return len(p), nil
}

// Write leaf node to disk
func (cw *Writer) flush(isLastNode bool) {

	blake2 := blake2.New(&blake2.Config{Size: 64, Tree: &blake2.Tree{Fanout: 0, MaxDepth: 2, LeafSize: config.Config.LeafSize, NodeOffset: uint64(len(cw.leaves)), NodeDepth: 0, InnerHashSize: 64, IsLastNode: isLastNode}})
	blake2.Write(cw.chunkBuf[:cw.chunkOffset])

	leafKey := NewKey(blake2.Sum(nil))
	cw.leaves = append(cw.leaves, leafKey)

	// Create file
	chunkWriter, err := createLeafNodeFile(leafKey.String(), cw.areaDir)
	if err != nil {
		return
	}

	// Write leaf blob contents to file
	chunkWriter.Write(cw.chunkBuf[:cw.chunkOffset])
	defer chunkWriter.Close()
	chunkWriter.Sync()

	// Add size of leaf to KV store
	err = addLeafBlobFileToKV(leafKey.String(), cw.areaDir, cw.chunkOffset)
	if err != nil {
		return
	}

	// Start over in buffer
	cw.chunkOffset = 0
}

func (cw *Writer) Flush() (string, []byte, bool, error) {

	// Close last node
	cw.flush(true)

	cw.flushed = true

	rootStr, err := computeRootBlake2(cw.leaves)
	if err != nil {
		return "", nil, false, err
	}

	if cw.cheatMode {
		repeatLastChar := strings.Repeat(string(rootStr[prefixNum-prefixCheat-1]), prefixCheat)
		rootStr = rootStr[0:prefixNum-prefixCheat] + repeatLastChar + rootStr[prefixNum:]
	}

	leafHashes := make([]byte, len(cw.leaves)*KeySize)
	for index, leave := range cw.leaves {
		offset := KeySize * index
		copy(leafHashes[offset:offset+KeySize], leave.object[:])
	}

	key, _ := hex.DecodeString(rootStr)
	val, _, err := kv.GetLevel1(key)
	if err != nil && !lmdb.IsNotFound(err) {
		return "", nil, false, err
	}
	newBlob := lmdb.IsNotFound(err) ||
	           len(val) == 0	// Also write leafHashes to KV database when no leafHashes were stored previously (BLOB was not pulled down)

	if newBlob {
		err := kv.AddToLevel1(key, leafHashes, cw.objType)
		if err != nil {
			return "", nil, false, err
		}
	}

	return rootStr, leafHashes, newBlob, nil
}

// Compute the root blake key
func computeRootBlake2(leaves []Key) (string, error) {

	// Compute hash of level 1 root key
	blake2 := blake2.New(&blake2.Config{Size: 64, Tree: &blake2.Tree{Fanout: 0, MaxDepth: 2, LeafSize: config.Config.LeafSize, NodeOffset: 0, NodeDepth: 1, InnerHashSize: 64, IsLastNode: true}})

	// Iterate over hashes of all underlying nodes
	for _, leave := range leaves {
		blake2.Write(leave.object[:])
	}

	return NewKey(blake2.Sum(nil)).String(), nil
}

func (cw *Writer) Close() error {
	if !cw.flushed {
		return errors.New("Stream closed without being flushed!")
	}
	return nil
}

// Create a file for a leaf node
func createLeafNodeFile(hash, areaDir string) (*os.File, error) {

	checkRepoSize()

	hashDir := path.Join(config.Config.BasePath, config.S3GIT_DIR, areaDir, hash[0:2], hash[2:4]) + "/"
	err := os.MkdirAll(hashDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return os.Create(hashDir + hash[4:])
}

func addLeafBlobFileToKV(hash, areaDir string, size uint32) error {

	if areaDir == stageDir {
		return kv.AddLevel0Stage(hash, size)
	} else if areaDir == cacheDir {
		return kv.AddLevel0Cache(hash, size)
	} else {
		return errors.New(fmt.Sprintf("Undefined area: %s", areaDir))
	}
}