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
	"io"
	"os"
	"errors"
	"io/ioutil"
	"github.com/s3git/s3git-go/internal/kv"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/backend"
	"github.com/s3git/s3git-go/internal/backend/s3"
	"encoding/hex"
)

func MakeReader(hash string) *Reader {
	cr := Reader{hash: hash}
	err := cr.open(hash)
	if err != nil {
		return nil
	}
	return &cr
}

type Reader struct {
	hash        string
	offset      int64
	leafNr      int
	leaves      []Key
	chunkBuf    []byte
	chunkOffset int
	chunkLast   bool
}

func openRoot(hash string) ([]Key, error) {

	key, _ := hex.DecodeString(hash)
	leafHashes, _, err := kv.GetLevel1(key)
	if err != nil {
		return nil, err
	}

	// Has blob already been pulled down to disk?
	if len(leafHashes) == 0 {

		var err error
		leafHashes, err = pullDownOnDemand(hash)
		if err != nil {
			return nil, err
		}
	}
	leaves := make([]Key, 0, len(leafHashes)/KeySize)
	for i := 0; i < len(leafHashes); i += KeySize {
		leaves = append(leaves, NewKey(leafHashes[i:i+KeySize]))
	}
	return leaves, nil
}

// Pull a blob on demand from the back end store
func pullDownOnDemand(hash string) ([]byte, error) {

	// TODO: [perf] implement streaming mode for large blobs, spawn off multiple GET range-headers

	var client backend.Backend

	// TODO: Remove hack to temporarily read from 100m bucket
	client = HackFor100mBucket(hash)
	if client == nil  {
		var err error
		client, err = backend.GetDefaultClient()
		if err != nil {
			return nil, err
		}
	}

	b, _ := hex.DecodeString(hash)
	_, objType, err := kv.GetLevel1(b)
	if err != nil {
		return nil, err
	}
	leafHashes, err := PullBlobDownToLocalDisk(hash, objType, client)
	if err != nil {
		return nil, err
	}

	return leafHashes, nil
}

// Hack to redirect S3 read to lifedrive-100m-usw2 bucket
func HackFor100mBucket(hash string) backend.Backend {

	// Following credentials have restricted access to just GetObject
	accessKey100mRestrictedPolicy := "AKIAJDXB2JULIRQQVHZQ"
	secretKey100mRestrictedPolicy := "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"

	var client backend.Backend
	if config.Config.Remotes[0].S3Bucket == "s3git-100m" ||
	config.Config.Remotes[0].S3Bucket == "s3git-100m-euc1-objs" ||
	config.Config.Remotes[0].S3Bucket == "s3git-100m-euc1-json" {

		client = s3.MakeClient(config.RemoteObject{S3Bucket: "lifedrive-100m-usw2", S3Region: "us-west-2",
			S3AccessKey: accessKey100mRestrictedPolicy, S3SecretKey: secretKey100mRestrictedPolicy})

		// Check whether blob is in lifedrive-100m-usw2 bucket, otherwise fall back to default
		exists, err := client.VerifyHash(hash)
		if err != nil {
			return nil
		}
		if !exists {
			client = nil	// fall back to default
		}
	}
	return client
}

// Open the root hash of a blob
func (cr *Reader) open(hash string) error {

	var err error
	cr.leaves, err = openRoot(hash)
	return err
}

// Read the contents of a blob
func (cr *Reader) Read(p []byte) (n int, err error) {

	// TODO: Consider returning EOF in case of errors/failed functions?

	bytesToRead := len(p)

	if cr.chunkLast && cr.chunkOffset == 0 {
		return 0, io.EOF
	}

	for {
		if cr.chunkBuf == nil {
			key := cr.leaves[cr.leafNr].String()
			// TODO: [perf] Consider using a memory pool and reusing chunks?

			// Check whether chunk is available on local disk, if not, pull down to local disk
			chunkFile := getBlobPath(key)
			if _, err := os.Stat(chunkFile); os.IsNotExist(err) {

				// Chunk is missing, load file from back end
				// TODO: [perf] Ideally optimize here to just get the missing chunk (not whole file)
				_, err = pullDownOnDemand(cr.hash)
				if err != nil {
					return 0, err
				}

				// Double check that missing chunk is now available
				if _, err := os.Stat(chunkFile); os.IsNotExist(err) {
					return 0, errors.New("Failed to fetch missing chunk from remote back end")
				}
			}

			cr.chunkBuf, err = ioutil.ReadFile(chunkFile)
			if err != nil {
				return 0, err
			}
			cr.chunkOffset = 0
			cr.leafNr++
			cr.chunkLast = cr.leafNr == len(cr.leaves) // Mark when we have the last chunk
		}

		remainingInBuffer := len(cr.chunkBuf) - cr.chunkOffset
		bytesRead := min(bytesToRead, remainingInBuffer)
		copy(p[len(p)-bytesToRead:], cr.chunkBuf[cr.chunkOffset:cr.chunkOffset+bytesRead])
		bytesToRead -= bytesRead
		remainingInBuffer -= bytesRead
		cr.chunkOffset += bytesRead

		if bytesToRead <= 0 {
			return bytesRead, nil
		}

		if remainingInBuffer <= 0 {
			if cr.chunkLast {
				return bytesRead, io.EOF
			}
			cr.chunkBuf = nil
			cr.chunkOffset = 0
		}
	}
}

// Fetch blob down to temp file in order to load
func FetchBlobToTempFile(hash string, client backend.Backend) (tempFile string, err error) {

	file, err := ioutil.TempFile("", "pull-")
	if err != nil {
		return "", err
	}

	err = client.DownloadWithWriter(hash, file)
	if err != nil {
		return "", err
	}
	name := file.Name()
	file.Close()

	return name, nil
}

// Store the blob in the caching area of the cas
func StoreBlobInCache(name, objType string) ([]byte, error) {

	in, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer in.Close()

	var cw *Writer
	if objType == kv.PREFIX {
		cw = MakeWriterInCheatMode(objType)
	} else {
		cw = MakeWriter(objType)
	}

	// For pulls write to the cache area (not stage)
	cw.setAreaDir(cacheDir)

	io.Copy(cw, in)

	_, leafHashes, _, err := cw.Flush()
	if err != nil {
		return nil, err
	}

	return leafHashes, nil
}

func PullBlobDownToLocalDisk(hash, objType string, client backend.Backend) ([]byte, error) {

	// TODO: [perf] Remove work around by using separate file
	name, err := FetchBlobToTempFile(hash, client)
	if err != nil {
		return nil, err
	}
	defer os.Remove(name)

	deduped, leaves, err := testForDedupedBlob(hash, name)
	if err != nil {
		return nil, err
	}

	if deduped {
		// TODO: [perf] Probably we do not want to fetch all blobs at once (maybe a couple), rather just the first one and let the others be fetched 'on demand'
		for _, l := range leaves {
			err := FetchLeafBlob(l.String(), client)
			if err != nil {
				return nil, err
			}
		}
		leafHashes := make([]byte, len(leaves)*KeySize)
		// TODO: [perf] Remove ugly 'recopying' back to byte array, instead return []Key directly
		for index, l := range leaves {
			copy(leafHashes[index*KeySize:(index+1)*KeySize], l.object[:])
		}
		return leafHashes, nil
	} else {
		return StoreBlobInCache(name, objType)
	}
}

// Test whether the blob has been stored in deduped manner (as opposed to hydrated manner)
func testForDedupedBlob(hash, filename string) (bool, []Key, error) {

	stat, err := os.Stat(filename)
	if err != nil {
		return false, nil, err
	}

	// Check whether the size is a multiple of 64 bytes
	if stat.Size() & (KeySize-1) != 0 {
		return false, nil, nil
	}

	// Otherwise compute the level 1 hash on the contents
	leafHashes, err := ioutil.ReadFile(filename)
	if err != nil {
		return false, nil, err
	}

	leaves := make([]Key, 0, len(leafHashes)/KeySize)
	for i := 0; i < len(leafHashes); i += KeySize {
		leaves = append(leaves, NewKey(leafHashes[i:i+KeySize]))
	}

	rootStr, err := computeRootBlake2(leaves)
	if err != nil {
		return false, nil, err
	}

	// Now if hashes equal it must be deduped format
	return hash == rootStr, leaves, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
