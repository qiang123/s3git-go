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

/*
 * Borrows ideas and code from https://github.com/bradfitz/gitbrute/blob/master/gitbrute.go
 */

package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/kv"
	"github.com/codahale/blake2"
	"strings"
	"runtime"
	"errors"
)

type prefixObject struct {
	coreObject
	S3gitFollowMe string `json:"s3gitFollowMe"`
	S3gitMagic    string `json:"s3gitMagic"`
}

func makePrefixObject(followMeHash string) *prefixObject {
	po := prefixObject{coreObject: coreObject{S3gitVersion: 1, S3gitType: kv.PREFIX}, S3gitFollowMe: followMeHash}
	return &po
}

var (
	//
	// With the additional constraint that objects are a multiple of 64 in size, this
	// adds 6 bits to the chance of getting a regular blob in the range, see table:
	//
	// PREFIX-SIZE | NR BITS | OCCURRENCE (MLD)
	//           5 |    20+6 |             0.1
	//           6 |    24+6 |             1.1
	//           7 |    28+6 |            17.2
	//           8 |    32+6 |             275
	//
	prefixChar  = '0'
	prefixNum   = 7
	prefixCheat = 3		// Number that is cheated in the prefix, like 0000xxx00000 -- will fail in file check mode
)

func prefix() string {
	return strings.Repeat(string(prefixChar), prefixNum)
}

// Get prefix object based on hash
func GetPrefixObject(hash string) (*prefixObject, error) {

	s, err := readBlob(hash)
	if err != nil {
		return nil, err
	}

	return GetPrefixObjectFromString(s)
}

// Get prefix object from string contents
func GetPrefixObjectFromString(s string) (*prefixObject, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	var po prefixObject
	if err := dec.Decode(&po); err != nil {
		return nil, err
	}

	return &po, nil
}


func StorePrefixObject(commitHash string) error {

	// Mine for proper start of the hash
	prefixJson, err := mine(commitHash)
	if err != nil {
		return err
	}

	// And save prefix object to disk
	cw := cas.MakeWriterInCheatMode(kv.PREFIX)
	fmt.Fprint(cw, prefixJson)

	// Flush the stream (and ignore hash)
	_, _, _, err = cw.Flush()
	if err != nil {
		return err
	}

	return nil
}

func mine(followMeHash string) (string, error) {

	prefixObject := makePrefixObject(followMeHash)

	buf := new(bytes.Buffer)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(prefixObject); err != nil {
		return "", err
	}

	obj := buf.String()
	magiclength := cas.KeySize - (len(obj) % cas.KeySize)

	if magiclength < cas.KeySize*3/4 { // Round up size of magic to next multiple of KeySize in case it is too short
		magiclength += cas.KeySize
	}
	possibilities := make(chan try, 512)
	done := make(chan struct{})
	go explore(possibilities, magiclength, done)

	winner := make(chan solution)

	for i := 0; i < int(runtime.NumCPU()); i++ {
		go bruteForce(obj, winner, possibilities, done)
	}

	w := <-winner
	close(done)

	return string(w.blob[:]), nil
}

func bruteForce(obj string, winner chan<- solution, possibilities <-chan try, done <-chan struct{}) {

	// blob is the blob to mutate in-place repeatedly while testing
	// whether we have a match.

	if len(obj) > cas.ChunkSize {
		// TODO: Need to split up stream in chunks
		panic(errors.New("Commit object too long"))
	}
	leafHash := blake2.New(&blake2.Config{Size: 64, Tree: &blake2.Tree{Fanout: 0, MaxDepth: 2, LeafSize: cas.ChunkSize, NodeOffset: 0, NodeDepth: 0, InnerHashSize: 64, IsLastNode: true}})
	rootHash := blake2.New(&blake2.Config{Size: 64, Tree: &blake2.Tree{Fanout: 0, MaxDepth: 2, LeafSize: cas.ChunkSize, NodeOffset: 0, NodeDepth: 1, InnerHashSize: 64, IsLastNode: true}})
	wantHexPrefix := []byte(prefix())[:prefixNum-prefixCheat]
	hexBuf := make([]byte, 0, cas.KeySizeHex)

	input := obj

	for t := range possibilities {
		select {
		case <-done:
			return
		default:
		// Round up blob size to multiple of 64 bytes
			blob := []byte(fmt.Sprintf("%s%s%s", input[:len(input)-3], t.magic, input[len(input)-3:]))

			leafHash.Reset()
			leafHash.Write(blob)
			digest := leafHash.Sum(hexBuf[:0])

			rootHash.Reset()
			rootHash.Write(digest)
			digest = rootHash.Sum(hexBuf[:0])

			hip := hexInPlace(digest)
			if !bytes.HasPrefix(hip, wantHexPrefix) ||
			rune(hip[prefixNum]) == prefixChar {			// Prevent accidentally gaining one 'free/by chance' extra level (so that we can safely distinguish between different lengths of prefixes)
				continue
			}

			winner <- solution{blob}
			return
		}
	}
}

// hexInPlace takes a slice of binary data and returns the same slice with double
// its length, hex-ified in-place.
func hexInPlace(v []byte) []byte {
	const hex = "0123456789abcdef"
	h := v[:len(v)*2]
	for i := len(v) - 1; i >= 0; i-- {
		b := v[i]
		h[i*2+0] = hex[b>>4]
		h[i*2+1] = hex[b&0xf]
	}
	return h
}

// try is a magic number looking for a matching prefix.
type try struct {
	magic string
}

// explore magic numbers
func explore(c chan<- try, magiclength int, done <-chan struct{}) {

	for magic := uint64(0); true; magic++ {
		select {
		case <-done:
			return
		default:
			c <- try{magicToStr(magic, magiclength)}
		}
	}
}

const letterBytes = "0123456789"

func magicToStr(magic uint64, n int) string {

	b := make([]byte, n)

	for i := n - 1; i >= 0; i-- {
		b[i] = letterBytes[int(magic%uint64(len(letterBytes)))]
		magic /= uint64(len(letterBytes))
	}

	return string(b[:])
}

type solution struct {
	blob []byte
}
