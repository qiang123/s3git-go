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
	"os"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/kv"
	"io"
	"strings"
	"path/filepath"
	"io/ioutil"
	"encoding/hex"
	"strconv"
)

// TODO: Implement gitignore like filtering
// TODO: Implement deletion of 'old' files for checkout
// TODO: Implement snapshot log
// TODO: Use commit hash instead of snapshot hash

const FileModeNoPerm = "100000" // Permissions still need to be ORred in
const DirectoryMode = "040000"

type snapshotObject struct {
	coreObject
	S3gitEntries    []SnapshotEntry `json:"s3gitEntries"`
	S3gitPadding    string   `json:"s3gitPadding"`
}

type SnapshotEntry struct {
	Mode string `json:"mode"`	// *nix filemode
	Name string `json:"name"`	// filename
	Blob string `json:"blob"`   // pointer to blob
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

func CheckoutSnapshot(hash string, fHydrate func(hash, filename string, perm os.FileMode)) error {

	path, _ := ioutil.TempDir("", "snapshot-checkout-")
	fmt.Println(path)

	hydrate := true

	return iterateSnapshots(hash, "", func(entry SnapshotEntry, base string) {
		fmt.Println(filepath.Join(path, base, entry.Name))

		if hydrate {

			perm, _ := strconv.ParseUint(entry.Mode[3:len(entry.Mode)], 8, 0)
			fHydrate(entry.Blob, filepath.Join(path, base, entry.Name), os.FileMode(perm))

		} else {
			hex, _ := hex.DecodeString(entry.Blob)
			leafHashes, _, err := kv.GetLevel1(hex)
			err = ioutil.WriteFile(filepath.Join(path, base, entry.Name), leafHashes, os.ModePerm)
			if err != nil {
				return
			}

		}

	}, func(base string) {
		err := os.MkdirAll(filepath.Join(path, base), os.ModePerm)
		if err != nil {
			return
		}
	})
}


func ListSnapshot(hash string, f func(entry SnapshotEntry, base string), fNewDir func(base string)) error {

	return iterateSnapshots(hash, "", f, fNewDir)
}

func iterateSnapshots(hash, path string, f func(entry SnapshotEntry, base string), fNewDir func(base string)) error {

	so, err := GetSnapshotObject(hash)
	if err != nil {
		return err
	}

	for _, entry := range so.S3gitEntries {
		if entry.Mode == DirectoryMode {
			fNewDir(filepath.Join(path, entry.Name))
			iterateSnapshots(entry.Blob, filepath.Join(path, entry.Name), f, fNewDir)
		} else {
			f(entry, path)
		}
	}
	return nil
}

func makeSnapshotObject(path string, addFn func(filename string) (string, error)) *snapshotObject {

	so := snapshotObject{coreObject: coreObject{S3gitVersion: 1, S3gitType: kv.SNAPSHOT}}

	path, err := filepath.Abs(path)
	if err != nil {
		return nil
	}

	fileList, err := filepath.Glob(filepath.Join(path, "*"))
	if err != nil {
		return nil
	}

	entries := []SnapshotEntry{}

	for _, filename := range fileList {

		stat, err := os.Stat(filename)
		if err != nil {
			return nil
		}

		var key string
		fmi, _ := strconv.ParseUint(FileModeNoPerm, 8, 0)
		filemode := fmt.Sprintf("%06o", os.FileMode(fmi) | stat.Mode().Perm())

		if stat.IsDir() {

			filemode = DirectoryMode
			var err error
			key, err = StoreSnapshotObject(filename, addFn)
			if err != nil {
				return nil
			}

		} else {

			var err error
			key, err = addFn(filename)
			if err != nil {
				return nil
			}
		}

		filenameLeaf, err := filepath.Rel(path, filename)
		if err != nil {
			return nil
		}

		entries = append(entries, SnapshotEntry{Mode: filemode, Name: filenameLeaf, Blob: key })
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
		return nil, errors.New(fmt.Sprint("Failed to read hash %s", hash))
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
