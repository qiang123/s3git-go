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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/kv"
	"io"
	"sort"
	"strings"
)

type treeObject struct {
	coreObject
	S3gitAdded      []string `json:"s3gitAdded"`
	S3gitRemoved    []string `json:"s3gitRemoved"`
	S3gitPadding    string   `json:"s3gitPadding"`
	S3gitBlobUrl    string   `json:"s3gitBlobUrl"`    // URL to access blob (when different from tree object itself)
	S3gitBlobRegion string   `json:"s3gitBlobRegion"` // URL to access blob (when different from tree object itself)
}

func makeTreeObject(added <-chan []byte, removed []string) *treeObject {
	to := treeObject{coreObject: coreObject{S3gitVersion: 1, S3gitType: kv.TREE}}

	addedArray := []string{}

	for k := range added {
		addedArray = append(addedArray, hex.EncodeToString(k))
	}

	sort.Strings(addedArray)
	to.S3gitAdded = addedArray

	sort.Strings(removed)
	to.S3gitRemoved = removed

	return &to
}

func (to *treeObject) empty() bool {
	return len(to.S3gitAdded) == 0 && len(to.S3gitRemoved) == 0
}

func (to *treeObject) writeToDisk() (string, error) {

	buf := new(bytes.Buffer)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(to); err != nil {
		return "", err
	}

	return to.write(buf, kv.TREE)
}

// Return tree object based on hash
func GetTreeObject(hash string) (*treeObject, error) {

	cr := cas.MakeReader(hash)
	if cr == nil {
		return nil, errors.New(fmt.Sprint("Failed to read hash %s", hash))
	}

	buf := bytes.NewBuffer(nil)
	io.Copy(buf, cr)

	s := string(buf.Bytes())

	return GetTreeObjectFromString(s)
}

// Return tree object from string contents
func GetTreeObjectFromString(s string) (*treeObject, error) {

	dec := json.NewDecoder(strings.NewReader(s))
	var to treeObject
	if err := dec.Decode(&to); err != nil {
		return nil, err
	}

	return &to, nil
}
