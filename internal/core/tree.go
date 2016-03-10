package core

import (
	"bytes"
	"encoding/json"
	"sort"
	"encoding/hex"
)

const TREE="tree"

type treeObject struct {
	coreObject
	S3gitAdded   []string `json:"s3gitAdded"`
	S3gitRemoved []string `json:"s3gitRemoved"`
	S3gitPadding string   `json:"s3gitPadding"`
}

func makeTreeObject(added <-chan []byte, removed []string) *treeObject {
	to := treeObject{coreObject: coreObject{S3gitVersion: 1, S3gitType: TREE}}

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

	return to.write(buf, TREE)
}
