package core

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
	"github.com/s3git/s3git-go/internal/kv"
)

const COMMIT = "commit"

// Type to create a commit object
// - total size of json object is always a multiple of 64, so we are padding the object
// - structured as a json object
//   - json keys are in fix order
//     - in case a key contains an array, the values are sorted alphabetically

type commitObject struct {
	coreObject
	S3gitMessage     string   `json:"s3gitMessage"`     // Message describing the commit (optional)
	S3gitCommitter   string   `json:"s3gitCommitter"`   // Person doing the commit
	S3gitBranch      string   `json:"s3gitBranch"`      // Name of the branch
	S3gitTree        string   `json:"s3gitTree"`        // Tree object for the commit
	S3gitWarmParents []string `json:"s3gitWarmParents"` // List of parent commits up the (possibly split) chain
	S3gitColdParents []string `json:"s3gitColdParents"` // Parent commits that are no longer part of the chain
	S3gitTimeStamp   string   `json:"s3gitTimeStamp"`
	S3gitPadding     string   `json:"s3gitPadding"`
}

func makeCommitObject(message, branch, tree string, warmParents, coldParents []string) *commitObject {

	co := commitObject{coreObject: coreObject{S3gitVersion: 1, S3gitType: COMMIT}, S3gitMessage: message, S3gitBranch: branch,
		S3gitTree: tree, S3gitWarmParents: warmParents, S3gitColdParents: coldParents}

	// TODO: Read from git config
	// TODO: brackets are translated to \u003c respectively \u003e
	co.S3gitCommitter = "frankw <fwessels@xs4all.nl>"
	// TODO: Want to report as UTC or not (git includes a timezone)
	co.S3gitTimeStamp = time.Now(). /*.UTC()*/ Format(time.RFC3339Nano)
	return &co
}

func (co *commitObject) ParseTime() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, co.S3gitTimeStamp)
}

func isCommit(hash string) bool {

	// TODO: Verify that this hash is a commit object
	// 1. in case size of object is not a multiple of KeySize --> not a commit object
	// otherwise:
	// 2. try to load as json object --> ok: true
	return true
}

// Return commit object based on hash
func GetCommitObject(hash string) (*commitObject, error) {

	s, err := readBlob(hash)
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(strings.NewReader(s))
	var co commitObject
	if err := dec.Decode(&co); err != nil {
		return nil, err
	}

	return &co, nil
}

func StoreCommitObject(message, branch string, warmParents, coldParents []string, added <-chan []byte, removed []string) (hash string, empty bool, err error) {

	// Create a tree object for this commit
	treeObject := makeTreeObject(added, removed)
	if treeObject.empty() {
		return "", true, nil
	}
	// Store tree object on disk
	treeHash, err := treeObject.writeToDisk()
	if err != nil {
		return "", false, err
	}

	// Create commit object
	commitObject := makeCommitObject(message, branch, treeHash, warmParents, coldParents)

	buf := new(bytes.Buffer)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(commitObject); err != nil {
		return "", false, err
	}

	// Write to disk
	h, e := commitObject.write(buf, COMMIT)

	// Remove previous parent commits
	for _, parentCommit := range warmParents {
		kv.RemoveTopMostCommit(parentCommit)
	}
	for _, parentCommit := range coldParents {
		kv.RemoveTopMostCommit(parentCommit)
	}

	// Set commit as top most commit
	kv.AddTopMostCommit(h)
	return h, false, e
}
