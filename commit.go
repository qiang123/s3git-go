package s3git

import (
	"encoding/hex"
	"github.com/s3git/s3git-go/core"
	"github.com/s3git/s3git-go/kv"
)

type Commit struct {
	Hash    string
	Message string
}

// Perform a commit for the repository
func (repo Repository) Commit(message string) (hash string, empty bool, err error) {

	list, err := kv.ListStage()
	if err != nil {
		return "", false, err
	}

	commits, err := kv.ListTopMostCommits()
	if err != nil {
		return "", false, err
	}

	// TODO: Handle case with multiple parents
	parents := []string{}
	for c := range commits {
		parents = append(parents, hex.EncodeToString(c))
	}

	// Create commit object on disk
	commitHash, empty, err := core.StoreCommitObject(message, parents, list, []string{})
	if err != nil {
		return "", false, err
	}
	if empty {
		return "", true, nil
	}

	// Remove added blobs from staging area
	err = kv.ClearStage()
	if err != nil {
		return "", false, err
	}

	err = core.StorePrefixObject(commitHash)
	if err != nil {
		return "", false, err
	}

	return commitHash, false, nil
}

// List the commits for a repository
func (repo Repository) ListCommits() (<-chan Commit, error) {

	commits, err := kv.ListTopMostCommits()
	if err != nil {
		return nil, err
	}

	var commit string
	// TODO: Deal with multiple top most commits
	for c := range commits {
		commit = hex.EncodeToString(c)
	}

	result := make(chan Commit)

	go func() {
		defer close(result)

		for {
			co, err := core.GetCommitObject(commit)
			if err != nil {
				return
			}
			result <- Commit{Hash: commit, Message: co.S3gitMessage}

			if len(co.S3gitWarmParents) == 0 {
				break
			} else {
				// TODO: Deal with commits after first one
				commit = co.S3gitWarmParents[0]
			}
		}
	}()

	return result, nil
}
