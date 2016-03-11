package s3git

import (
	"encoding/hex"
	"errors"
	"time"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/s3git/s3git-go/internal/kv"
)

type Commit struct {
	Hash    string
	Message string
	TimeStamp string
	Parent	string
}

// Perform a commit for the repository
func (repo Repository) Commit(message string) (hash string, empty bool, err error) {
	return repo.commit(message, "master", []string{})
}

// Perform a commit for the named branch of the repository
func (repo Repository) CommitToBranch(message, branch string) (hash string, empty bool, err error) {
	return repo.commit(message, branch, []string{})
}

func (repo Repository) commit(message, branch string, parents []string) (hash string, empty bool, err error) {

	warmParents := []string{}
	coldParents := []string{}

	commits, err := kv.ListTopMostCommits()
	if err != nil {
		return "", false, err
	}

	if len(parents) == 0 {
		for c := range commits {
			warmParents = append(warmParents, hex.EncodeToString(c))
		}
		if len(warmParents) > 1 {
			// TODO: Do extra check whether the trees are the same, in that case we can safely ignore the warning
			return "", false, errors.New("Multiple top most commits founds as parents")
		}
	} else {
		for c := range commits {
			p := hex.EncodeToString(c)
			if contains(parents, p) {
				warmParents = append(warmParents, p)
			} else {
				coldParents = append(coldParents, p)
			}
		}
	}

	return repo.commitWithWarmAndColdParents(message, branch, warmParents, coldParents)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (repo Repository) commitWithWarmAndColdParents(message, branch string, warmParents, coldParents []string) (hash string, empty bool, err error) {

	list, err := kv.ListStage()
	if err != nil {
		return "", false, err
	}

	// Create commit object on disk
	commitHash, empty, err := core.StoreCommitObject(message, branch, warmParents, coldParents, list, []string{})
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
