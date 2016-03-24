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

package s3git

import (
	"fmt"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestEmptyCommit(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	hash, empty, err := repo.Commit("test empty commit")
	assert.Nil(t, err)
	assert.True(t, empty, "Empty commit expected")
	assert.Empty(t, hash, "Hash for empty commit should be empty")
}

func TestSingleCommit(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	repo.Add(strings.NewReader("hello s3git"))

	hash, empty, err := repo.Commit("1st commit")
	assert.Nil(t, err)
	assert.False(t, empty, "Commit is not empty")
	assert.NotEmpty(t, hash, "Expected hash for non-empty commit")
}

func TestTwoCommits(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	repo.Add(strings.NewReader("hello s3git"))
	repo.Commit("1st commit")

	repo.Add(strings.NewReader("another hello to s3git"))

	hash, empty, err := repo.Commit("2nd commit")
	assert.Nil(t, err)
	assert.False(t, empty, "Commit is not empty")
	assert.NotEmpty(t, hash, "Expected hash for non-empty commit")

	hash2, empty2, err := repo.Commit("test empty commit")
	assert.Nil(t, err)
	assert.True(t, empty2, "Empty commit expected")
	assert.Empty(t, hash2, "Hash for empty commit should be empty")
}

func TestListThreeCommits(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	repo.Add(strings.NewReader("hello s3git"))
	repo.Commit("1st commit")

	repo.Add(strings.NewReader("another hello to s3git"))
	repo.Commit("2nd commit")

	repo.Add(strings.NewReader("yet another hello to s3git"))
	repo.Commit("3rd commit")

	list, err := repo.ListCommits("")
	assert.Nil(t, err)

	count := 0
	for l := range list {
		if count == 0 {
			assert.Equal(t, "3rd commit", l.Message, "3rd commit message is not correct")
		} else if count == 1 {
			assert.Equal(t, "2nd commit", l.Message, "2nd commit message is not correct")
		} else if count == 2 {
			assert.Equal(t, "1st commit", l.Message, "1st commit message is not correct")
		}
		count++
	}
}

func TestForkedCommitsJoiningBoth(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	repo.Add(strings.NewReader("hello s3git"))
	repo.Commit("1st commit")

	repo.Add(strings.NewReader("another hello to s3git"))
	parent, _, _ := repo.Commit("2nd commit")

	repo.Add(strings.NewReader("yet another hello to s3git"))
	forkParent1, _, _ := repo.Commit("3rd commit")

	repo.Add(strings.NewReader("this is for a fork at the 2nd commit"))
	forkParent2, _, _ := repo.commitWithWarmAndColdParents("forked at 2nd commit", "master", []string{parent}, []string{})

	repo.Add(strings.NewReader("joining both forks"))
	hash, _, err := repo.commit("5th commit joining both forks", "master", []string{forkParent1, forkParent2})
	assert.Nil(t, err)

	co, _ := core.GetCommitObject(hash)
	assert.Contains(t, co.S3gitWarmParents, forkParent1, "forkParent1 not found")
	assert.Contains(t, co.S3gitWarmParents, forkParent2, "forkParent2 not found")
	assert.Len(t, co.S3gitColdParents, 0, "There should be no cold parents")

	list, err := repo.ListCommits("")
	assert.Nil(t, err)

	count := 0
	for l := range list {
		fmt.Println(l.Message)
		//if count == 0 {
		//	assert.Equal(t, "5th commit joining just fork 1", l.Message, "5th commit message is not correct")
		//} else if count == 1 {
		//	assert.Equal(t, "3rd commit", l.Message, "3rd commit message is not correct")
		//} else if count == 2 {
		//	assert.Equal(t, "2nd commit", l.Message, "2nd commit message is not correct")
		//} else if count == 3 {
		//	assert.Equal(t, "1st commit", l.Message, "1st commit message is not correct")
		//}
		count++
	}
}

func TestForkedCommitsJoiningWithOneCold(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	repo.Add(strings.NewReader("hello s3git"))
	repo.Commit("1st commit")

	repo.Add(strings.NewReader("another hello to s3git"))
	parent, _, _ := repo.Commit("2nd commit")

	repo.Add(strings.NewReader("yet another hello to s3git"))
	forkParent1, _, _ := repo.Commit("3rd commit")

	repo.Add(strings.NewReader("this is for a fork at the 2nd commit"))
	forkParent2, _, _ := repo.commitWithWarmAndColdParents("forked at 2nd commit", "master", []string{parent}, []string{})

	repo.Add(strings.NewReader("selecting fork 1 to join"))
	hash, _, err := repo.commit("5th commit joining just fork 1", "master", []string{forkParent1})
	assert.Nil(t, err)

	co, _ := core.GetCommitObject(hash)
	assert.Contains(t, co.S3gitWarmParents, forkParent1, "forkParent1 not found")
	assert.Contains(t, co.S3gitColdParents, forkParent2, "forkParent2 not found")
	assert.Len(t, co.S3gitWarmParents, 1, "There should be one warm parent")
	assert.Len(t, co.S3gitColdParents, 1, "There should be one cold parent")

	list, err := repo.ListCommits("")
	assert.Nil(t, err)

	count := 0
	for l := range list {
		if count == 0 {
			assert.Equal(t, "5th commit joining just fork 1", l.Message, "5th commit message is not correct")
		} else if count == 1 {
			assert.Equal(t, "3rd commit", l.Message, "3rd commit message is not correct")
		} else if count == 2 {
			assert.Equal(t, "2nd commit", l.Message, "2nd commit message is not correct")
		} else if count == 3 {
			assert.Equal(t, "1st commit", l.Message, "1st commit message is not correct")
		}
		count++
	}
}

func TestForkedCommitsSplit(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	repo.Add(strings.NewReader("hello s3git"))
	repo.Commit("1st commit")

	repo.Add(strings.NewReader("another hello to s3git"))
	parent, _, _ := repo.Commit("2nd commit")

	repo.Add(strings.NewReader("yet another hello to s3git"))
	repo.Commit("3rd commit")

	repo.Add(strings.NewReader("this is for a fork at the 2nd commit"))
	repo.commitWithWarmAndColdParents("forked at 2nd commit", "master", []string{parent}, []string{})

	list, err := repo.ListCommits("")
	assert.Nil(t, err)

	count := 0
	for l := range list {
		if count == 0 {
			assert.Equal(t, "forked at 2nd commit", l.Message, "4th commit message is not correct")
		} else if count == 1 {
			assert.Equal(t, "3rd commit", l.Message, "3rd commit message is not correct")
		} else if count == 2 {
			assert.Equal(t, "2nd commit", l.Message, "2nd commit message is not correct")
		} else if count == 3 {
			assert.Equal(t, "1st commit", l.Message, "1st commit message is not correct")
		}
		count++
	}
}

func setupRepo() (*Repository, string) {
	path, _ := ioutil.TempDir(os.TempDir(), "s3git-test")

	repo, _ := InitRepository(path)

	return repo, path
}

func teardownRepo(path string) {
	defer os.RemoveAll(path)
}
