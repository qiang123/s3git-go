package s3git

import (
	"os"
	"io/ioutil"
	"testing"
	"strings"
	"github.com/stretchr/testify/assert"
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

	list, err := repo.ListCommits()
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


func setupRepo() (*Repository, string) {
	path, _ := ioutil.TempDir(os.TempDir(), "s3git-test")

	repo, _ := InitRepository(path)

	return repo, path
}

func teardownRepo(path string) {
	defer os.RemoveAll(path)
}
