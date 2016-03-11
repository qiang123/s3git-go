package s3git

import (
	"bytes"
	"fmt"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestAddAndGet(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	hash, _, _ := repo.Add(strings.NewReader("s3git"))
	r, _ := repo.Get(hash)

	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	s := buf.String()

	assert.Equal(t, "s3git", s, "Expected s3git")
}

func TestManyAdds(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	for i := 0; i < 100; i++ {
		repo.Add(strings.NewReader(fmt.Sprintf("hello s3git: %d", i)))
	}

	hash, _, _ := repo.Commit("1st commit")

	co, _ := core.GetCommitObject(hash)
	to, _ := core.GetTreeObject(co.S3gitTree)

	assert.Equal(t, 100, len(to.S3gitAdded), "Expected 100 items in tree object")
}

func TestList(t *testing.T) {

}

func TestStatus(t *testing.T) {

}
