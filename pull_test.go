package s3git

import (
	"fmt"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

// TODO: Think about how to deal with S3 test bucket (or make fake 'back-end' that runs locally in temp storage)
// TODO: Do proper test that does not rely on external (S3) state
func TestPull(t *testing.T) {
	repo, path := setupRepo()
	fmt.Println(path)
	//defer teardownRepo(path)

	err := repo.Pull()
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		repo.Add(strings.NewReader(fmt.Sprintf("hello s3git: %d, %s", i, time.Now())))
	}

	hash, _, _ := repo.Commit("1st commit")

	core.GetCommitObject(hash)

	repo.Push()

	err = repo.Pull()
	assert.Nil(t, err)
}
