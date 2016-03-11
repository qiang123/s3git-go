package s3git

import (
	"fmt"
	"github.com/s3git/s3git-go/internal/core"
	_ "github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestPush(t *testing.T) {
	repo, path := setupRepo()
	fmt.Println(path)
	//defer teardownRepo(path)

	for i := 0; i < 100; i++ {
		repo.Add(strings.NewReader(fmt.Sprintf("hello s3git: %d", i)))
	}

	hash, _, _ := repo.Commit("1st commit")

	core.GetCommitObject(hash)

	repo.Push()
}
