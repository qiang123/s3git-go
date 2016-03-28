package main

import (
	"fmt"
	"github.com/s3git/s3git-go"
	"io/ioutil"
	"strings"
)

func main() {

	dir, _ := ioutil.TempDir("", "s3git")

	repo, _ := s3git.InitRepository(dir)

	repo.Add(strings.NewReader("hello s3git"))

	repo.Commit("Initial commit")

	commits, _ := repo.ListCommits("")

	for commit := range commits {
		fmt.Println(commit)
	}
}
