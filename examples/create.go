package main

import (
	"fmt"
	"github.com/s3git/s3git-go"
	"io/ioutil"
	"strings"
)

func main() {

	dir, _ := ioutil.TempDir("", "s3git")

	// Create repo
	repo, _ := s3git.InitRepository(dir)

	// Add some data
	repo.Add(strings.NewReader("hello s3git"))

	// Commit changes
	repo.Commit("Initial commit")

	// List commits
	commits, _ := repo.ListCommits("")

	for commit := range commits {
		fmt.Println(commit)
	}
}
