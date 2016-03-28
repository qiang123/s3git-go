package main

import (
	"fmt"
	"github.com/s3git/s3git-go"
	"strings"
)

func main() {
	repo, _ := s3git.InitRepository(".")

	repo.Add(strings.NewReader("hello s3git"))

	repo.Commit("Initial commit")

	commits, _ := repo.ListCommits("")

	for commit := range commits {
		fmt.Println(commit)
	}
}
