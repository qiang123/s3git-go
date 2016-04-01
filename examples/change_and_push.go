package main

import (
	"fmt"
	"time"
	"github.com/s3git/s3git-go"
	"io/ioutil"
	"strings"
)

func main() {

	dir, _ := ioutil.TempDir("", "s3git")

	options := []s3git.CloneOptions{}
	options = append(options, s3git.CloneOptionSetAccessKey("AKIAJYNT4FCBFWDQPERQ"))
	options = append(options, s3git.CloneOptionSetSecretKey("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"))

	fmt.Println("Clone a repo...")
	repo, _ := s3git.Clone("s3://s3git-spoon-knife", dir, options...)

	fmt.Println("Add some data...")
	repo.Add(strings.NewReader(fmt.Sprint(time.Now())))

	fmt.Println("Commit changes...")
	repo.Commit("New commit")

	fmt.Println("And push...")
	hydrate := false 	// For explanation, see https://github.com/s3git/s3git/blob/master/BLAKE2.md#hydrated
	repo.Push(hydrate, func(total int64) {})

	fmt.Println("Done.")
}

