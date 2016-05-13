package main

import (
	"fmt"
	"github.com/s3git/s3git-go"
	"io/ioutil"
)

func create_main() {

	dir, _ := ioutil.TempDir("", "s3git")

	options := []s3git.CloneOptions{}
	options = append(options, s3git.CloneOptionSetAccessKey("AKIAJYNT4FCBFWDQPERQ"))
	options = append(options, s3git.CloneOptionSetSecretKey("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"))

	// Clone a repo
	repo, _ := s3git.Clone("s3://s3git-spoon-knife", dir, options...)

	// List contents
	list, _ := repo.List("")

	for l := range list {
		fmt.Println(l)
	}
}
