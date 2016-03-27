s3git-go
========

[![Join the chat at https://gitter.im/s3git/s3git](https://badges.gitter.im/s3git/s3git.svg)](https://gitter.im/s3git/s3git?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This is the go SDK package for s3git.

For brevity reasons, error handling and other boilerplate code like package naming etc. is not shown in the examples. Actual client code should always check for errors, see [s3git](https://github.com/s3git/s3git) as an example.

**DISCLAIMER: This software is still under development (although the storage format/model using BLAKE2 hasing is stable)  -- use at your own peril for now**

BLAKE2 Tree Hashing
-------------------

If you would like to understand how s3git uses the BLAKE2 Tree hashing mode please see [here](https://github.com/s3git/s3git/blob/master/BLAKE2.md). 

Create a repository
-------------------

```go
import "github.com/s3git/s3git-go"

repo, _ := s3git.InitRepository(".")

repo.Add(strings.NewReader("hello s3git"))

repo.Commit("Initial commit")

list, _ := repo.List("")

for l := range list {
    fmt.Println(l)
}

commits, _ := repo.ListCommits("")

for commit := range commits {
    fmt.Println(commit)
}
```

See [here](https://github.com/s3git/s3git-go/blob/master/examples/create.go) for the full example (and others).

Clone a repository
------------------

```go
import "github.com/s3git/s3git-go"

repo, _ := s3git.Clone("s3://s3git-100m", ".")

for elem := range repo.List("123456") {
    fmt.Println(elem)
}
```

Make changes and push
---------------------

```go
import "github.com/s3git/s3git-go"

repo, _ := s3git.OpenRepository(".")

file, _ := os.Open("picture.jpg")

repo.Add(file)

repo.Commit("Added a picture")

repo.Push()
```

Pull down changes
-----------------

```go
import "github.com/s3git/s3git-go"

repo, _ := s3git.OpenRepository(".")

repo.Pull()

repo.Log()
```

Extract data
------------

```go
import "github.com/s3git/s3git-go"

repo, _ := s3git.OpenRepository(".")

r, _ := repo.Get("012345678")

io.Copy(os.Stdout, r)
```

Clone a repository with progress
--------------------------------

```go
import "github.com/s3git/s3git-go"

repo, _ := s3git.Clone("s3://s3git-100m", ".")

```

Contributions
-------------

Contributions are welcome! Please see [`CONTRIBUTING.md`](CONTRIBUTING.md).

License
-------

s3git-go is released under the Apache License v2.0. You can find the complete text in the file LICENSE.
