s3git-go
========

This is the go SDK package for s3git.

Create a repository
-------------------

```go
import "github.com/s3git/s3git-go"

repo, _ := s3git.InitRepository(".")

repo.Add()

repo.Commit("Initial commit")

repo.List()

repo.Log()
```

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