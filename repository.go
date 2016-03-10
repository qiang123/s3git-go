package s3git

import (
	"io"
	"encoding/hex"
	"time"
	"errors"
	"github.com/s3git/s3git-go/cas"
	"github.com/s3git/s3git-go/config"
	"github.com/s3git/s3git-go/kv"
	"bytes"
)

type Repository struct {

	Remotes []Remote
}

// Initialize a new repository
func InitRepository(path string) (*Repository, error) {

	config.SaveConfig(path)

	return &Repository{}, nil
}

// Open an existing repository
func OpenRepository(path string) (*Repository, error) {

	success, err := config.LoadConfig(path)
	if err != nil {
		return nil, err
	}
	if success {
		kv.OpenDatabase()
	}

	return &Repository{}, nil
}

// Clone a remote repository
func Clone(url, path string, progressDownloading, progressProcessing func(maxTicks int64)) (*Repository, error) {

	downloadTicks := int64(250)
	for i := int64(0); i < downloadTicks; i++ {

		time.Sleep(time.Millisecond * 20)
		if progressDownloading != nil {
			progressDownloading(downloadTicks)
		}
	}

	processTicks := int64(100)
	for i := int64(0); i < processTicks; i++ {

		time.Sleep(time.Millisecond * 50)
		if progressProcessing != nil {
			progressProcessing(processTicks)
		}
	}

	return &Repository{}, nil
}

// Get the list of changes for a repository
func (repo Repository) Status() (<-chan string, error) {

	result := make(chan string)

	go func() {
		defer close(result)

		list, err := kv.ListStage()
		if err != nil {
			return
		}

		for l := range list {
			result <- hex.EncodeToString(l)
		}
	}()

	return result, nil
}


// List the contents of a repository
func (repo Repository) List(prefix string) (<-chan string, error) {

	result := make(chan string)

	go func() {
		// make sure we always close the channel
		defer close(result)

		keys, err := kv.ListLevel1Blobs(prefix)
		if err != nil {
			return
		}

		for key := range keys {
			result <- hex.EncodeToString(key)
		}
	}()

	return result, nil
}

// Add a stream to the repository
func (repo Repository) Add(r io.Reader) (string, bool, error) {

	cw := cas.MakeWriter(cas.BLOB)
	defer cw.Close()

	_, err := io.Copy(cw, r)
	if err != nil {
		return "", false, nil
	}

	rootKeyStr, _ ,newBlob, err := cw.Flush()
	if err != nil {
		return "", false, nil
	}

	// Check whether object is not already in repository
	if newBlob {
		// Add root key stage
		kv.AddToStage(rootKeyStr)
	}

	return rootKeyStr, newBlob, err
}

// Get a stream from the repository
func (repo Repository) Get(hash string) (io.Reader, error) {

	cr := cas.MakeReader(hash)
	if cr == nil {
		return nil, errors.New("Failed to create cas reader")
	}

	r, w := io.Pipe()

	go func(cr *cas.Reader, w io.WriteCloser) {

		defer w.Close()

		size := 0
		array := make([]byte, cas.ChunkSize)
		for {
			read, err := cr.Read(array)
			size += read
			if read > 0 {
				_, err := io.Copy(w, bytes.NewBuffer(array[:read]))
				if err != nil {
					panic(err)
				}
			}
			if err == io.EOF {
				break
			}
		}

	}(cr, w)

	return r, nil
}

// Push updates of the repository
func (repo Repository) Push(progress func(maxTicks int64)) error {

	return nil
}

// Pull updates for the repository
func (repo Repository) Pull(progress func(maxTicks int64)) error {

	return nil
}