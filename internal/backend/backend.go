package backend

import (
	"io"
	"errors"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/backend/s3"
)

type Backend interface {
	UploadWithReader(hash string, r io.Reader) error
	DownloadWithWriter(hash string, w io.WriterAt) error
	VerifyHash(hash string) (bool, error)
	// TODO Replace []string with output channel
	List(prefix string, action func(key string)) ([]string, error)
}

func GetDefaultClient() (Backend, error) {

	if len(config.Config.Remotes) == 0 {
		return nil, errors.New("No remotes configured")
	}
	return s3.MakeClient(config.Config.Remotes[0]), nil
}