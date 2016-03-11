package backend

import (
	"io"
)

type Backend interface {
	UploadWithReader(hash string, r io.Reader) error
	DownloadWithWriter(hash string, w io.WriterAt) error
	VerifyHash(hash string) (bool, error)
	// TODO Replace []string with output channel
	List(prefix string, action func(key string)) ([]string, error)
}

