package core

import (
	"bytes"
	"io"
	"fmt"
	"github.com/s3git/s3git-go/internal/cas"
	"strings"
	"errors"
)

type coreObject struct {
	S3gitVersion int    `json:"s3gitVersion"`
	S3gitType    string `json:"s3gitType"`
}

// Write object to CAS
func (co *coreObject) write(buf *bytes.Buffer, objType string) (string, error) {

	paddinglength := cas.KeySize - (buf.Len() % cas.KeySize)

	cw := cas.MakeWriter(objType)

	// Write out json objects with a size of a multiple of 64 bytes
	fmt.Fprint(cw, string(buf.Bytes()[:buf.Len()-3]))
	fmt.Fprint(cw, strings.Repeat("0", paddinglength))
	fmt.Fprint(cw, string(buf.Bytes()[buf.Len()-3:]))

	// Flush the stream to get hash
	hash, _, _, err := cw.Flush()

	return hash, err
}

func readBlob(hash string) (string, error) {

	cr := cas.MakeReader(hash)
	if cr == nil {
		return "", errors.New(fmt.Sprint("Failed to read hash %s", hash))
	}

	buf := bytes.NewBuffer(nil)
	io.Copy(buf, cr)
	return string(buf.Bytes()), nil
}
