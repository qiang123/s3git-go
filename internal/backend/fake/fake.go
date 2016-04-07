/*
 * Copyright 2016 Frank Wessels <fwessels@xs4all.nl>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fake

import (
	"io"
	"os"
	"path/filepath"
	"github.com/s3git/s3git-go/internal/config"
)

type Client struct {
	Directory  string
}

func MakeClient(remote config.RemoteObject) *Client {

	return &Client{
		Directory: remote.FakeDirectory}
}

// Fake uploading a file
func (c *Client) UploadWithReader(hash string, r io.Reader) error {

	f, err := os.OpenFile(c.Directory + "/" + hash, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	if err != nil {
		return err
	}

	return nil
}

// Verify the existence of a hash
func (c *Client) VerifyHash(hash string) (bool, error) {

	fileList, err := filepath.Glob(c.Directory + "/" + hash)
	if err != nil {
		return false, err
	}

	if len(fileList) == 1 && fileList[0] == hash {
		return true, nil
	}

	return false, nil
}

// Fake downloading a file
func (c *Client) DownloadWithWriter(hash string, w io.WriterAt) error {

	f, err := os.Open(c.Directory + "/" + hash)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = copyBufferAt(w, f, nil)
	if err != nil {
		return err
	}

	return nil
}

// List with a prefix string
func (c *Client) List(prefix string, action func(key string)) ([]string, error) {

	fileList, err := filepath.Glob(c.Directory + "/" + prefix + "*")
	if err != nil {
		return []string{}, err
	}

	result := make([]string, 0, len(fileList))

	for _, path := range fileList {
		_, file := filepath.Split(path)
		action(file)
	}

	return result, nil
}

// From io/io.go, adapted for io.WriterAt as opposed to io.Writer
func copyBufferAt(dst io.WriterAt, src io.Reader, buf []byte) (written int64, err error) {
	// Similarly, if the writer has a ReadFrom method, use it to do the copy.
	if rt, ok := dst.(io.ReaderFrom); ok {
		return rt.ReadFrom(src)
	}
	if buf == nil {
		buf = make([]byte, 32*1024)
	}
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.WriteAt(buf[0:nr], written)
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	return written, err
}
