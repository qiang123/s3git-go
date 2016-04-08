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

package cas

import (
	"os"
	"io"
	"io/ioutil"
	"github.com/stretchr/testify/assert"
	"testing"
	"strings"
	"bytes"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/kv"
	"math/rand"
	"time"
)

func TestWriteSingleChunk(t *testing.T) {

	path := setupRepo(t)
	defer teardownRepo(path)

	input := "hello s3git\n"

	rootKeyStr := writeTo(t, strings.NewReader(input))
	output := readBack(t, rootKeyStr)

	assert.Equal(t, input, output, "Input and output are different")
}

func TestWriteTwoChunks(t *testing.T) {

	path := setupRepo(t)
	defer teardownRepo(path)

	input := strings.Repeat("0123456789abcdef", 7.5*1024*1024/16)

	rootKeyStr := writeTo(t, strings.NewReader(input))
	output := readBack(t, rootKeyStr)

	assert.Equal(t, input, output, "Input and output are different")
}

func TestWriteManyChunks(t *testing.T) {

	path := setupRepo(t)
	defer teardownRepo(path)

	input := strings.Repeat("0123456789abcdef", 22.5*1024*1024/16)

	rootKeyStr := writeTo(t, strings.NewReader(input))
	output := readBack(t, rootKeyStr)

	assert.Equal(t, input, output, "Input and output are different")
}

func TestWriteDifferentChunkSize(t *testing.T) {

	path := setupRepo(t)
	defer teardownRepo(path)

	config.Config.ChunkSize = 1*1024*1024

	input := strings.Repeat("0123456789abcdef", int((0.5+float32(random(5, 10)))*1024*1024/16))

	rootKeyStr := writeTo(t, strings.NewReader(input))
	output := readBack(t, rootKeyStr)

	assert.Equal(t, input, output, "Input and output are different")
}

func TestSmallerChunkSizeForReading(t *testing.T) {

	path := setupRepo(t)
	defer teardownRepo(path)

	input := strings.Repeat("AbCdEfGhIjKlMnOpQrDtUvWxYz", int((0.5+float32(random(15, 20)))*1024*1024/16))

	rootKeyStr := writeTo(t, strings.NewReader(input))

	config.Config.ChunkSize = uint32(1e6 + random(1e5, 2e5))

	output := readBack(t, rootKeyStr)

	assert.Equal(t, input, output, "Input and output are different")
}


func writeTo(t *testing.T, r io.Reader) string {

	cw := MakeWriter(BLOB)
	defer cw.Close()

	_, err := io.Copy(cw, r)
	assert.Nil(t, err)

	rootKeyStr, _, _, err := cw.Flush()
	assert.Nil(t, err)

	return rootKeyStr
}

func readBack(t *testing.T, hash string) string {

	cr := MakeReader(hash)
	assert.NotEmpty(t, cr)

	buf := bytes.NewBuffer(nil)

	size := 0
	array := make([]byte, config.Config.ChunkSize)
	for {
		read, err := cr.Read(array)
		size += read
		if read > 0 {
			_, err := buf.Write(array[:read])
			assert.Nil(t, err)
		}
		if err == io.EOF {
			break
		}
	}

	return string(buf.Bytes())
}

func setupRepo(t *testing.T) (string) {
	path, _ := ioutil.TempDir("", "s3git-cas-")

	config.SaveConfig(path)

	success, err := config.LoadConfig(path)
	assert.Nil(t, err)
	assert.Equal(t, true, success, "Failed to init repo")
	kv.OpenDatabase()

	return path
}

func teardownRepo(path string) {
	defer os.RemoveAll(path)
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max - min) + min
}
