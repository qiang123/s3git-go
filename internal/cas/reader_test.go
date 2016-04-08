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
	"github.com/stretchr/testify/assert"
	"testing"
	"strings"
	"github.com/s3git/s3git-go/internal/config"
)


func TestReadWithSmallerChunkSize(t *testing.T) {

	path := setupRepo(t)
	defer teardownRepo(path)

	input := strings.Repeat("AbCdEfGhIjKlMnOpQrDtUvWxYz", int((0.5+float32(random(15, 20)))*1024*1024/16))

	rootKeyStr := writeTo(t, strings.NewReader(input))

	config.Config.ChunkSize = uint32(1e6 + random(1e5, 2e5))

	output := readBack(t, rootKeyStr)

	assert.Equal(t, input, output, "Input and output are different")
}

func TestReadWithBiggerChunkSize(t *testing.T) {

	path := setupRepo(t)
	defer teardownRepo(path)

	input := strings.Repeat("AbCdEfGhIjKlMnOpQrDtUvWxYz", int((0.5+float32(random(25, 30)))*1024*1024/16))

	config.Config.ChunkSize = uint32(2e6 + random(1e5, 2e5))

	rootKeyStr := writeTo(t, strings.NewReader(input))

	config.Config.ChunkSize = uint32(7e6 + random(1e5, 2e5))

	output := readBack(t, rootKeyStr)

	assert.Equal(t, input, output, "Input and output are different")
}
