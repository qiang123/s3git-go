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

package s3git

import (
	"bytes"
	"fmt"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestAddAndGet(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	hash, _, _ := repo.Add(strings.NewReader("s3git"))
	r, _ := repo.Get(hash)

	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	s := buf.String()

	assert.Equal(t, "s3git", s, "Expected s3git")
}

func TestManyAdds(t *testing.T) {
	repo, path := setupRepo()
	defer teardownRepo(path)

	for i := 0; i < 100; i++ {
		repo.Add(strings.NewReader(fmt.Sprintf("hello s3git: %d", i)))
	}

	hash, _, _ := repo.Commit("1st commit")

	co, _ := core.GetCommitObject(hash)
	to, _ := core.GetTreeObject(co.S3gitTree)

	assert.Equal(t, 100, len(to.S3gitAdded), "Expected 100 items in tree object")
}

func TestList(t *testing.T) {

}

func TestStatus(t *testing.T) {

}
