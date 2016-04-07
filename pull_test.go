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
	"fmt"
	"io/ioutil"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestPull(t *testing.T) {

	fakeDir, _ := ioutil.TempDir("", "s3git-fake-backend-")

	testCreateFakeRepo(t, fakeDir)

	repo, path := setupRepo()
	fmt.Println("Pull", path)
	repo.remoteAddFake("fake", fakeDir)
	defer teardownRepo(path)

	err := repo.Pull(func(total int64) {})
	assert.Nil(t, err)

	stats, err := repo.Statistics()
	assert.Nil(t, err)
	assert.Equal(t, uint64(10), stats.Objects, "Number of objects is not correct")
}

func testCreateFakeRepo(t *testing.T, fakeDir string) {

	repoFake, path := setupRepo()
	repoFake.remoteAddFake("fake", fakeDir)
	defer teardownRepo(path)

	for i := 0; i < 10; i++ {
		repoFake.Add(strings.NewReader(fmt.Sprintf("hello s3git: %d, %s", i, time.Now())))
	}

	hash, _, _ := repoFake.Commit("1st commit")

	core.GetCommitObject(hash)

	err := repoFake.Push(true, func(total int64) {})
	assert.Nil(t, err)
}