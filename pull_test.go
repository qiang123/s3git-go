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
	"github.com/s3git/s3git-go/internal/core"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

// TODO: Think about how to deal with S3 test bucket (or make fake 'back-end' that runs locally in temp storage)
// TODO: Do proper test that does not rely on external (S3) state
func TestPull(t *testing.T) {
	repo, path := setupRepo()
	fmt.Println(path)
	//defer teardownRepo(path)

	err := repo.Pull()
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		repo.Add(strings.NewReader(fmt.Sprintf("hello s3git: %d, %s", i, time.Now())))
	}

	hash, _, _ := repo.Commit("1st commit")

	core.GetCommitObject(hash)

	repo.Push()

	err = repo.Pull()
	assert.Nil(t, err)
}
