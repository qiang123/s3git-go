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
	"testing"
)

func TestRepoSize(t *testing.T) {
	// TODO: [test] Add test in order to check max repo size
}

func TestRepoSizeFetchLeafAgain(t *testing.T) {

	// TODO: [test] Delete a leaf node as part of the pruning process and make sure it is fetched again from upstream when needed again
}

func TestLeafNodeAlreadyInCache(t *testing.T) {

	// TODO: [test] Test case to move leaf node from stage to cache where leaf is already in cache
}