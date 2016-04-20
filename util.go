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
	"errors"
)

// Get the full size unique hash for a given prefix.
// Return error in case none or multiple candidates are found
func (repo Repository) MakeUnique(prefix string) (string, error) {

	list, errList := repo.List(prefix)
	if errList != nil {
		return "", errList
	}

	return getUnique(list)
}

func getUnique(list <-chan string) (string, error) {

	err := errors.New("Not found (be less specific)")
	hash := ""
	for elem := range list {
		if len(hash) == 0 {
			hash, err = elem, nil
		} else {
			err = errors.New("More than one possiblity found (be more specific)")
		}
	}

	return hash, err
}