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

package backend

import (
	"io"
	"errors"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/backend/s3"
)

type Backend interface {
	UploadWithReader(hash string, r io.Reader) error
	DownloadWithWriter(hash string, w io.WriterAt) error
	VerifyHash(hash string) (bool, error)
	// TODO Replace []string with output channel
	List(prefix string, action func(key string)) ([]string, error)
}

func GetDefaultClient() (Backend, error) {

	if len(config.Config.Remotes) == 0 {
		return nil, errors.New("No remotes configured")
	}
	return s3.MakeClient(config.Config.Remotes[0]), nil
}