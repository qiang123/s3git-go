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
	"github.com/s3git/s3git-go/internal/backend/fake"
	"github.com/s3git/s3git-go/internal/backend/s3"
	"github.com/s3git/s3git-go/internal/backend/acd"
	"github.com/s3git/s3git-go/internal/backend/dynamodb"
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

		// TODO: Give proper error when AWS credentials are incorrect
		//
		// $ s3git clone s3://s3git-100m-euc1-objs -a "AKIAI26TSIF6JIMMDSPQ" -s "5NvshAhI0KMz5Gbqkp7WNqXYlnjBjkf9IaJD75x7"
		// Cloning into /home/ec2-user/golang/src/github.com/s3git/test/s3git-100m-euc1-objs
		// Error: No remotes configured

		return nil, errors.New("No remotes configured")
	}

	switch config.Config.Remotes[0].Type {
	case config.REMOTE_FAKE:
		return fake.MakeClient(config.Config.Remotes[0]), nil
	case config.REMOTE_ACD:
		return acd.MakeClient(config.Config.Remotes[0]), nil
	case config.REMOTE_DYNAMODB:
		return dynamodb.MakeClient(config.Config.Remotes[0])
	default: // config.REMOTE_S3
		return s3.MakeClient(config.Config.Remotes[0]), nil
	}
}