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
	"github.com/s3git/s3git-go/internal/config"
)

type Remote struct {
	Name     string
	Resource string
}

type remoteOptions struct {
	endpoint string
}

func RemoteOptionSetEndpoint(endpoint string) func(optns *remoteOptions) {
	return func(optns *remoteOptions) {
		optns.endpoint = endpoint
	}
}

type RemoteOptions func(*remoteOptions)

func (repo Repository) RemoteAdd(name, resource, accessKey, secretKey string, options ...RemoteOptions) error {

	optns := &remoteOptions{}
	for _, op := range options {
		op(optns)
	}

	remote, err := config.CreateRemote(name, resource, accessKey, secretKey, optns.endpoint)
	if err != nil {
		return err
	}

	return config.AddRemote(remote)
}

func (repo Repository) remoteAddFake(name, directory string) error {

	return config.AddFakeRemote(name, directory)
}

func (repo Repository) RemotesShow() ([]Remote, error) {

	remotes := []Remote{}

	for _, r := range config.Config.Remotes {
		remotes = append(remotes, Remote{Name: r.Name, Resource: r.S3Bucket})
	}

	return remotes, nil
}
