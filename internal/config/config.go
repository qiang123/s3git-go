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

package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

const S3GIT_CONFIG = ".s3git.config"
const S3GIT_DIR = ".s3git"
const CONFIG = "config"
const REMOTE_S3 = "s3"
const REMOTE_FAKE = "fake"
const REMOTE_ACD = "acd"
const REMOTE_DYNAMODB = "dynamodb"

const LeafSizeMinimum = 1024
const LeafSizeDefault = 5 * 1024 * 1024
const MaxRepoSizeMinimum = 1024 * 1024
const MaxRepoSizeDefault = 25 * 1024 * 1024 * 1024

var Config ConfigObject

type ConfigObject struct {
	Version         int            `json:"s3gitVersion"`
	Type            string         `json:"s3gitType"` // config
	BasePath        string         `json:"s3gitBasePath"`
	LeafSize        uint32         `json:"s3gitLeafSize"`
	MaxRepoSize     uint64         `json:"s3gitMaxRepoSize"`
	RollingHashBits int            `json:"s3gitRollingHashBits"`
	RollingHashMin  int            `json:"s3gitRollingHashMin"`
	Remotes         []RemoteObject `json:"s3gitRemotes"`
}

// Base object for Remotes
type RemoteObject struct {
	Name    string `json:"Name"`
	Type    string `json:"Type"`
	Hydrate bool   `json:"Hydrate"`

	// Remote object for S3
	S3Bucket    string `json:"S3Bucket"`
	S3Region    string `json:"S3Region"`
	S3AccessKey string `json:"S3AccessKey"`
	S3SecretKey string `json:"S3SecretKey"`
	S3Endpoint  string `json:"S3Endpoint"`

	// Remote object for DynamoDB
	DynamoDbTable     string `json:"DynamoDbTable"`
	DynamoDbRegion    string `json:"DynamoDbRegion"`
	DynamoDbAccessKey string `json:"DynamoDbAccessKey"`
	DynamoDbSecretKey string `json:"DynamoDbSecretKey"`

	MinioInsecure bool `json:"MinioInsecure"`

	AcdRefreshToken string `json:"AcdRefreshToken"`

	// Remote object for fake backend
	FakeDirectory string `json:"FakeDirectory"`
}

func getConfigFile(dir string) string {
	return dir + "/" + S3GIT_CONFIG
}

func LoadConfig(dir string) (bool, error) {

	configFile := getConfigFile(dir)
	byteConfig, err := ioutil.ReadFile(configFile)
	if err != nil { // No config found, fine, ignore
		return false, nil
	}

	dec := json.NewDecoder(strings.NewReader(string(byteConfig)))
	if err := dec.Decode(&Config); err != nil {
		return false, err
	}

	if Config.LeafSize == 0 { // If unspecified, set to default
		Config.LeafSize = LeafSizeDefault
	}
	if Config.MaxRepoSize == 0 { // If unspecified, set to default
		Config.MaxRepoSize = MaxRepoSizeDefault
	}

	return true, nil
}

func SaveConfig(dir string, leafSize uint32, maxRepoSize uint64) error {

	configObject := ConfigObject{Version: 1, Type: CONFIG, BasePath: dir}

	if leafSize == 0 {
		configObject.LeafSize = LeafSizeDefault
	} else if leafSize < LeafSizeMinimum {
		configObject.LeafSize = LeafSizeMinimum
	} else {
		configObject.LeafSize = leafSize
	}

	if maxRepoSize == 0 {
		configObject.MaxRepoSize = MaxRepoSizeDefault
	} else if maxRepoSize < MaxRepoSizeMinimum {
		configObject.MaxRepoSize = MaxRepoSizeMinimum
	} else {
		configObject.MaxRepoSize = maxRepoSize
	}

	return saveConfig(configObject, []RemoteObject{})
}

func SaveConfigFromUrl(url, dir, accessKey, secretKey, endpoint string, leafSize uint32, maxRepoSize uint64) error {

	err := SaveConfig(dir, leafSize, maxRepoSize)
	if err != nil {
		return err
	}

	remote, err := CreateRemote("primary", url, accessKey, secretKey, endpoint)
	if err != nil {
		return err
	}

	err = AddRemote(remote)
	if err != nil {
		return err
	}

	return nil
}

func saveConfig(configObject ConfigObject, remotes []RemoteObject) error {

	for _, r := range remotes {
		configObject.Remotes = append(configObject.Remotes, r)
	}

	buf := new(bytes.Buffer)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(configObject); err != nil {
		return err
	}

	// Save config to file
	err := ioutil.WriteFile(getConfigFile(configObject.BasePath), buf.Bytes(), os.ModePerm)
	if err != nil {
		return err
	}

	// Reload config
	_, err = LoadConfig(configObject.BasePath)
	if err != nil {
		return err
	}

	return nil
}

func AddRemote(remote *RemoteObject) error {

	for _, r := range Config.Remotes {
		if r.Name == remote.Name {
			return errors.New(fmt.Sprintf("Remote already exists with name: %s", remote.Name))
		}
	}

	// TODO: Remove restriction for just a single remote
	if len(Config.Remotes) >= 1 {
		return errors.New("Current restriction applies of one remote only (to be lifted)")
	}

	remotes := []RemoteObject{}
	remotes = append(remotes, *remote)

	return saveConfig(Config, remotes)
}

func AddFakeRemote(name, directory string) error {

	for _, r := range Config.Remotes {
		if r.Name == name {
			return errors.New(fmt.Sprintf("Remote already exists with name: %s", name))
		}
	}

	// TODO: Remove restriction for just a single remote
	if len(Config.Remotes) >= 1 {
		return errors.New("Current restriction applies of one remote only (to be lifted)")
	}

	remotes := []RemoteObject{}
	remotes = append(remotes, RemoteObject{Name: "fake", Type: REMOTE_FAKE, FakeDirectory: directory})

	return saveConfig(Config, remotes)
}
