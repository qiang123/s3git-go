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
	"io/ioutil"
	"os"
	"fmt"
	"strings"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

const S3GIT_CONFIG = ".s3git.config"
const S3GIT_DIR = ".s3git"
const CONFIG = "config"

var Config ConfigObject

type ConfigObject struct {
	Version         int            `json:"s3gitVersion"`
	Type            string         `json:"s3gitType"` // config
	BasePath        string         `json:"s3gitBasePath"`
	RollingHashBits int            `json:"s3gitRollingHashBits"`
	RollingHashMin  int            `json:"s3gitRollingHashMin"`
	Remotes         []RemoteObject `json:"s3gitRemotes"`
}

type RemoteObject struct {
	Name		string `json:"Name"`
	Hydrate     bool   `json:"Hydrate"`
	S3Bucket    string `json:"S3Bucket"`
	S3Region    string `json:"S3Region"`
	S3AccessKey string `json:"S3AccessKey"`
	S3SecretKey string `json:"S3SecretKey"`
	S3Endpoint  string `json:"S3Endpoint"`

	MinioInsecure bool   `json:"MinioInsecure"`

	AcdRefreshToken string `json:"AcdRefreshToken"`
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

	return true, nil
}

func SaveConfig(dir string) error {

	return saveNewConfig(dir, []RemoteObject{})
}

func SaveConfigFromUrl(url, dir, accessKey, secretKey, endpoint string) error {

	parts := strings.Split(url, "//")
	if len(parts) != 2 {
		return errors.New(fmt.Sprintf("Bucket missing for cloning: %s", url))
	}
	bucket := parts[1]
	accessKey = getDefaultValue(accessKey, "S3GIT_S3_ACCESS_KEY")
	secretKey = getDefaultValue(secretKey, "S3GIT_S3_SECRET_KEY")
	endpoint = getDefaultValue(endpoint, "S3GIT_S3_ENDPOINT")
	var region string
	if endpoint == "" {
		var err error
		region, err = GetRegionForBucket(bucket, accessKey, secretKey)
		if err != nil {
			return err
		}
	} else {
		region = "us-east-1"	// TODO: Remove hard-coded region for endpoints
	}
	region = getDefaultValue(region, "S3GIT_S3_REGION")	// Allow to be overriden when set explicitly

	remotes := []RemoteObject{}
	remotes = append(remotes, RemoteObject{Name: "primary", S3Bucket: bucket, S3Region: region, S3AccessKey: accessKey, S3SecretKey: secretKey, S3Endpoint: endpoint, MinioInsecure: true})

	return saveNewConfig(dir, remotes)
}

func saveNewConfig(dir string, remotes []RemoteObject) error {

	configObject := ConfigObject{Version: 1, Type: CONFIG, BasePath: dir}

	return saveConfig(configObject, remotes)
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

	err := ioutil.WriteFile(getConfigFile(configObject.BasePath), buf.Bytes(), os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func AddRemote(name, bucket, region, accessKey, secretKey, endpoint string) error {

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
	remotes = append(remotes, RemoteObject{Name: "primary", S3Bucket: bucket, S3Region: region, S3AccessKey: accessKey, S3SecretKey: secretKey, S3Endpoint: endpoint, MinioInsecure: true})

	return saveConfig(Config, remotes)
}


// Get the region for a bucket or return US Standard otherwise
func GetRegionForBucket(bucket, accessKey, secretKey string) (string, error) {

	var region string

	// Determine region for bucket
	if accessKey != "" && secretKey != "" {
		svc := s3.New(session.New(&aws.Config{Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""), Region: aws.String("us-east-1")}))

		out, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: aws.String(bucket)})
		if err != nil {
			return "", err
		}
		if out.LocationConstraint != nil {
			region = *out.LocationConstraint
		}
	}

	// Or default to US Standard if not found
	if region == "" {
		region = "us-east-1"
	}

	return region, nil
}


func getDefaultValue(def, envName string) string {

	val := def

	envVal := os.Getenv(envName)
	if envVal != "" {
		val = envVal
	}
	return val
}
