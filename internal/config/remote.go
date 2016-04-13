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
	"strings"
	"errors"
	"fmt"
	"os"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const RegionDefault = "us-east-1"

func CreateRemote(name, resource, accessKey, secretKey, endpoint string) (*RemoteObject, error) {

	parts := strings.Split(resource, "://")
	if len(parts) != 2 {
		return nil, errors.New(fmt.Sprintf("Bad resource (missing '://' separator): %s", resource))
	}

	var remote *RemoteObject

	switch parts[0] {
	case REMOTE_S3:
		bucket := parts[1]

		// Allow values to be read from environment variables (for easy testing)
		accessKey = getEnvironmentValueIfUnspecified(accessKey, "S3GIT_S3_ACCESS_KEY")
		secretKey = getEnvironmentValueIfUnspecified(secretKey, "S3GIT_S3_SECRET_KEY")
		endpoint = getEnvironmentValueIfUnspecified(endpoint, "S3GIT_S3_ENDPOINT")

		var region string
		if endpoint == "" {
			// Just look for region when endpoint is not explicitly specified
			var err error
			region, err = getRegionForBucket(bucket, accessKey, secretKey)
			if err != nil {
				return nil, err
			}
		} else {
			// TODO: 'Ping' remote to check credentials
		}

		region = getEnvironmentValueIfUnspecified(region, "S3GIT_S3_REGION") // Allow to be overriden when set explicitly
		region = getRegionDefaultIfUnspecified(region)

		remote = &RemoteObject{Name: name, Type: REMOTE_S3, S3Bucket: bucket, S3Region: region, S3AccessKey: accessKey, S3SecretKey: secretKey, S3Endpoint: endpoint, MinioInsecure: true}

	case REMOTE_DYNAMODB:

		table := parts[1]

		var region string
		region = getRegionDefaultIfUnspecified(region)

		remote = &RemoteObject{Name: name, Type: REMOTE_DYNAMODB, DynamoDbTable: table, DynamoDbRegion: region, DynamoDbAccessKey: accessKey, DynamoDbSecretKey: secretKey}

	default:
		return nil, errors.New(fmt.Sprintf("Unknown resource type: %s", parts[0]))
	}

	return remote, nil
}

// Get the region for a bucket or return US Standard otherwise
func getRegionForBucket(bucket, accessKey, secretKey string) (string, error) {

	var region string

	// Determine region for bucket
	if accessKey != "" && secretKey != "" {
		svc := s3.New(session.New(&aws.Config{Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""), Region: aws.String(RegionDefault)}))

		out, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: aws.String(bucket)})
		if err != nil {
			return "", err
		}
		if out.LocationConstraint != nil {
			region = *out.LocationConstraint
		}
	}

	region = getRegionDefaultIfUnspecified(region)

	return region, nil
}

func getRegionDefaultIfUnspecified(region string) string {

	if region == "" {
		return RegionDefault	// Return default region
	}
	return region
}

func getEnvironmentValueIfUnspecified(def, envName string) string {

	val := def

	envVal := os.Getenv(envName)
	if val == "" && envVal != "" {
		val = envVal
	}
	return val
}
