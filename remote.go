package s3git

import (
	"fmt"
	"github.com/s3git/s3git-go/internal/config"
	"strings"
	"errors"
)

type Remote struct {
	Name string
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

	// TODO: 'Ping' remote to check credentials

	parts := strings.Split(resource, "//")
	if len(parts) != 2 {
		return errors.New(fmt.Sprintf("Bad resource for cloning (missing '//' separator): %s", resource))
	}

	var region, endpoint string
	if optns.endpoint == "" {
		// Just look for region when endpoint not explicitly specified
		var err error
		region, err = config.GetRegionForBucket(parts[1], accessKey, secretKey)
		if err != nil {
			return err
		}
	} else {
		endpoint = optns.endpoint
	}

	if region == "" {
		region = "us-east-1"
	}

	return config.AddRemote(name, parts[1], region, accessKey, secretKey, endpoint)
}

func (repo Repository) RemotesShow() ([]Remote, error) {

	remotes := []Remote{}

	for _, r := range config.Config.Remotes {
		remotes = append(remotes, Remote{Name: r.Name, Resource: r.S3Bucket})
	}

	return remotes, nil
}