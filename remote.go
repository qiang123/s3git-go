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

func (repo Repository) RemoteAdd(name, resource, accessKey, secretKey string) error {

	// TODO: 'Ping' remote to check credentials

	parts := strings.Split(resource, "//")
	if len(parts) != 2 {
		return errors.New(fmt.Sprintf("Bad resource for cloning (missing '//' separator): %s", resource))
	}

	region, err := config.GetRegionForBucket(parts[1], accessKey, secretKey)
	if err != nil {
		return err
	}

	return config.AddRemote(name, parts[1], region, accessKey, secretKey)
}

func (repo Repository) RemotesShow() ([]Remote, error) {

	remotes := []Remote{}

	for _, r := range config.Config.Remotes {
		remotes = append(remotes, Remote{Name: r.Name, Resource: r.S3Bucket})
	}

	return remotes, nil
}