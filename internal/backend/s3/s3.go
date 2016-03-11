package s3

import (
	"io"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

type Client struct {
	Bucket     string
	Region     string
	AccessKey  string
	SecretKey  string
}

func MakeClient(bucket, region, accessKey, secretKey string) *Client {

	return &Client{Bucket: bucket, Region: region, AccessKey: accessKey, SecretKey: secretKey}
}

// Upload a file to S3
func (c *Client) UploadWithReader(hash string, r io.Reader) error {

	uploader := s3manager.NewUploader(session.New(c.getAwsConfig()))
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(hash),
	})
	if err != nil {
		return err
	}

	return nil
}

// Verify the existence of a hash in S3
func (c *Client) VerifyHash(hash string) (bool, error) {

	svc := s3.New(session.New(), c.getAwsConfig())
	_, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(hash),
	})
	if err != nil {
		if _, ok := err.(awserr.Error); ok {
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				if reqErr.StatusCode() == 404 {
					return false, nil
				}
			}
		}
		return false, errors.New("Failed to get head of object")
	}

	return true, nil
}

func (c *Client) DownloadWithWriter(hash string, w io.WriterAt) error {

	downloader := s3manager.NewDownloader(session.New(c.getAwsConfig()))
	_, err := downloader.Download(w,
		&s3.GetObjectInput{
			Bucket: aws.String(c.Bucket),
			Key:    aws.String(hash),
		})
	if err != nil {
		return err
	}

	return nil
}

// List with a prefix string in S3
func (c *Client) List(prefix string, action func(key string)) ([]string, error) {

	lister := lister{Client: c, action: action}

	client := s3.New(session.New(), c.getAwsConfig())
	params := &s3.ListObjectsInput{Bucket: &c.Bucket, Prefix: &prefix}
	client.ListObjectsPages(params, lister.eachPage)

	result := make([]string, 0)

	return result, nil
}

type lister struct {
	*Client
	action func(key string)
}

func (l *lister) eachPage(page *s3.ListObjectsOutput, more bool) bool {
	for _, obj := range page.Contents {
		l.action(*obj.Key)
	}

	return true
}

func (c *Client) getAwsConfig() *aws.Config {
	return &aws.Config{Credentials: credentials.NewStaticCredentials(c.AccessKey, c.SecretKey, ""), Region: aws.String(c.Region)}
}
