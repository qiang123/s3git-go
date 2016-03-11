package s3git

import (
	"fmt"
	"errors"

	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/kv"
	"github.com/s3git/s3git-go/internal/util"
	"github.com/s3git/s3git-go/internal/backend"
	"github.com/s3git/s3git-go/internal/backend/s3"

	"github.com/dustin/go-humanize"
	"sync"
	"encoding/hex"
)

// Perform a push to the back end for the repository
func (repo Repository) Push(/*progress func(maxTicks int64)*/) error {

	list, err := kv.ListLevel1Prefixes()
	if err != nil {
		return err
	}

	client := s3.MakeClient(config.Config.S3gitS3Bucket, config.Config.S3gitS3Region, config.Config.S3gitS3AccessKey, config.Config.S3gitS3SecretKey)
	return push(list, client)
}

// Push any new commit objects including all added objects to the back end store
func push(prefixChan <-chan []byte, client backend.Backend) error {

	fmt.Println("Starting push")
	defer func() { fmt.Println("Finished push") }()

	// Get map of prefixes already in store
	prefixesInBackend, err := ListPrefixes(client)
	if err != nil {
		return err
	}

	for prefixByte := range prefixChan {

		prefix := hex.EncodeToString(prefixByte)

		_, verified := prefixesInBackend[prefix]

		// We can safely skip in case a prefix object is verified (pushed as last object)
		if !verified {

			// Get prefix object
			po, err := core.GetPrefixObject(prefix)
			if err != nil {
				return err
			}

			// Get commit object
			co, err := core.GetCommitObject(po.S3gitFollowMe)
			if err != nil {
				return err
			}

			// Get tree object
			to, err := core.GetTreeObject(co.S3gitTree)
			if err != nil {
				return err
			}

			// first push all added blobs in this commit ...
			err = PushBlobRange(to.S3gitAdded, nil, client)
			if err != nil {
				return err
			}

			// then push tree object
			_, err = PushBlob(co.S3gitTree, nil, client)
			if err != nil {
				return err
			}

			// then push commit object
			_, err = PushBlob(po.S3gitFollowMe, nil, client)
			if err != nil {
				return err
			}

			// ...  finally push prefix object itself
			// (if something goes in chain above, the prefix object will be missing so
			//  will be (attempted to) uploaded again during the next push)
			_, err = PushBlob(prefix, nil, client)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Push a single blob to the back end store
func PushBlob(hash string, size *uint64, client backend.Backend) (newlyUploaded bool, err error) {

	startOfLine := ""
	if size != nil {
		startOfLine = fmt.Sprintf("Uploading %s (%s)", util.FriendlyHash(hash), humanize.Bytes(*size))
	} else {
		startOfLine = fmt.Sprintf("Uploading %s", util.FriendlyHash(hash))
	}

	// TODO: Consider whether we want to verify again...
	if false {
		verified, err := client.VerifyHash(hash)
		if err != nil {
			fmt.Println(startOfLine, "verification failed", err)
			return false, err
		}

		if verified { // Resource already in back-end
			fmt.Println(startOfLine, "already in store")

			return false, nil
		}
	}

	// TODO: for back ends storing whole files: consider multipart upload?

	// TODO: implement back ends that store chunks (not whole files)
	// TODO: for back ends storing chunks: consider uploading chunks in parallel
	cr := cas.MakeReader(hash)
	if cr == nil {
		panic(errors.New("Failed to create cas reader"))
	}

	err = client.UploadWithReader(hash, cr)
	if err != nil {
		fmt.Println(startOfLine, "failed to upload to store", err)
		return false, err
	}

	// Move blob from .stage to .cache directory upon successful upload
	err = cas.MoveBlobToCache(hash)
	if err != nil {
		fmt.Println(startOfLine, "failed to move underlying blobs to cache", err)
		return false, err
	}

	fmt.Println(startOfLine, "successfully uploaded to store")

	return true, nil
}

func minu64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

// Push a range of blobs to the back end store in parallel
//
// See https://github.com/adonovan/gopl.io/blob/master/ch8/thumbnail/thumbnail_test.go
//
func PushBlobRange(hashes []string, size *uint64, client backend.Backend) error {

	var wg sync.WaitGroup
	var msgs = make(chan string)
	var results = make(chan error)

	for i := 0; i < min(len(hashes), 100); i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for hash := range msgs  {
				_, err := PushBlob(hash, size, client)
				results <- err
			}
		}()
	}

	go func() {
		for _, hash := range hashes {
			msgs <- hash
		}
		close(msgs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var err error
	for e := range results {
		if e != nil {
			err = e
		}
	}

	return err
}

// List prefixes at back end store, doing 16 lists in parallel
func ListPrefixes(client backend.Backend) (map[string]bool, error){

	var wg sync.WaitGroup
	var results = make(chan []string)

	for i := 0x0; i <= 0xf; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			result := make([]string, 0, 1000)

			// TODO: Hard coded prefix of 7 zero's
			client.List(fmt.Sprintf("0000000%x", i), func(key string) {
				result = append(result, key)

				// TODO: WE NEED TO wg.Done() HERE WHEN LAST KEY HAS BEEN RECEIVED
				//       -- SEE /Users/frankw/golang/src/github.com/fwessels/listperf/listperf.go
				// IMPORTANT: WE WILL BE MISSING OBJECTS HERE
			})

			results <- result
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	prefixHash := make(map[string]bool)
	for result := range results {
		for _, r := range result {
			prefixHash[r] = true
		}
	}

	return prefixHash, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
