package s3git

import (
	"encoding/hex"
	"fmt"
	"github.com/s3git/s3git-go/internal/backend"
	"github.com/s3git/s3git-go/internal/backend/s3"
	"github.com/s3git/s3git-go/internal/cas"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/core"
	"github.com/s3git/s3git-go/internal/kv"
	"github.com/s3git/s3git-go/internal/util"
	"github.com/szferi/gomdb"
	"io"
	"io/ioutil"
	"os"
)

// Pull updates for the repository
func (repo Repository) Pull( /*progress func(maxTicks int64)*/ ) error {

	client := s3.MakeClient(config.Config.S3gitS3Bucket, config.Config.S3gitS3Region, config.Config.S3gitS3AccessKey, config.Config.S3gitS3SecretKey)
	return pull(client)
}

func pull(client backend.Backend) error {

	// Get map of prefixes already in store
	prefixesInBackend, err := ListPrefixes(client)
	if err != nil {
		return err
	}

	prefixesToFetch := []string{}

	// Iterate over prefixes and find out if they are already available locally
	for prefix, _ := range prefixesInBackend {
		key, _ := hex.DecodeString(prefix)
		_, _, err := kv.GetLevel1(key)
		if err != nil && err != mdb.NotFound {
			return err
		}

		if err == mdb.NotFound {
			// Prefix is not available locally  --> add to get fetched
			prefixesToFetch = append(prefixesToFetch, prefix)
		}
	}

	// TODO: Speedup by running parallel in multiple go routines
	// TODO: Add progress feedback
	for _, prefix := range prefixesToFetch {
		fmt.Println("Fetching", util.FriendlyHash(prefix))
		// Fetch Prefix object and all objects directly and indirectly referenced by it
		err = fetchPrefix(prefix, client)
		if err != nil {
			return err
		}
	}

	return nil
}

// Fetch Prefix object and all objects directly and indirectly referenced by it
func fetchPrefix(prefix string, client backend.Backend) error {

	// Make sure that we do the storage of the object in the cas in
	// reverse order, that is store prefix object in the cas as the
	// *last* action, just before that the commit object, etc.
	// This ensures that, if the pull action is prematurely terminated
	// (either by user/application, or some sort of error/crash) that
	// a subsequent pull will still miss the prefix object and pull it
	// again

	// First fetch blob for prefix object
	prefixName, prefixBytes, err := fetchBlobTempFileAndContents(prefix, client)
	if err != nil {
		return err
	}
	defer os.Remove(prefixName)

	po, err := core.GetPrefixObjectFromString(string(prefixBytes))
	if err != nil {
		return err
	}

	{
		// Now pull down commit object
		commitName, commitBytes, err := fetchBlobTempFileAndContents(po.S3gitFollowMe, client)
		if err != nil {
			return err
		}
		defer os.Remove(commitName)

		co, err := core.GetCommitObjectFromString(string(commitBytes))
		if err != nil {
			return err
		}

		{
			// Now pull down tree object
			treeName, treeBytes, err := fetchBlobTempFileAndContents(co.S3gitTree, client)
			if err != nil {
				return err
			}
			defer os.Remove(treeName)

			to, err := core.GetTreeObjectFromString(string(treeBytes))
			if err != nil {
				return err
			}

			// Cache root keys for all added blobs in this commit ...
			err = cacheKeysForBlobs(to.S3gitAdded)
			if err != nil {
				return err
			}

			// Add tree object to cas
			_, err = storeBlobInCasCache(treeName, core.TREE)
			if err != nil {
				return err
			}

			// Delete the chunks for the tree object since we are unlikely the need it again
			err = cas.DeleteChunksForBlob(co.S3gitTree)
			if err != nil {
				return err
			}
		}

		// Add commit object to cas
		_, err = storeBlobInCasCache(commitName, core.COMMIT)
		if err != nil {
			return err
		}

		// Mark warm and cold parents as parents
		err = co.MarkWarmAndColdParents()
		if err != nil {
			return err
		}
	}

	// Add prefix object to cas
	_, err = storeBlobInCasCache(prefixName, core.PREFIX)
	if err != nil {
		return err
	}

	return nil
}

// Fetch blob down to temp file along with contents
func fetchBlobTempFileAndContents(prefix string, client backend.Backend) (tempFile string, contents []byte, err error) {

	name, err := fetchBlobToTempFile(prefix, client)
	if err != nil {
		return "", nil, err
	}

	contents, err = ioutil.ReadFile(name)
	if err != nil {
		return "", nil, err
	}

	return name, contents, nil
}

// Fetch blob down to temp file in order to load
func fetchBlobToTempFile(hash string, client backend.Backend) (tempFile string, err error) {

	file, err := ioutil.TempFile("", "pull-")
	if err != nil {
		return "", err
	}

	err = client.DownloadWithWriter(hash, file)
	if err != nil {
		return "", err
	}
	name := file.Name()
	file.Close()

	return name, nil
}

func pullBlob(hash, objType string, client backend.Backend, verbose bool) error {

	if verbose && objType == core.COMMIT {
		fmt.Println("Fetching commit", hash)
	}
	_, err := pullBlobDownToLocalDisk(hash, objType, client)
	return err
}

func pullBlobDownToLocalDisk(hash, objType string, client backend.Backend) ([]byte, error) {

	// TODO: Remove work around by using separate file
	name, err := fetchBlobToTempFile(hash, client)
	if err != nil {
		return nil, err
	}
	defer os.Remove(name)

	return storeBlobInCasCache(name, objType)
}

// Store the blob in the caching area of the cas
func storeBlobInCasCache(name, objType string) ([]byte, error) {

	in, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer in.Close()

	var cw *cas.Writer
	if objType == core.PREFIX {
		cw = cas.MakeWriterInCheatMode(objType)
	} else {
		cw = cas.MakeWriter(objType)
	}

	// For pulls write to the cache area (not stage)
	cw.SetAreaDir(cas.CacheDir)

	io.Copy(cw, in)

	_, leafHashes, _, err := cw.Flush()
	if err != nil {
		return nil, err
	}

	return leafHashes, nil
}

// Just cache the key for BLOBs, content will be pulled down later when needed
// For performance do not write per key to the KV but write in larger batches
func cacheKeysForBlobs(added []string) error {

	const TREE_BATCH_SIZE = 0x4000

	// Create arrays to be able to batch the operation
	keys := make([][]byte, 0, TREE_BATCH_SIZE)
	values := make([][]byte, 0, TREE_BATCH_SIZE)

	count := 0
	for _, add := range added {
		key, _ := hex.DecodeString(add)

		keys = append(keys, key)
		values = append(values, nil)

		if len(keys) == cap(keys) {
			err := kv.AddMultiToLevel1(keys, values, core.BLOB)
			if err != nil {
				return err
			}

			keys = keys[:0]
			values = values[:0]
		}
		count++
	}

	if len(keys) > 0 {
		err := kv.AddMultiToLevel1(keys, values, core.BLOB)
		if err != nil {
			return err
		}
		count++
	}

	return nil
}
