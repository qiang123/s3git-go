package cas

import (
	"os"
	"path"
	"github.com/s3git/s3git-go/config"
)

// Upon writing, make sure the size of the repository does not exceed the max local size,
// prune stale chunks otherwise
func checkRepoSize() {
	// TODO: Implement
}

// Get the filepath for a given hash
func getBlobPath(hash string) string {

	// Check if in stage directory
	nameInStage := getBlobPathWithinArea(hash, stageDir)
	if _, err := os.Stat(nameInStage); err == nil {
		return nameInStage
	}

	// Check if in cache directory
	nameInCache := getBlobPathWithinArea(hash, cacheDir)
	if _, err := os.Stat(nameInCache); err == nil {
		return nameInCache
	}

	// Check size of repo, prune stale chunks if necessary
	checkRepoSize()

	// TODO: Chunk is not in cache, download from upstream

	return nameInCache
}

// Get the filepath for a given hash in either the .stage or .cache area
func getBlobPathWithinArea(hash, area string) string {
	return path.Join(config.Config.S3gitCasPath, area, hash[0:2], hash[2:4], hash[4:])
}

// Move underlying chunks for a hash from .stage area to .cache area
func moveBlobToCache(hash string) error {

	leaves, err := openRoot(hash)
	if err != nil {
		return err
	}

	for _, l := range leaves {
		leaveHash := l.String()

		oldPath := getBlobPathWithinArea(leaveHash, stageDir)
		if _, err := os.Stat(oldPath); os.IsNotExist(err) {
			return err
		}

		hashDir := path.Join(config.Config.S3gitCasPath, cacheDir, leaveHash[0:2], leaveHash[2:4]) + "/"
		err := os.MkdirAll(hashDir, os.ModePerm)
		if err != nil {
			return err
		}
		newPath := hashDir + leaveHash[4:]

		err = os.Rename(oldPath, newPath)
		if err != nil {
			return err
		}
	}

	// TODO: Consider removing directories that are left empty in the .stage area
	//   However be aware of files moving in at potentially the same time (in a seperate process)
	//   Maybe do in separate maintenance step???
	return nil
}
