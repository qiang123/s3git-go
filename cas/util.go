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
	return path.Join(config.Config.LdCasPath, area, hash[0:2], hash[2:4], hash[4:])
}
