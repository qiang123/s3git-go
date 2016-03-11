package cas

import (
	"encoding/hex"
	"github.com/s3git/s3git-go/internal/kv"
	"os"
)

// Delete the chunks for a given blob
func deleteChunksForBlob(hash string) error {

	key, _ := hex.DecodeString(hash)
	leafHashes, _, err := kv.GetLevel1(key)
	if err != nil {
		return err
	}

	for i := 0; i < len(leafHashes); i += KeySize {
		leafKey := hex.EncodeToString(leafHashes[i : i+KeySize])

		// Test if available in cache directory
		nameInCache := getBlobPathWithinArea(leafKey, cacheDir)
		if _, err := os.Stat(nameInCache); err == nil {
			if err := os.Remove(nameInCache); err != nil {
				return err
			}
		}
	}

	return nil
}
