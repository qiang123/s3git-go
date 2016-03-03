package cas

import (
	"io"
	"os"
	"errors"
	"io/ioutil"
	"github.com/s3git/s3git-go/kv"
	"encoding/hex"
)

func MakeReader(hash string) *Reader {
	cr := Reader{hash: hash}
	err := cr.open(hash)
	if err != nil {
		return nil
	}
	return &cr
}

type Reader struct {
	hash        string
	offset      int64
	leafNr      int
	leaves      []Key
	chunkBuf    []byte
	chunkOffset int
	chunkLast   bool
}

func openRoot(hash string) ([]Key, error) {

	key, _ := hex.DecodeString(hash)
	leafHashes, _, err := kv.GetLevel1(key)
	if err != nil {
		return nil, err
	}

	// Has blob already been pulled down to disk?
	if len(leafHashes) == 0 {

		var err error
		leafHashes, err = pullDownOnDemand(hash)
		if err != nil {
			return nil, err
		}
	}
	leaves := make([]Key, 0, len(leafHashes)/KeySize)
	for i := 0; i < len(leafHashes); i += KeySize {
		leaves = append(leaves, NewKey(leafHashes[i:i+KeySize]))
	}
	return leaves, nil
}

// Pull a blob on demand from the back end store
func pullDownOnDemand(hash string) ([]byte, error) {

	return nil, nil
}

// Open the root hash of a blob
func (cr *Reader) open(hash string) error {

	var err error
	cr.leaves, err = openRoot(hash)
	return err
}

// Read the contents of a blob
func (cr *Reader) Read(p []byte) (n int, err error) {

	// TODO: Consider returning EOF in case of errors/failed functions?

	bytesToRead := len(p)

	if cr.chunkLast && cr.chunkOffset == 0 {
		return 0, io.EOF
	}

	for {
		if cr.chunkBuf == nil {
			key := cr.leaves[cr.leafNr].String()
			// TODO: Consider using a memory pool and reusing chunks?

			// Check whether chunk is available on local disk, if not, pull down to local disk
			chunkFile := getBlobPath(key)
			if _, err := os.Stat(chunkFile); os.IsNotExist(err) {

				// Chunk is missing, load file from back end
				// TODO: Ideally optimize here to just get the missing chunk (not whole file)
				_, err = pullDownOnDemand(cr.hash)
				if err != nil {
					return 0, err
				}

				// Double check that missing chunk is now available
				if _, err := os.Stat(chunkFile); os.IsNotExist(err) {
					return 0, errors.New("Failed to fetch missing chunk from back end")
				}
			}

			cr.chunkBuf, err = ioutil.ReadFile(chunkFile)
			if err != nil {
				return 0, err
			}
			cr.chunkOffset = 0
			cr.leafNr++
			cr.chunkLast = cr.leafNr == len(cr.leaves) // Mark when we have the last chunk
		}

		remainingInBuffer := len(cr.chunkBuf) - cr.chunkOffset
		bytesRead := min(bytesToRead, remainingInBuffer)
		copy(p[len(p)-bytesToRead:], cr.chunkBuf[cr.chunkOffset:cr.chunkOffset+bytesRead])
		bytesToRead -= bytesRead
		remainingInBuffer -= bytesRead
		cr.chunkOffset += bytesRead

		if bytesToRead <= 0 {
			return bytesRead, nil
		}

		if remainingInBuffer <= 0 {
			if cr.chunkLast {
				return bytesRead, io.EOF
			}
			cr.chunkBuf = nil
			cr.chunkOffset = 0
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
