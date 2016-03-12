package cas

import (
	"os"
	"github.com/codahale/blake2"
	"path"
	"errors"
	"strings"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/s3git/s3git-go/internal/kv"
	"encoding/hex"
	mdb "github.com/szferi/gomdb"
)

func MakeWriter(objType string) *Writer {
	cw := Writer{areaDir: StageDir, objType: objType}
	cw.chunkBuf = make([]byte, ChunkSize)
	return &cw
}

// Use cheat mode for prefix objects
func MakeWriterInCheatMode(objType string) *Writer {
	cw := MakeWriter(objType)
	cw.cheatMode = true
	return cw
}

type Writer struct {
	cheatMode   bool
	leaves      []Key
	chunkBuf    []byte
	chunkOffset int
	objType     string
	areaDir		string
	flushed		bool
}

func (cw *Writer) SetAreaDir(dir string) {
	cw.areaDir = dir
}

func (cw *Writer) Write(p []byte) (nn int, err error) {

	for bytesToWrite := len(p); bytesToWrite > 0; {

		if cw.chunkOffset == ChunkSize {
			// Write out full chunk (without last chunk marker)
			cw.flush(false)
		}

		if cw.chunkOffset + bytesToWrite < ChunkSize {
			copy(cw.chunkBuf[cw.chunkOffset:], p[len(p)-bytesToWrite:])
			cw.chunkOffset += bytesToWrite
			bytesToWrite = 0
		} else {
			bytesWritten := ChunkSize - cw.chunkOffset
			copy(cw.chunkBuf[cw.chunkOffset:], p[len(p)-bytesToWrite:len(p)-bytesToWrite+bytesWritten])
			bytesToWrite -= bytesWritten
			cw.chunkOffset += bytesWritten
		}
	}

	return len(p), nil
}


func (cw *Writer) flush(isLastNode bool) {

	blake2 := blake2.New(&blake2.Config{Size: 64, Tree: &blake2.Tree{Fanout: 0, MaxDepth: 2, LeafSize: ChunkSize, NodeOffset: uint64(len(cw.leaves)), NodeDepth: 0, InnerHashSize: 64, IsLastNode: isLastNode}})
	blake2.Write(cw.chunkBuf[:cw.chunkOffset])

	leafKey := NewKey(blake2.Sum(nil))
	cw.leaves = append(cw.leaves, leafKey)

	chunkWriter, err := createBlobFile(leafKey.String(), cw.areaDir)
	if err != nil {
		return
	}
	chunkWriter.Write(cw.chunkBuf[:cw.chunkOffset])
	defer chunkWriter.Close()
	chunkWriter.Sync()

	// Start over in buffer
	cw.chunkOffset = 0
}

func (cw *Writer) Flush() (string, []byte, bool, error) {

	// Close last node
	cw.flush(true)

	cw.flushed = true

	// Compute hash of level 1 root key
	blake2 := blake2.New(&blake2.Config{Size: 64, Tree: &blake2.Tree{Fanout: 0, MaxDepth: 2, LeafSize: ChunkSize, NodeOffset: 0, NodeDepth: 1, InnerHashSize: 64, IsLastNode: true}})

	// Iterate over hashes of all underlying nodes
	for _, leave := range cw.leaves {
		blake2.Write(leave.object[:])
	}

	rootStr := NewKey(blake2.Sum(nil)).String()
	if cw.cheatMode {
		repeatLastChar := strings.Repeat(string(rootStr[prefixNum-prefixCheat-1]), prefixCheat)
		rootStr = rootStr[0:prefixNum-prefixCheat] + repeatLastChar + rootStr[prefixNum:]
	}

	leafHashes := make([]byte, len(cw.leaves)*KeySize)
	for index, leave := range cw.leaves {
		offset := KeySize * index
		copy(leafHashes[offset:offset+KeySize], leave.object[:])
	}

	key, _ := hex.DecodeString(rootStr)
	val, _, err := kv.GetLevel1(key)
	if err != nil && !(err == mdb.NotFound) {
		return "", nil, false, err
	}
	newBlob := err == mdb.NotFound
	newBlob = newBlob || len(val) == 0	// Also write leafHashes to KV database when no leafHashes were stored previously (BLOB was not pulled down)

	if newBlob {
		err := kv.AddToLevel1(key, leafHashes, cw.objType)
		if err != nil {
			return "", nil, false, err
		}
	}

	return rootStr, leafHashes, newBlob, nil
}

func (cw *Writer) Close() error {
	if !cw.flushed {
		return errors.New("Stream closed without being flushed!")
	}
	return nil
}

func createBlobFile(hash, areaDir string) (*os.File, error) {

	checkRepoSize()

	hashDir := path.Join(config.Config.S3gitCasPath, areaDir, hash[0:2], hash[2:4]) + "/"
	err := os.MkdirAll(hashDir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return os.Create(hashDir + hash[4:])

}
