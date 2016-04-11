/*
 * Copyright 2016 Frank Wessels <fwessels@xs4all.nl>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kv

import (
	"encoding/hex"
	"encoding/binary"
	"fmt"
	"github.com/s3git/s3git-go/internal/config"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"os"
	"path"
)

// TODO: Use new transaction style for lmdb

var env *lmdb.Env

// KV databases containing root level digests for different object types
// When a particular key is present, the value is as follows:
//   - when empty, underlying chunk(s) are not cached locally
//   - when set, it is the concatenation of the leaf level digests for all nodes
//     (and thus necessarily needs to correspond when BLAKE2'd to its key)
//
// If you know the type of the key, you can fetch it directly for the corresponding database
// If you do not know the type, you need to search all stores

var dbiLevel1Blobs lmdb.DBI
var dbiLevel1Commits lmdb.DBI
var dbiLevel1Prefixes lmdb.DBI
var dbiLevel1Trees lmdb.DBI

var dbiLevel0CacheSize lmdb.DBI
var dbiLevel0StageSize lmdb.DBI

// KV database containing overview of added/removed blobs in stage
var dbiStage lmdb.DBI

// KV database that marks commits objects as being a parent commit
var dbiLevel1CommitsIsParent lmdb.DBI

func OpenDatabase() error {

	mdbDir := path.Join(config.Config.BasePath, config.S3GIT_DIR, "mdb")
	err := os.MkdirAll(mdbDir, os.ModePerm)
	if err != nil {
		return err
	}

	env, _ = lmdb.NewEnv()
	// TODO: Figure out proper size for lmdb
	// TODO: Windows: max size is capped at 32
	env.SetMapSize(1 << 36) // max file size
	env.SetMaxDBs(10)       // up to 10 named databases
	env.Open(mdbDir, 0, 0664)

	err = env.Update(func(txn *lmdb.Txn) (err error) {

		// overview of blobs in stage
		dbiStage, err = txn.OpenDBI("stage", lmdb.Create)
		if err != nil {
			return err
		}

		// Level 1 databases
		dbiLevel1Blobs, err = txn.OpenDBI("l1blobs", lmdb.Create)
		if err != nil {
			return err
		}
		dbiLevel1Commits, err = txn.OpenDBI("l1commits", lmdb.Create)
		if err != nil {
			return err
		}
		dbiLevel1Prefixes, err = txn.OpenDBI("l1prefixes", lmdb.Create)
		if err != nil {
			return err
		}
		dbiLevel1Trees, err = txn.OpenDBI("l1trees", lmdb.Create)
		if err != nil {
			return err
		}

		// Level 0 leaf databases
		dbiLevel0CacheSize, err = txn.OpenDBI("l0cache", lmdb.Create)
		if err != nil {
			return err
		}
		dbiLevel0StageSize, err = txn.OpenDBI("l0stage", lmdb.Create)
		if err != nil {
			return err
		}

		// list of top most commits
		dbiLevel1CommitsIsParent, err = txn.OpenDBI("l1commitsisparent", lmdb.Create)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	// From https://godoc.org/github.com/bmatsuo/lmdb-go/lmdb
	//   A database is referenced by an opaque handle known as its DBI which
	//   must be opened inside a transaction with the OpenDBI or OpenRoot methods.
	//   DBIs may be closed but it is not required. Typically, applications acquire
	//   handles for all their databases immediately after opening an environment
	//   and retain them for the lifetime of the process.

	return nil
}

func AddToStage(key string) error {

	hx, _ := hex.DecodeString(key)

	txn, _ := env.BeginTxn(nil, 0)
	txn.Put(dbiStage, hx, nil, 0)
	txn.Commit()

	return nil
}

func ClearStage() error {

	list, err := listMdb(&dbiStage, "")
	if err != nil {
		return err
	}

	txn, _ := env.BeginTxn(nil, 0)
	for k := range list {
		txn.Del(dbiStage, k, nil)
	}
	txn.Commit()

	return nil
}

func ListStage() (<-chan []byte, error) {

	return listMdb(&dbiStage, "")
}

func MarkCommitAsParent(key string) error {

	hx, _ := hex.DecodeString(key)

	txn, _ := env.BeginTxn(nil, 0)
	txn.Put(dbiLevel1CommitsIsParent, hx, nil, 0)
	txn.Commit()

	return nil
}

func CommitIsParent(key []byte) (bool, error) {

	txn, _ := env.BeginTxn(nil, lmdb.Readonly)
	defer txn.Abort()

	_, err := txn.Get(dbiLevel1CommitsIsParent, key)
	if err != nil && !lmdb.IsNotFound(err) {
		return false, err
	} else if !lmdb.IsNotFound(err) {
		return true, err
	}

	return false, lmdb.NotFound
}

func ListTopMostCommits() (<-chan []byte, error) {

	list, err := ListLevel1Commits()
	if err != nil {
		return nil, err
	}

	result := make(chan []byte)

	go func() {
		// make sure we always close the channel
		defer close(result)

		for l := range list {
			isParent, err := CommitIsParent(l)
			if err != nil && !lmdb.IsNotFound(err) {
				return
			}

			if !isParent { // In case this commit is not a parent --> output it as a top most commit
				result <- l
			}
		}
	}()

	return result, nil
}

func ListLevel1Commits() (<-chan []byte, error) {

	return listMdb(&dbiLevel1Commits, "")
}

func ListLevel1Prefixes() (<-chan []byte, error) {

	return listMdb(&dbiLevel1Prefixes, "")
}

func ListLevel1Trees() (<-chan []byte, error) {

	return listMdb(&dbiLevel1Trees, "")
}

func ListLevel1Blobs(query string) (<-chan []byte, error) {

	return listMdb(&dbiLevel1Blobs, query)
}

func GetLevel1BlobsStats() (uint64, error) {

	txn, _ := env.BeginTxn(nil, lmdb.Readonly)
	defer txn.Abort()
	stats, err := txn.Stat(dbiLevel1Blobs)
	if err != nil {
		return 0, err
	}

	return stats.Entries, nil
}

const BLOB = "blob"
const COMMIT = "commit"
const PREFIX = "prefix"
const TREE = "tree"

func getDbForObjectType(objType string) *lmdb.DBI {

	var dbi *lmdb.DBI
	switch objType {
	case BLOB:
		dbi = &dbiLevel1Blobs
	case COMMIT:
		dbi = &dbiLevel1Commits
	case PREFIX:
		dbi = &dbiLevel1Prefixes
	case TREE:
		dbi = &dbiLevel1Trees
	default:
		panic(fmt.Sprintf("Bad type: %s", objType))
	}
	return dbi
}

func AddToLevel1(key, value []byte, objType string) error {

	dbi := getDbForObjectType(objType)
	txn, _ := env.BeginTxn(nil, 0)
	txn.Put(*dbi, key, value, 0)
	txn.Commit()

	return nil
}

func AddMultiToLevel1(keys, values [][]byte, objType string) error {

	dbi := getDbForObjectType(objType)
	txn, _ := env.BeginTxn(nil, 0)
	for index, key := range keys {
		txn.Put(*dbi, key, values[index], 0)
	}
	txn.Commit()

	return nil
}

// Get object of any type, return value and type
func GetLevel1(key []byte) ([]byte, string, error) {

	txn, _ := env.BeginTxn(nil, lmdb.Readonly)
	defer txn.Abort()

	val, err := txn.Get(dbiLevel1Blobs, key)
	if err != nil && !lmdb.IsNotFound(err) {
		return nil, "", err
	} else if !lmdb.IsNotFound(err) {
		return val, BLOB, err
	}

	val, err = txn.Get(dbiLevel1Commits, key)
	if err != nil && !lmdb.IsNotFound(err) {
		return nil, "", err
	} else if !lmdb.IsNotFound(err) {
		return val, COMMIT, err
	}

	val, err = txn.Get(dbiLevel1Prefixes, key)
	if err != nil && !lmdb.IsNotFound(err) {
		return nil, "", err
	} else if !lmdb.IsNotFound(err) {
		return val, PREFIX, err
	}

	val, err = txn.Get(dbiLevel1Trees, key)
	if err != nil && !lmdb.IsNotFound(err) {
		return nil, "", err
	} else if !lmdb.IsNotFound(err) {
		return val, TREE, err
	}

	return nil, "", lmdb.NotFound
}

func AddLevel0Stage(hash string, size uint32) error {

	hx, _ := hex.DecodeString(hash)
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, size)

	err := env.Update(func(txn *lmdb.Txn) (err error) {
		return txn.Put(dbiLevel0StageSize, hx, val, 0)
	})
	return err
}

func AddLevel0Cache(hash string, size uint32) error {

	hx, _ := hex.DecodeString(hash)
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, size)

	err := env.Update(func(txn *lmdb.Txn) (err error) {
		return txn.Put(dbiLevel0CacheSize, hx, val, 0)
	})
	return err
}

func MoveLevel0FromStageToCache(hash string) error {

	hx, _ := hex.DecodeString(hash)

	var val []byte

	// First obtain current value
	err := env.View(func(txn *lmdb.Txn) (err error) {
		var err2 error
		val, err2 = txn.Get(dbiLevel0StageSize, hx)
		return err2
	})
	if err != nil {
		return err
	}

	err = env.Update(func(txn *lmdb.Txn) (err error) {

		var err2 error
		// First delete from stage
		err2 = txn.Del(dbiLevel0StageSize, hx, nil)
		if err2 != nil {
			return err2
		}

		// Then add item to cache
		return txn.Put(dbiLevel0CacheSize, hx, val, 0)
	})
	if err != nil {
		return err
	}

	return err
}

func listMdb(dbi *lmdb.DBI, query string) (<-chan []byte, error) {

	result := make(chan []byte)

	go func() {
		// make sure we always close the channel
		defer close(result)

		// scan the database
		txn, _ := env.BeginTxn(nil, lmdb.Readonly)
		defer txn.Abort()
		cursor, _ := txn.OpenCursor(*dbi)
		defer cursor.Close()

		setRangeUponStart := len(query) > 0
		var queryKey []byte
		if setRangeUponStart {
			q := query
			if len(q)%2 == 1 {
				q = q + "0"
			}
			queryKey, _ = hex.DecodeString(q)
		}

		for {

			var bkey []byte
			if setRangeUponStart {
				var err error
				bkey, _, err = cursor.Get(queryKey, nil, lmdb.SetRange)
				if lmdb.IsNotFound(err) {
					break
				}
				if err != nil {
					fmt.Fprint(os.Stderr, err)
					return
				}

				setRangeUponStart = false
			} else {
				var err error
				bkey, _, err = cursor.Get(nil, nil, lmdb.Next)
				if lmdb.IsNotFound(err) {
					break
				}
				if err != nil {
					fmt.Fprint(os.Stderr, err)
					return
				}
			}

			// break early if start of key is not longer
			if hex.EncodeToString(bkey)[:len(query)] != query {
				break
			}
			result <- bkey
		}

	}()

	return result, nil
}
