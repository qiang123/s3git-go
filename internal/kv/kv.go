package kv

import (
	"encoding/hex"
	"fmt"
	"github.com/s3git/s3git-go/internal/config"
	mdb "github.com/szferi/gomdb"
	"os"
	"path"
)

var env *mdb.Env

// KV databases containing root level digests for different object types
// When a particular key is present, the value is as follows:
//   - when empty, underlying chunk(s) are not cached locally
//   - when set, it is the concatenation of the leaf level digests for all nodes
//     (and thus necessarily needs to correspond when BLAKE2'd to its key)
//
// If you know the type of the key, you can fetch it directly for the corresponding database
// If you do not know the type, you need to search all stores

var dbiLevel1Blobs mdb.DBI
var dbiLevel1Commits mdb.DBI
var dbiLevel1Prefixes mdb.DBI
var dbiLevel1Trees mdb.DBI

// KV database containing overview of added/removed blobs in stage
var dbiStage mdb.DBI

// KV database that marks commits objects as being a parent commit
var dbiLevel1CommitsIsParent mdb.DBI

func OpenDatabase() error {

	mdbDir := path.Join(config.Config.CasPath, ".mdb")
	err := os.MkdirAll(mdbDir, 0777)
	if err != nil {
		return err
	}

	env, _ = mdb.NewEnv()
	// TODO: Figure out proper size for lmdb
	// TODO: Windows: max size is capped at 32
	env.SetMapSize(1 << 36) // max file size
	env.SetMaxDBs(10)       // up to 10 named databases
	env.Open(mdbDir, 0, 0664)
	txn, _ := env.BeginTxn(nil, 0)

	// overview of blobs in stage
	dbstage := "stage"
	dbiStage, _ = txn.DBIOpen(&dbstage, mdb.CREATE)

	// Level 1 databases
	dbl1blobs := "l1blobs"
	dbiLevel1Blobs, _ = txn.DBIOpen(&dbl1blobs, mdb.CREATE)
	dbl1commits := "l1commits"
	dbiLevel1Commits, _ = txn.DBIOpen(&dbl1commits, mdb.CREATE)
	dbl1prefixes := "l1prefixes"
	dbiLevel1Prefixes, _ = txn.DBIOpen(&dbl1prefixes, mdb.CREATE)
	dbl1trees := "l1trees"
	dbiLevel1Trees, _ = txn.DBIOpen(&dbl1trees, mdb.CREATE)

	// list of top most commits
	dbcommitsisparents := "l1commitsisparent"
	dbiLevel1CommitsIsParent, _ = txn.DBIOpen(&dbcommitsisparents, mdb.CREATE)

	txn.Commit()

	// TODO: Make sure all databases are flushed before exiting program
	//	defer env.DBIClose(dbi)
	//	defer env.Close()

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

	txn, _ := env.BeginTxn(nil, mdb.RDONLY)
	defer txn.Abort()

	_, err := txn.Get(dbiLevel1CommitsIsParent, key)
	if err != nil && !(err == mdb.NotFound) {
		return false, err
	} else if !(err == mdb.NotFound) {
		return true, err
	}

	return false, mdb.NotFound
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
			if err != nil && err != mdb.NotFound {
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

const BLOB = "blob"
const COMMIT = "commit"
const PREFIX = "prefix"
const TREE = "tree"

func getDbForObjectType(objType string) *mdb.DBI {

	var dbi *mdb.DBI
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

	txn, _ := env.BeginTxn(nil, mdb.RDONLY)
	defer txn.Abort()

	val, err := txn.Get(dbiLevel1Blobs, key)
	if err != nil && !(err == mdb.NotFound) {
		return nil, "", err
	} else if !(err == mdb.NotFound) {
		return val, BLOB, err
	}

	val, err = txn.Get(dbiLevel1Commits, key)
	if err != nil && !(err == mdb.NotFound) {
		return nil, "", err
	} else if !(err == mdb.NotFound) {
		return val, COMMIT, err
	}

	val, err = txn.Get(dbiLevel1Prefixes, key)
	if err != nil && !(err == mdb.NotFound) {
		return nil, "", err
	} else if !(err == mdb.NotFound) {
		return val, PREFIX, err
	}

	val, err = txn.Get(dbiLevel1Trees, key)
	if err != nil && !(err == mdb.NotFound) {
		return nil, "", err
	} else if !(err == mdb.NotFound) {
		return val, TREE, err
	}

	return nil, "", mdb.NotFound
}

func listMdb(dbi *mdb.DBI, query string) (<-chan []byte, error) {

	result := make(chan []byte)

	go func() {
		// make sure we always close the channel
		defer close(result)

		// scan the database
		txn, _ := env.BeginTxn(nil, mdb.RDONLY)
		defer txn.Abort()
		cursor, _ := txn.CursorOpen(*dbi)
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
				bval, _, err := cursor.GetVal(queryKey, nil, mdb.SET_RANGE)
				if err == mdb.NotFound {
					break
				}
				if err != nil {
					// TODO: Log error
					return
				}

				bkey = bval.Bytes()

				setRangeUponStart = false
			} else {
				var err error
				bkey, _, err = cursor.Get(nil, nil, mdb.NEXT)
				if err == mdb.NotFound {
					break
				}
				if err != nil {
					// TODO: Log error
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
