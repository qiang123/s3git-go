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
	"github.com/bmatsuo/lmdb-go/lmdb"
)

func AddLevel0Stage(hash string, size uint32) error {

	return addLevel0(&dbiLevel0StageSize, hash, size)
}

func AddLevel0Cache(hash string, size uint32) error {

	return addLevel0(&dbiLevel0CacheSize, hash, size)
}

func addLevel0(dbi *lmdb.DBI, hash string, size uint32) error {

	hx, _ := hex.DecodeString(hash)
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, size)

	err := env.Update(func(txn *lmdb.Txn) (err error) {
		return txn.Put(*dbi, hx, val, 0)
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

func GetLevel0StageSize() (uint64, error) {

	return getLevel0Size(&dbiLevel0StageSize)
}

func GetLevel0CacheSize() (uint64, error) {

	return getLevel0Size(&dbiLevel0CacheSize)
}

func getLevel0Size(dbi *lmdb.DBI) (uint64, error) {

	var size uint64

	err := env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(*dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			_, v, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			leafSize := binary.LittleEndian.Uint32(v)
			size += uint64(leafSize)
		}
	})

	if err != nil {
		return 0, err
	}

	return size, nil
}
