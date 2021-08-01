package gokv

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

const MapSize = 1024

type DB struct {
	data map[string][]byte
	path string
	lock sync.RWMutex
}

func Open(path string) (*DB, error) {
	var db = &DB{}
	f, err := os.Open(path)
	if err != nil {
		if !os.IsExist(err) {
			db.data = make(map[string][]byte, MapSize)
		} else {
			return nil, err
		}
	} else {
		defer f.Close()
		buff := make([]byte, 5)
		len, err := f.Read((buff))

		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(buff, &len); err != nil {
			return nil, err
		}
		buff1 := make([]byte, len)
		len, err = f.ReadAt(buff1, 5)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(buff1, &db.data); err != nil {
			return nil, err
		}

	}
	db.path = path
	return db, nil
}

func (db *DB) Put(key string, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.data[key] = value
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	if val, ok := db.data[key]; ok {
		return val, nil
	}
	return nil, errors.New("value not found")
}

func (db *DB) Del(key string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	delete(db.data, key)
	return nil
}

func (db *DB) Close() error {
	buff, err := json.Marshal(db.data)
	if err != nil {
		return err
	}
	f, err := os.Create(db.path)
	if err != nil {
		return err
	}
	l := len(buff)
	buff0, err := json.Marshal(l)
	if err != nil {
		return err
	}
	fmt.Println(len(buff0))
	buff = append(buff0, buff...)
	if _, err = f.WriteAt(buff, 0); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return nil
}
