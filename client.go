/*
Copyright 2013 Tamás Gulácsi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package camliWeed

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io"
	"path/filepath"

	"github.com/cznic/kv"
	"github.com/tgulacsi/weed-client"
)

// Client represents the Weed-FS client
type Client struct {
	db   *kv.DB
	weed weed.WeedClient
}

// kvOptions returns the usable kv.Options - according to cnic, reuse may be unsafe
func kvOptions() *kv.Options {
    return new(kv.Options)
}

// NewClient creates a new client for the Weed-FS' masterURL
// using the given dbDir for the local DB.
func NewClient(masterURL string, dbDir string) (c *Client, err error) {
	c = &Client{weed: weed.NewWeedClient(masterURL)}
	name := filepath.Join(dbDir,
		"camli-"+base64.URLEncoding.EncodeToString([]byte(masterURL))+".db")
	if c.db, err = kv.Open(name, kvOptions()); err != nil {
        var e error
		c.db, e = kv.Create(name, kvOptions())
        if e != nil {
            err = fmt.Errorf("open error: %s; create error: %s", err ,e)
        }
	}
	if err == nil && c.db == nil {
		err = fmt.Errorf("couldn't create db as %s", name)
	}
	return
}

// Get returns the file data as an io.ReadCloser, and the size
func (c *Client) Get(key string) (file io.ReadCloser, size int64, err error) {
	fileID, s, e := c.dbGet(key)
	if e != nil {
		err = e
		return
	}
	size = s
	file, err = c.weed.Download(fileID)
	return
}

func (c *Client) dbGet(key string) (fileID string, size int64, err error) {
	val, e := c.db.Get(nil, []byte(key))
	if e != nil {
		err = fmt.Errorf("error getting key %q from db: %s", key, e)
		return
	}
	if val == nil {
		err = fmt.Errorf("%q not found in db", key)
		return
	}
	err = decodeVal(val, &fileID, &size)
	return
}

// Put stores the file data
func (c *Client) Put(key string, size int64, file io.Reader) error {
	err := c.db.BeginTransaction()
	if err != nil {
		return fmt.Errorf("error beginning transaction: %s", err)
	}
	var obj Object
	if obj.FileID, err = c.weed.Upload(key, "application/octet-stream", file); err != nil {
		c.db.Rollback()
		return err
	}
	obj.Size = size
	val, err := encodeVal(nil, obj.FileID, size)
	if err != nil {
		return err
	}
	if err = c.db.Set([]byte(key), val); err != nil {
		return fmt.Errorf("error setting key %q to %q: %s", key, obj, err)
	}
	return c.db.Commit()
}

// Delete deletes the key from the backing Weed-FS and from the local db
func (c *Client) Delete(key string) error {
	fileID, _, err := c.dbGet(key)
	if err != nil {
		return err
	}
	if err = c.db.BeginTransaction(); err != nil {
		return err
	}
	if err = c.db.Delete([]byte(key)); err != nil {
		c.db.Rollback()
		return err
	}
	if err = c.weed.Delete(fileID); err != nil {
		c.db.Rollback()
		return err
	}
	return c.db.Commit()
}

// Stat returns the size of the key's file
func (c *Client) Stat(key string) (int64, error) {
	_, size, err := c.dbGet(key)
	return size, err
}

// Check checks the master's availability
func (c *Client) Check() error {
	_, _, err := c.weed.Status()
	return err
}

// Object holds the info about an object: the keys and size
type Object struct {
	Key    string // Camlistore's key: the blobref
	FileID string // Weed-FS' key: the fileID
	Size   int64  // Size is the object's size
}

// List lists all the keys after the given key
func (c *Client) List(after string, limit int) ([]Object, error) {
	enum, _, err := c.db.Seek([]byte(after))
	if err != nil {
		return nil, err
	}
	n := limit / 2
	if limit < 1000 {
		n = limit
	}

	objs := make([]Object, 0, n)
	var (
		key, val []byte
		obj      Object
		e        error
	)
	for i := 0; i < limit; i++ {
		if key, val, e = enum.Next(); e != nil {
			if e != io.EOF {
				return nil, e
			}
			break
		}
		obj.Key = string(key)
		if err = decodeVal(val, &obj.FileID, &obj.Size); err != nil {
			return nil, err
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

func encodeVal(dst []byte, fileID string, size int64) ([]byte, error) {
	if dst == nil {
		dst = make([]byte, 0, 48)
	}
	buf := bytes.NewBuffer(dst)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(size)
	if err != nil {
		return nil, err
	}
	err = enc.Encode(fileID)
	return buf.Bytes(), err
}
func decodeVal(val []byte, fileID *string, size *int64) error {
	dec := gob.NewDecoder(bytes.NewReader(val))
	err := dec.Decode(size)
	if err != nil {
		return err
	}
	return dec.Decode(fileID)
}
