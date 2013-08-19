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
	"os"
	"path/filepath"

	"github.com/cznic/kv"
	"github.com/tgulacsi/weed-client"
)

// Client represents the Weed-FS client
type Client struct {
	db   *kv.DB
	weed weed.WeedClient
}

var kvOptions = new(kv.Options)

// NewClient creates a new client for the Weed-FS' masterURL
// using the given dbDir for the local DB.
func NewClient(masterURL string, dbDir string) (c *Client, err error) {
	c.weed = weed.NewWeedClient(masterURL)
	name := filepath.Join(dbDir,
		"camli-"+base64.URLEncoding.EncodeToString([]byte(masterURL))+".db")
	if c.db, err = kv.Open(name, kvOptions); err != nil {
		if _, ok := err.(*os.PathError); ok {
			c.db, err = kv.Create(name, kvOptions)
		}
	}
	return
}

// Get returns the file data as an io.ReadCloser, and the size
func (c *Client) Get(key string) (file io.ReadCloser, size int64, err error) {
	val, e := c.db.Get(nil, []byte(key))
	if e != nil {
		err = fmt.Errorf("error getting key %q from db: %s", key, e)
		return
	}
	if val == nil {
		err = fmt.Errorf("%q not found in db", key)
		return
	}
	var obj Object
	if err = decodeVal(&obj, val); err != nil {
		return
	}
	size = obj.Size
	file, err = c.weed.Download(obj.FileID)
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
		return err
	}
	obj.Size = size
	val, err := encodeVal(obj)
	if err != nil {
		return err
	}
	if err = c.db.Set([]byte(key), val); err != nil {
		return fmt.Errorf("error setting key %q to %q: %s", key, obj, err)
	}
	return c.db.Commit()
}

// Check checks the master's availability
func (c *Client) Check() error {
	_, _, err := c.weed.Status()
	return err
}

// Object holds the info about an object: the keys and size
type Object struct {
	FileID string // Weed-FS' key: the fileID
	Size   int64  // Size is the object's size
}

// List lists all the keys after the given key
func (c *Client) List(after string, limit int) ([]Object, error) {
	enum, _, err := c.db.Seek([]byte(after))
	if err != nil {
		return nil, err
	}
	objs := make([]Object, 0, 512)
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
		if err = decodeVal(&obj, val); err != nil {
			return nil, err
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

func encodeVal(obj Object) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	err := gob.NewEncoder(buf).Encode(obj)
	return buf.Bytes(), err
}
func decodeVal(obj *Object, val []byte) error {
	return gob.NewDecoder(bytes.NewReader(val)).Decode(obj)
}
