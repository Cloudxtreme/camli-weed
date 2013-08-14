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

/*
Package camliWeed registers the "weed" blobserver storage type, storing
blobs in a Weed-FS' storage.

Example low-level config:

     "/r1/": {
         "handler": "storage-weed",
         "handlerArgs": {
            "master": "http://localhost:9333",
          }
     },

*/
package camliWeed

import (
	"fmt"

	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/jsonconfig"
)

type weedStorage struct {
	*blobserver.SimpleBlobHubPartitionMap
	weedClient *Client
	masterURL  string
}

func newFromConfig(_ blobserver.Loader, config jsonconfig.Obj) (storage blobserver.Storage, err error) {
	stor := &weedStorage{
		SimpleBlobHubPartitionMap: &blobserver.SimpleBlobHubPartitionMap{},
		masterURL:                 config.RequiredString("master"),
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err = stor.weedClient.Check(); err != nil {
		return nil, fmt.Errorf("Weed master check error: %s", err)
	}
	return stor, nil
}

func init() {
	blobserver.RegisterStorageConstructor("weed", blobserver.StorageConstructor(newFromConfig))
}
