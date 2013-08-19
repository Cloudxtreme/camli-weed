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
	"time"

	"camlistore.org/pkg/blobref"
)

// Stat checks for the existence of blobs, writing their sizes
// (if found back to the dest channel), and returning an error
// or nil.  Stat() should NOT close the channel.
// wait is the max time to wait for the blobs to exist,
// or 0 for no delay.
func (sto *weedStorage) StatBlobs(dest chan<- blobref.SizedBlobRef,
	blobs []*blobref.BlobRef,
	wait time.Duration) error {

	// TODO: do n stats in parallel
	for _, br := range blobs {
		size, err := sto.weedClient.Stat(br.String())
		if err == nil {
			dest <- blobref.SizedBlobRef{BlobRef: br, Size: size}
		} else {
			// TODO: handle
		}
	}
	return nil
}
