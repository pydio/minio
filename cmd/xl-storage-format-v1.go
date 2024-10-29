/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package cmd

import (
	"time"
)

// StatInfo - carries stat information of the object.
type StatInfo struct {
	Size    int64     `json:"size"`    // Size of the object `xl.meta`.
	ModTime time.Time `json:"modTime"` // ModTime of the object `xl.meta`.
}

// BitrotAlgorithm specifies a algorithm used for bitrot protection.
type BitrotAlgorithm uint

const (
	// SHA256 represents the SHA-256 hash function
	SHA256 BitrotAlgorithm = 1 + iota
	// HighwayHash256 represents the HighwayHash-256 hash function
	HighwayHash256
	// HighwayHash256S represents the Streaming HighwayHash-256 hash function
	HighwayHash256S
	// BLAKE2b512 represents the BLAKE2b-512 hash function
	BLAKE2b512
)

// ObjectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type ObjectPartInfo struct {
	ETag       string `json:"etag,omitempty"`
	Number     int    `json:"number"`
	Size       int64  `json:"size"`
	ActualSize int64  `json:"actualSize"`
}
