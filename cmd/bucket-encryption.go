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
	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
)

// BucketSSEConfigSys - in-memory cache of bucket encryption config
type BucketSSEConfigSys struct {
	Globals *Globals
}

// NewBucketSSEConfigSys - Creates an empty in-memory bucket encryption configuration cache
func NewBucketSSEConfigSys(g *Globals) *BucketSSEConfigSys {
	return &BucketSSEConfigSys{Globals: g}
}

// Get - gets bucket encryption config for the given bucket.
func (sys *BucketSSEConfigSys) Get(bucket string) (*bucketsse.BucketSSEConfig, error) {
	if sys.Globals.IsGateway {
		objAPI := sys.Globals.newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}

		return nil, BucketSSEConfigNotFound{Bucket: bucket}
	}

	return sys.Globals.BucketMetadataSys.GetSSEConfig(bucket)
}
