/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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

const (
	maxEConfigJSONSize = 262272
)

// Only valid query params for mgmt admin APIs.
const (
	mgmtBucket      = "bucket"
	mgmtPrefix      = "prefix"
	mgmtClientToken = "clientToken"
	mgmtForceStart  = "forceStart"
	mgmtForceStop   = "forceStop"
)

// ServerConnStats holds transferred bytes from/to the server
type ServerConnStats struct {
	TotalInputBytes  uint64 `json:"transferred"`
	TotalOutputBytes uint64 `json:"received"`
	Throughput       uint64 `json:"throughput,omitempty"`
	S3InputBytes     uint64 `json:"transferredS3"`
	S3OutputBytes    uint64 `json:"receivedS3"`
}

// ServerHTTPAPIStats holds total number of HTTP operations from/to the server,
// including the average duration the call was spent.
type ServerHTTPAPIStats struct {
	APIStats map[string]int `json:"apiStats"`
}

// ServerHTTPStats holds all type of http operations performed to/from the server
// including their average execution time.
type ServerHTTPStats struct {
	S3RequestsInQueue      int32              `json:"s3RequestsInQueue"`
	CurrentS3Requests      ServerHTTPAPIStats `json:"currentS3Requests"`
	TotalS3Requests        ServerHTTPAPIStats `json:"totalS3Requests"`
	TotalS3Errors          ServerHTTPAPIStats `json:"totalS3Errors"`
	TotalS3Canceled        ServerHTTPAPIStats `json:"totalS3Canceled"`
	TotalS3RejectedAuth    uint64             `json:"totalS3RejectedAuth"`
	TotalS3RejectedTime    uint64             `json:"totalS3RejectedTime"`
	TotalS3RejectedHeader  uint64             `json:"totalS3RejectedHeader"`
	TotalS3RejectedInvalid uint64             `json:"totalS3RejectedInvalid"`
}
