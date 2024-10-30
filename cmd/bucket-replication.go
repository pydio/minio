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
	"context"
	"fmt"
	"net/http"
	"strings"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/pkg/bucket/replication"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

// gets replication config associated to a given bucket name.
func getReplicationConfig(ctx context.Context, bucketName string) (rc *replication.Config, err error) {
	globals := mustGlobalsFromContext(ctx)
	if globals.IsGateway {
		objAPI := globals.newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}

		return nil, BucketReplicationConfigNotFound{Bucket: bucketName}
	}

	return globals.BucketMetadataSys.GetReplicationConfig(ctx, bucketName)
}

// mustReplicate returns 2 booleans - true if object meets replication criteria and true if replication is to be done in
// a synchronous manner.
func mustReplicate(ctx context.Context, r *http.Request, bucket, object string, meta map[string]string, replStatus string) (replicate bool, sync bool) {
	if s3Err := isPutActionAllowed(ctx, getRequestAuthType(r), bucket, "", r, iampolicy.GetReplicationConfigurationAction); s3Err != ErrNone {
		return
	}
	return mustReplicater(ctx, bucket, object, meta, replStatus)
}

// mustReplicater returns 2 booleans - true if object meets replication criteria and true if replication is to be done in
// a synchronous manner.
func mustReplicater(ctx context.Context, bucket, object string, meta map[string]string, replStatus string) (replicate bool, sync bool) {
	globals := mustGlobalsFromContext(ctx)
	if globals.IsGateway {
		return replicate, sync
	}
	if rs, ok := meta[xhttp.AmzBucketReplicationStatus]; ok {
		replStatus = rs
	}
	if replication.StatusType(replStatus) == replication.Replica {
		return replicate, sync
	}
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		return replicate, sync
	}
	opts := replication.ObjectOpts{
		Name: object,
		SSEC: crypto.SSEC.IsEncrypted(meta),
	}
	tagStr, ok := meta[xhttp.AmzObjectTagging]
	if ok {
		opts.UserTags = tagStr
	}
	tgt := globals.BucketTargetSys.GetRemoteTargetClient(ctx, cfg.RoleArn)
	// the target online status should not be used here while deciding
	// whether to replicate as the target could be temporarily down
	if tgt != nil {
		return cfg.Replicate(opts), tgt.replicateSync
	}
	return cfg.Replicate(opts), false
}

// Standard headers that needs to be extracted from User metadata.
var standardHeaders = []string{
	xhttp.ContentType,
	xhttp.CacheControl,
	xhttp.ContentEncoding,
	xhttp.ContentLanguage,
	xhttp.ContentDisposition,
	xhttp.AmzStorageClass,
	xhttp.AmzObjectTagging,
	xhttp.AmzBucketReplicationStatus,
	xhttp.AmzObjectLockMode,
	xhttp.AmzObjectLockRetainUntilDate,
	xhttp.AmzObjectLockLegalHold,
	xhttp.AmzTagCount,
	xhttp.AmzServerSideEncryption,
}

// returns true if any of the objects being deleted qualifies for replication.
func hasReplicationRules(ctx context.Context, bucket string, objects []ObjectToDelete) bool {
	c, err := getReplicationConfig(ctx, bucket)
	if err != nil || c == nil {
		return false
	}
	for _, obj := range objects {
		if c.HasActiveRules(obj.ObjectName, true) {
			return true
		}
	}
	return false
}

// isStandardHeader returns true if header is a supported header and not a custom header
func isStandardHeader(matchHeaderKey string) bool {
	return equals(matchHeaderKey, standardHeaders...)
}

// returns whether object version is a deletemarker and if object qualifies for replication
func checkReplicateDelete(ctx context.Context, bucket string, dobj ObjectToDelete, oi ObjectInfo, gerr error) (replicate, sync bool) {
	rcfg, err := getReplicationConfig(ctx, bucket)
	if err != nil || rcfg == nil {
		return false, sync
	}
	opts := replication.ObjectOpts{
		Name:         dobj.ObjectName,
		SSEC:         crypto.SSEC.IsEncrypted(oi.UserDefined),
		UserTags:     oi.UserTags,
		DeleteMarker: oi.DeleteMarker,
		VersionID:    dobj.VersionID,
		OpType:       replication.DeleteReplicationType,
	}
	replicate = rcfg.Replicate(opts)
	// when incoming delete is removal of a delete marker( a.k.a versioned delete),
	// GetObjectInfo returns extra information even though it returns errFileNotFound
	if gerr != nil {
		validReplStatus := false
		switch oi.ReplicationStatus {
		case replication.Pending, replication.Completed, replication.Failed:
			validReplStatus = true
		}
		if oi.DeleteMarker && (validReplStatus || replicate) {
			return true, sync
		}
		// can be the case that other cluster is down and duplicate `mc rm --vid`
		// is issued - this still needs to be replicated back to the other target
		return oi.VersionPurgeStatus == Pending || oi.VersionPurgeStatus == Failed, sync
	}
	globals := mustGlobalsFromContext(ctx)
	tgt := globals.BucketTargetSys.GetRemoteTargetClient(ctx, rcfg.RoleArn)
	// the target online status should not be used here while deciding
	// whether to replicate deletes as the target could be temporarily down
	if tgt == nil {
		return false, false
	}
	return replicate, tgt.replicateSync
}

type caseInsensitiveMap map[string]string

// Lookup map entry case insensitively.
func (m caseInsensitiveMap) Lookup(key string) (string, bool) {
	if len(m) == 0 {
		return "", false
	}
	for _, k := range []string{
		key,
		strings.ToLower(key),
		http.CanonicalHeaderKey(key),
	} {
		v, ok := m[k]
		if ok {
			return v, ok
		}
	}
	return "", false
}

type replicationAction string

const (
	replicateMetadata replicationAction = "metadata"
	replicateNone     replicationAction = "none"
	replicateAll      replicationAction = "all"
)

// matches k1 with all keys, returns 'true' if one of them matches
func equals(k1 string, keys ...string) bool {
	for _, k2 := range keys {
		if strings.ToLower(k1) == strings.ToLower(k2) {
			return true
		}
	}
	return false
}

// replicateObject replicates the specified version of the object to destination bucket
// The source object is then updated to reflect the replication status.
func replicateObject(ctx context.Context, ri ReplicateObjectInfo, objectAPI ObjectLayer) {
	return
}

// filterReplicationStatusMetadata filters replication status metadata for COPY
func filterReplicationStatusMetadata(metadata map[string]string) map[string]string {
	// Copy on write
	dst := metadata
	var copied bool
	delKey := func(key string) {
		if _, ok := metadata[key]; !ok {
			return
		}
		if !copied {
			dst = make(map[string]string, len(metadata))
			for k, v := range metadata {
				dst[k] = v
			}
			copied = true
		}
		delete(dst, key)
	}

	delKey(xhttp.AmzBucketReplicationStatus)
	return dst
}

// DeletedObjectVersionInfo has info on deleted object
type DeletedObjectVersionInfo struct {
	DeletedObject
	Bucket string
}

// get Reader from replication target if active-active replication is in place and
// this node returns a 404
func proxyGetToReplicationTarget(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, proxy bool) {
	tgt, oi, proxy, err := proxyHeadToRepTarget(ctx, bucket, object, opts)
	if !proxy || err != nil {
		return nil, false
	}
	fn, off, length, err := NewGetObjectReader(rs, oi, opts)
	if err != nil {
		return nil, false
	}
	gopts := miniogo.GetObjectOptions{
		VersionID:            opts.VersionID,
		ServerSideEncryption: opts.ServerSideEncryption,
		Internal: miniogo.AdvancedGetOptions{
			ReplicationProxyRequest: "true",
		},
	}
	// get correct offsets for encrypted object
	if off >= 0 && length >= 0 {
		if err := gopts.SetRange(off, off+length-1); err != nil {
			return nil, false
		}
	}
	// Make sure to match ETag when proxying.
	if err = gopts.SetMatchETag(oi.ETag); err != nil {
		return nil, false
	}
	c := miniogo.Core{Client: tgt.Client}
	obj, _, _, err := c.GetObject(ctx, bucket, object, gopts)
	if err != nil {
		return nil, false
	}
	closeReader := func() { obj.Close() }

	reader, err := fn(obj, h, opts.CheckPrecondFn, closeReader)
	if err != nil {
		return nil, false
	}
	reader.ObjInfo = oi.Clone()
	return reader, true
}

// isProxyable returns true if replication config found for this bucket
func isProxyable(ctx context.Context, bucket string) bool {
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		return false
	}
	dest := cfg.GetDestination()
	return dest.Bucket == bucket
}

func proxyHeadToRepTarget(ctx context.Context, bucket, object string, opts ObjectOptions) (tgt *TargetClient, oi ObjectInfo, proxy bool, err error) {
	// this option is set when active-active replication is in place between site A -> B,
	// and site B does not have the object yet.
	if opts.ProxyRequest || (opts.ProxyHeaderSet && !opts.ProxyRequest) { // true only when site B sets MinIOSourceProxyRequest header
		return nil, oi, false, nil
	}
	cfg, err := getReplicationConfig(ctx, bucket)
	if err != nil {
		return nil, oi, false, err
	}
	dest := cfg.GetDestination()
	if dest.Bucket != bucket { // not active-active
		return nil, oi, false, err
	}
	ssec := false
	if opts.ServerSideEncryption != nil {
		ssec = opts.ServerSideEncryption.Type() == encrypt.SSEC
	}
	ropts := replication.ObjectOpts{
		Name: object,
		SSEC: ssec,
	}
	if !cfg.Replicate(ropts) { // no matching rule for object prefix
		return nil, oi, false, nil
	}
	globals := mustGlobalsFromContext(ctx)
	tgt = globals.BucketTargetSys.GetRemoteTargetClient(ctx, cfg.RoleArn)
	if tgt == nil || tgt.isOffline() {
		return nil, oi, false, fmt.Errorf("target is offline or not configured")
	}

	gopts := miniogo.GetObjectOptions{
		VersionID:            opts.VersionID,
		ServerSideEncryption: opts.ServerSideEncryption,
		Internal: miniogo.AdvancedGetOptions{
			ReplicationProxyRequest: "true",
		},
	}

	objInfo, err := tgt.StatObject(ctx, dest.Bucket, object, gopts)
	if err != nil {
		return nil, oi, false, err
	}

	tags, _ := tags.MapToObjectTags(objInfo.UserTags)
	oi = ObjectInfo{
		Bucket:            bucket,
		Name:              object,
		ModTime:           objInfo.LastModified,
		Size:              objInfo.Size,
		ETag:              objInfo.ETag,
		VersionID:         objInfo.VersionID,
		IsLatest:          objInfo.IsLatest,
		DeleteMarker:      objInfo.IsDeleteMarker,
		ContentType:       objInfo.ContentType,
		Expires:           objInfo.Expires,
		StorageClass:      objInfo.StorageClass,
		ReplicationStatus: replication.StatusType(objInfo.ReplicationStatus),
		UserTags:          tags.String(),
	}
	oi.UserDefined = make(map[string]string, len(objInfo.Metadata))
	for k, v := range objInfo.Metadata {
		oi.UserDefined[k] = v[0]
	}
	ce, ok := oi.UserDefined[xhttp.ContentEncoding]
	if !ok {
		ce, ok = oi.UserDefined[strings.ToLower(xhttp.ContentEncoding)]
	}
	if ok {
		oi.ContentEncoding = ce
	}
	return tgt, oi, true, nil
}

// get object info from replication target if active-active replication is in place and
// this node returns a 404
func proxyHeadToReplicationTarget(ctx context.Context, bucket, object string, opts ObjectOptions) (oi ObjectInfo, proxy bool, err error) {
	_, oi, proxy, err = proxyHeadToRepTarget(ctx, bucket, object, opts)
	return oi, proxy, err
}
