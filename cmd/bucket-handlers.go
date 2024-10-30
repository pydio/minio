/*
 * MinIO Cloud Storage, (C) 2015-2020 MinIO, Inc.
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
	"bytes"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/handlers"
	"github.com/minio/minio/pkg/hash"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

const (
	objectLockConfig        = "object-lock.xml"
	bucketTaggingConfig     = "tagging.xml"
	bucketReplicationConfig = "replication.xml"
)

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api objectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketLocation")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketLocationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	getBucketInfo := objectAPI.GetBucketInfo

	if _, err := getBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Generate response.
	encodedSuccessResponse := encodeResponse(LocationResponse{})
	// Get current region.
	region := api.ServerRegion
	if region != globalMinioDefaultRegion {
		encodedSuccessResponse = encodeResponse(LocationResponse{
			Location: region,
		})
	}

	// Write success response.
	writeSuccessResponseXML(ctx, w, encodedSuccessResponse)
}

// ListMultipartUploadsHandler - GET Bucket (List Multipart uploads)
// -------------------------
// This operation lists in-progress multipart uploads. An in-progress
// multipart upload is a multipart upload that has been initiated,
// using the Initiate Multipart Upload request, but has not yet been
// completed or aborted. This operation returns at most 1,000 multipart
// uploads in the response.
func (api objectAPIHandlers) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListMultipartUploads")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketMultipartUploadsAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, encodingType, errCode := getBucketMultipartResources(r.URL.Query())
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, errCode), r.URL, guessIsBrowserReq(r))
		return
	}

	if maxUploads < 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrInvalidMaxUploads), r.URL, guessIsBrowserReq(r))
		return
	}

	if keyMarker != "" {
		// Marker not common with prefix is not implemented.
		if !HasPrefix(keyMarker, prefix) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrNotImplemented), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	listMultipartsInfo, err := objectAPI.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	// generate response
	response := generateListMultipartUploadsResponse(bucket, listMultipartsInfo, encodingType)
	encodedSuccessResponse := encodeResponse(response)

	// write success response.
	writeSuccessResponseXML(ctx, w, encodedSuccessResponse)
}

// ListBucketsHandler - GET Service.
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api objectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListBuckets")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	listBuckets := objectAPI.ListBuckets

	cred, owner, s3Error := checkRequestAuthTypeCredential(ctx, r, policy.ListAllMyBucketsAction, "", "")
	if s3Error != ErrNone && s3Error != ErrAccessDenied {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// If etcd, dns federation configured list buckets from etcd.
	var bucketsInfo []BucketInfo
	// Invoke the list buckets.
	var err error
	bucketsInfo, err = listBuckets(ctx)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error == ErrAccessDenied {
		// Set prefix value for "s3:prefix" policy conditionals.
		r.Header.Set("prefix", "")

		// Set delimiter value for "s3:delimiter" policy conditionals.
		r.Header.Set("delimiter", SlashSeparator)

		// err will be nil here as we already called this function
		// earlier in this request.
		claims, _ := getClaimsFromToken(api.ActiveCred, getSessionToken(r))
		n := 0
		// Use the following trick to filter in place
		// https://github.com/golang/go/wiki/SliceTricks#filter-in-place
		for _, bucketInfo := range bucketsInfo {
			if api.IAMSys.IsAllowed(iampolicy.Args{
				AccountName:     cred.AccessKey,
				Groups:          cred.Groups,
				Action:          iampolicy.ListBucketAction,
				BucketName:      bucketInfo.Name,
				ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
				IsOwner:         owner,
				ObjectName:      "",
				Claims:          claims,
			}) {
				bucketsInfo[n] = bucketInfo
				n++
			}
		}
		bucketsInfo = bucketsInfo[:n]
		// No buckets can be filtered return access denied error.
		if len(bucketsInfo) == 0 {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Generate response.
	response := generateListBucketsResponse(bucketsInfo)
	encodedSuccessResponse := encodeResponse(response)

	// Write response.
	writeSuccessResponseXML(ctx, w, encodedSuccessResponse)
}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api objectAPIHandlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteMultipleObjects")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Content-Md5 is requied should be set
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if _, ok := r.Header[xhttp.ContentMD5]; !ok {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMissingContentMD5), r.URL, guessIsBrowserReq(r))
		return
	}

	// Content-Length is required and should be non-zero
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if r.ContentLength <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMissingContentLength), r.URL, guessIsBrowserReq(r))
		return
	}

	// The max. XML contains 100000 object names (each at most 1024 bytes long) + XML overhead
	const maxBodySize = 2 * 100000 * 1024

	// Unmarshal list of keys to be deleted.
	deleteObjects := &DeleteObjectsRequest{}
	if err := xmlDecoder(r.Body, deleteObjects, maxBodySize); err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Convert object name delete objects if it has `/` in the beginning.
	for i := range deleteObjects.Objects {
		deleteObjects.Objects[i].ObjectName = trimLeadingSlash(deleteObjects.Objects[i].ObjectName)
	}

	// Call checkRequestAuthType to populate ReqInfo.AccessKey before GetBucketInfo()
	// Ignore errors here to preserve the S3 error behavior of GetBucketInfo()
	checkRequestAuthType(ctx, r, policy.DeleteObjectAction, bucket, "")

	// Before proceeding validate if bucket exists.
	_, err := objectAPI.GetBucketInfo(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	deleteObjectsFn := objectAPI.DeleteObjects

	// Return Malformed XML as S3 spec if the list of objects is empty
	if len(deleteObjects.Objects) == 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMalformedXML), r.URL, guessIsBrowserReq(r))
		return
	}

	var objectsToDelete = map[ObjectToDelete]int{}
	getObjectInfoFn := objectAPI.GetObjectInfo
	var (
		hasLockEnabled bool
		goi            ObjectInfo
		gerr           error
	)
	if rcfg, _ := api.BucketObjectLockSys.Get(bucket); rcfg.LockEnabled {
		hasLockEnabled = true
	}

	dErrs := make([]DeleteError, len(deleteObjects.Objects))
	for index, object := range deleteObjects.Objects {
		if apiErrCode := checkRequestAuthType(ctx, r, policy.DeleteObjectAction, bucket, object.ObjectName); apiErrCode != ErrNone {
			if apiErrCode == ErrSignatureDoesNotMatch || apiErrCode == ErrInvalidAccessKeyID {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, apiErrCode), r.URL, guessIsBrowserReq(r))
				return
			}
			apiErr := errorCodes.ToAPIErr(ctx, apiErrCode)
			dErrs[index] = DeleteError{
				Code:      apiErr.Code,
				Message:   apiErr.Description,
				Key:       object.ObjectName,
				VersionID: object.VersionID,
			}
			continue
		}
		if object.VersionID != "" && object.VersionID != nullVersionID {
			if _, err := uuid.Parse(object.VersionID); err != nil {
				logger.LogIf(ctx, fmt.Errorf("invalid version-id specified %w", err))
				apiErr := errorCodes.ToAPIErr(ctx, ErrNoSuchVersion)
				dErrs[index] = DeleteError{
					Code:      apiErr.Code,
					Message:   apiErr.Description,
					Key:       object.ObjectName,
					VersionID: object.VersionID,
				}
				continue
			}
		}

		if hasLockEnabled {
			goi, gerr = getObjectInfoFn(ctx, bucket, object.ObjectName, ObjectOptions{
				VersionID: object.VersionID,
			})
		}
		if object.VersionID != "" {
			if hasLockEnabled {
				if apiErrCode := enforceRetentionBypassForDelete(ctx, r, bucket, object, goi, gerr); apiErrCode != ErrNone {
					apiErr := errorCodes.ToAPIErr(ctx, apiErrCode)
					dErrs[index] = DeleteError{
						Code:      apiErr.Code,
						Message:   apiErr.Description,
						Key:       object.ObjectName,
						VersionID: object.VersionID,
					}
					continue
				}
			}
		}

		// Avoid duplicate objects, we use map to filter them out.
		if _, ok := objectsToDelete[object]; !ok {
			objectsToDelete[object] = index
		}
	}

	toNames := func(input map[ObjectToDelete]int) (output []ObjectToDelete) {
		output = make([]ObjectToDelete, len(input))
		idx := 0
		for obj := range input {
			output[idx] = obj
			idx++
		}
		return
	}

	deleteList := toNames(objectsToDelete)
	dObjects, errs := deleteObjectsFn(ctx, bucket, deleteList, ObjectOptions{
		Versioned:        api.BucketVersioningSys.Enabled(bucket),
		VersionSuspended: api.BucketVersioningSys.Suspended(bucket),
	})
	deletedObjects := make([]DeletedObject, len(deleteObjects.Objects))
	for i := range errs {
		// DeleteMarkerVersionID is not used specifically to avoid
		// lookup errors, since DeleteMarkerVersionID is only
		// created during DeleteMarker creation when client didn't
		// specify a versionID.
		objToDel := ObjectToDelete{
			ObjectName:                    dObjects[i].ObjectName,
			VersionID:                     dObjects[i].VersionID,
			VersionPurgeStatus:            dObjects[i].VersionPurgeStatus,
			DeleteMarkerReplicationStatus: dObjects[i].DeleteMarkerReplicationStatus,
			PurgeTransitioned:             dObjects[i].PurgeTransitioned,
		}
		dindex := objectsToDelete[objToDel]
		if errs[i] == nil || isErrObjectNotFound(errs[i]) || isErrVersionNotFound(errs[i]) {
			deletedObjects[dindex] = dObjects[i]
			continue
		}
		apiErr := toAPIError(ctx, errs[i])
		dErrs[dindex] = DeleteError{
			Code:      apiErr.Code,
			Message:   apiErr.Description,
			Key:       deleteList[i].ObjectName,
			VersionID: deleteList[i].VersionID,
		}
	}

	var deleteErrors []DeleteError
	for _, dErr := range dErrs {
		if dErr.Code != "" {
			deleteErrors = append(deleteErrors, dErr)
		}
	}

	// Generate response
	response := generateMultiDeleteResponse(deleteObjects.Quiet, deletedObjects, deleteErrors)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(ctx, w, encodedSuccessResponse)
	for _, dobj := range deletedObjects {
		if dobj.ObjectName == "" {
			continue
		}

		eventName := event.ObjectRemovedDelete
		objInfo := ObjectInfo{
			Name:         dobj.ObjectName,
			VersionID:    dobj.VersionID,
			DeleteMarker: dobj.DeleteMarker,
		}

		if objInfo.DeleteMarker {
			objInfo.VersionID = dobj.DeleteMarkerVersionID
			eventName = event.ObjectRemovedDeleteMarkerCreated
		}

		sendEvent(api.Globals, eventArgs{
			EventName:    eventName,
			BucketName:   bucket,
			Object:       objInfo,
			ReqParams:    extractReqParams(r),
			RespElements: extractRespElements(w),
			UserAgent:    r.UserAgent(),
			Host:         handlers.GetSourceIP(r),
		})
	}
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api objectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucket")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectLockEnabled := false
	if vs, found := r.Header[http.CanonicalHeaderKey("x-amz-bucket-object-lock-enabled")]; found {
		v := strings.ToLower(strings.Join(vs, ""))
		if v != "true" && v != "false" {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrInvalidRequest), r.URL, guessIsBrowserReq(r))
			return
		}
		objectLockEnabled = v == "true"
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.CreateBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Parse incoming location constraint.
	location, s3Error := parseLocationConstraint(r)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate if location sent by the client is valid, reject
	// requests which do not follow valid region requirements.
	if !isValidLocation(api.ServerRegion, location) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrInvalidRegion), r.URL, guessIsBrowserReq(r))
		return
	}

	opts := BucketOptions{
		Location:    location,
		LockEnabled: objectLockEnabled,
	}

	// Proceed to creating a bucket.
	err := objectAPI.MakeBucketWithLocation(ctx, bucket, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Load updated bucket metadata into memory.
	api.NotificationSys.LoadBucketMetadata(GlobalContext, bucket)

	// Make sure to add Location information here only for bucket
	if cp := pathClean(r.URL.Path); cp != "" {
		w.Header().Set(xhttp.Location, cp) // Clean any trailing slashes.
	}

	writeSuccessResponseHeadersOnly(ctx, w)

	sendEvent(api.Globals, eventArgs{
		EventName:    event.BucketCreated,
		BucketName:   bucket,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// PostPolicyBucketHandler - POST policy
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data
func (api objectAPIHandlers) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PostPolicyBucket")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if crypto.S3KMS.IsRequested(r.Header) { // SSE-KMS is not supported
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}

	if _, ok := crypto.IsRequested(r.Header); !objectAPI.IsEncryptionSupported() && ok {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}

	bucket := mux.Vars(r)["bucket"]

	// Require Content-Length to be set in the request
	size := r.ContentLength
	if size < 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMissingContentLength), r.URL, guessIsBrowserReq(r))
		return
	}

	resource, err := getResource(r.URL.Path, r.Host, api.DomainNames)
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrInvalidRequest), r.URL, guessIsBrowserReq(r))
		return
	}

	// Make sure that the URL does not contain object name.
	if bucket != path.Clean(resource[1:]) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMethodNotAllowed), r.URL, guessIsBrowserReq(r))
		return
	}

	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	reader, err := r.MultipartReader()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
		return
	}

	// Read multipart data and save in memory and in the disk if needed
	form, err := reader.ReadForm(maxFormMemory)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
		return
	}

	// Remove all tmp files created during multipart upload
	defer form.RemoveAll()

	// Extract all form fields
	fileBody, fileName, fileSize, formValues, err := extractPostPolicyFormValues(ctx, form)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if file is provided, error out otherwise.
	if fileBody == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrPOSTFileRequired), r.URL, guessIsBrowserReq(r))
		return
	}

	// Close multipart file
	defer fileBody.Close()

	formValues.Set("Bucket", bucket)
	if fileName != "" && strings.Contains(formValues.Get("Key"), "${filename}") {
		// S3 feature to replace ${filename} found in Key form field
		// by the filename attribute passed in multipart
		formValues.Set("Key", strings.Replace(formValues.Get("Key"), "${filename}", fileName, -1))
	}
	object := trimLeadingSlash(formValues.Get("Key"))

	successRedirect := formValues.Get("success_action_redirect")
	successStatus := formValues.Get("success_action_status")
	var redirectURL *url.URL
	if successRedirect != "" {
		redirectURL, err = url.Parse(successRedirect)
		if err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Verify policy signature.
	cred, errCode := doesPolicySignatureMatch(ctx, formValues)
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, errCode), r.URL, guessIsBrowserReq(r))
		return
	}

	// Once signature is validated, check if the user has
	// explicit permissions for the user.
	{
		token := formValues.Get(xhttp.AmzSecurityToken)
		if token != "" && cred.AccessKey == "" {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrNoAccessKey), r.URL, guessIsBrowserReq(r))
			return
		}

		if cred.IsServiceAccount() && token == "" {
			token = cred.SessionToken
		}

		if subtle.ConstantTimeCompare([]byte(token), []byte(cred.SessionToken)) != 1 {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrInvalidToken), r.URL, guessIsBrowserReq(r))
			return
		}

		// Extract claims if any.
		claims, err := getClaimsFromToken(api.ActiveCred, getSessionToken(r))
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		if !api.IAMSys.IsAllowed(iampolicy.Args{
			AccountName:     cred.AccessKey,
			Action:          iampolicy.PutObjectAction,
			ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
			BucketName:      bucket,
			ObjectName:      object,
			IsOwner:         api.ActiveCred.AccessKey == cred.AccessKey,
			Claims:          claims,
		}) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrAccessDenied), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	policyBytes, err := base64.StdEncoding.DecodeString(formValues.Get("Policy"))
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
		return
	}

	// Handle policy if it is set.
	if len(policyBytes) > 0 {
		postPolicyForm, err := parsePostPolicyForm(bytes.NewReader(policyBytes))
		if err != nil {
			errAPI := errorCodes.ToAPIErr(ctx, ErrPostPolicyConditionInvalidFormat)
			errAPI.Description = fmt.Sprintf("%s '(%s)'", errAPI.Description, err)
			writeErrorResponse(ctx, w, errAPI, r.URL, guessIsBrowserReq(r))
			return
		}

		// Make sure formValues adhere to policy restrictions.
		if err = checkPostPolicy(formValues, postPolicyForm); err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErrWithErr(ctx, ErrAccessDenied, err), r.URL, guessIsBrowserReq(r))
			return
		}

		// Ensure that the object size is within expected range, also the file size
		// should not exceed the maximum single Put size (5 GiB)
		lengthRange := postPolicyForm.Conditions.ContentLengthRange
		if lengthRange.Valid {
			if fileSize < lengthRange.Min {
				writeErrorResponse(ctx, w, toAPIError(ctx, errDataTooSmall), r.URL, guessIsBrowserReq(r))
				return
			}

			if fileSize > lengthRange.Max || isMaxObjectSize(fileSize) {
				writeErrorResponse(ctx, w, toAPIError(ctx, errDataTooLarge), r.URL, guessIsBrowserReq(r))
				return
			}
		}
	}

	// Extract metadata to be saved from received Form.
	metadata := make(map[string]string)
	err = extractMetadataFromMime(ctx, textproto.MIMEHeader(formValues), metadata)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	hashReader, err := hash.NewReader(fileBody, fileSize, "", "", fileSize)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	rawReader := hashReader
	pReader := NewPutObjReader(rawReader)
	var objectEncryptionKey crypto.ObjectKey

	// Check if bucket encryption is enabled
	if _, err = api.BucketSSEConfigSys.Get(bucket); err == nil {
		// This request header needs to be set prior to setting ObjectOptions
		if !crypto.SSEC.IsRequested(r.Header) {
			r.Header.Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionAES)
		}
	}

	// get gateway encryption options
	var opts ObjectOptions
	opts, err = api.putOpts(ctx, r, bucket, object, metadata)
	if err != nil {
		writeErrorResponseHeadersOnly(ctx, w, toAPIError(ctx, err))
		return
	}
	if objectAPI.IsEncryptionSupported() {
		if _, ok := crypto.IsRequested(formValues); ok && !HasSuffix(object, SlashSeparator) { // handle SSE requests
			if crypto.SSECopy.IsRequested(r.Header) {
				writeErrorResponse(ctx, w, toAPIError(ctx, errInvalidEncryptionParameters), r.URL, guessIsBrowserReq(r))
				return
			}
			var reader io.Reader
			var key []byte
			if crypto.SSEC.IsRequested(formValues) {
				key, err = ParseSSECustomerHeader(formValues)
				if err != nil {
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
			}
			reader, objectEncryptionKey, err = newEncryptReader(hashReader, key, bucket, object, metadata, crypto.S3.IsRequested(formValues))
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			info := ObjectInfo{Size: fileSize}
			// do not try to verify encrypted content
			hashReader, err = hash.NewReader(reader, info.EncryptedSize(), "", "", fileSize)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			pReader, err = pReader.WithEncryption(hashReader, &objectEncryptionKey)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
		}
	}

	objInfo, err := objectAPI.PutObject(ctx, bucket, object, pReader, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// We must not use the http.Header().Set method here because some (broken)
	// clients expect the ETag header key to be literally "ETag" - not "Etag" (case-sensitive).
	// Therefore, we have to set the ETag directly as map entry.
	w.Header()[xhttp.ETag] = []string{`"` + objInfo.ETag + `"`}

	// Set the relevant version ID as part of the response header.
	if objInfo.VersionID != "" {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	w.Header().Set(xhttp.Location, getObjectLocation(r, api.DomainNames, bucket, object))

	// Notify object created event.
	defer sendEvent(api.Globals, eventArgs{
		EventName:    event.ObjectCreatedPost,
		BucketName:   objInfo.Bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})

	if successRedirect != "" {
		// Replace raw query params..
		redirectURL.RawQuery = getRedirectPostRawQuery(objInfo)
		writeRedirectSeeOther(ctx, w, redirectURL.String())
		return
	}

	// Decide what http response to send depending on success_action_status parameter
	switch successStatus {
	case "201":
		resp := encodeResponse(PostResponse{
			Bucket:   objInfo.Bucket,
			Key:      objInfo.Name,
			ETag:     `"` + objInfo.ETag + `"`,
			Location: w.Header().Get(xhttp.Location),
		})
		writeResponse(ctx, w, http.StatusCreated, resp, mimeXML)
	case "200":
		writeSuccessResponseHeadersOnly(ctx, w)
	default:
		writeSuccessNoContent(ctx, w)
	}
}

// GetBucketPolicyStatusHandler -  Retrieves the policy status
// for an MinIO bucket, indicating whether the bucket is public.
func (api objectAPIHandlers) GetBucketPolicyStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketPolicyStatus")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyStatusAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponseHeadersOnly(ctx, w, errorCodes.ToAPIErr(ctx, s3Error))
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if anonymous (non-owner) has access to list objects.
	readable := api.PolicySys.IsAllowed(policy.Args{
		Action:          policy.ListBucketAction,
		BucketName:      bucket,
		ConditionValues: getConditionValues(r, "", "", nil),
		IsOwner:         false,
	})

	// Check if anonymous (non-owner) has access to upload objects.
	writable := api.PolicySys.IsAllowed(policy.Args{
		Action:          policy.PutObjectAction,
		BucketName:      bucket,
		ConditionValues: getConditionValues(r, "", "", nil),
		IsOwner:         false,
	})

	encodedSuccessResponse := encodeResponse(PolicyStatus{
		IsPublic: func() string {
			// Silly to have special 'boolean' values yes
			// but complying with silly implementation
			// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html
			if readable && writable {
				return "TRUE"
			}
			return "FALSE"
		}(),
	})

	writeSuccessResponseXML(ctx, w, encodedSuccessResponse)
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api objectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HeadBucket")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponseHeadersOnly(ctx, w, errorCodes.ToAPIErr(ctx, s3Error))
		return
	}

	getBucketInfo := objectAPI.GetBucketInfo

	if _, err := getBucketInfo(ctx, bucket); err != nil {
		writeErrorResponseHeadersOnly(ctx, w, toAPIError(ctx, err))
		return
	}

	writeSuccessResponseHeadersOnly(ctx, w)
}

// DeleteBucketHandler - Delete bucket
func (api objectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucket")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Verify if the caller has sufficient permissions.
	if s3Error := checkRequestAuthType(ctx, r, policy.DeleteBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	forceDelete := false
	if value := r.Header.Get(xhttp.MinIOForceDelete); value != "" {
		var err error
		forceDelete, err = strconv.ParseBool(value)
		if err != nil {
			apiErr := errorCodes.ToAPIErr(ctx, ErrInvalidRequest)
			apiErr.Description = err.Error()
			writeErrorResponse(ctx, w, apiErr, r.URL, guessIsBrowserReq(r))
			return
		}

		// if force delete header is set, we need to evaluate the policy anyways
		// regardless of it being true or not.
		if s3Error := checkRequestAuthType(ctx, r, policy.ForceDeleteBucketAction, bucket, ""); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
			return
		}

		if forceDelete {
			if rcfg, _ := api.BucketObjectLockSys.Get(bucket); rcfg.LockEnabled {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrMethodNotAllowed), r.URL, guessIsBrowserReq(r))
				return
			}
		}
	}

	deleteBucket := objectAPI.DeleteBucket

	// Attempt to delete bucket.
	if err := deleteBucket(ctx, bucket, forceDelete); err != nil {
		if _, ok := err.(BucketNotEmpty); ok && (api.BucketVersioningSys.Enabled(bucket) || api.BucketVersioningSys.Suspended(bucket)) {
			apiErr := toAPIError(ctx, err)
			apiErr.Description = "The bucket you tried to delete is not empty. You must delete all versions in the bucket."
			writeErrorResponse(ctx, w, apiErr, r.URL, guessIsBrowserReq(r))
		} else {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		}
		return
	}

	api.NotificationSys.DeleteBucketMetadata(ctx, bucket)

	// Write success response.
	writeSuccessNoContent(ctx, w)

	sendEvent(api.Globals, eventArgs{
		EventName:    event.BucketRemoved,
		BucketName:   bucket,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// PutBucketObjectLockConfigHandler - PUT Bucket object lock configuration.
// ----------
// Places an Object Lock configuration on the specified bucket. The rule
// specified in the Object Lock configuration will be applied by default
// to every new object placed in the specified bucket.
func (api objectAPIHandlers) PutBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketObjectLockConfig")
	writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ctx, ErrNotImplemented), r.URL)
	return
}

// GetBucketObjectLockConfigHandler - GET Bucket object lock configuration.
// ----------
// Gets the Object Lock configuration for a bucket. The rule specified in
// the Object Lock configuration will be applied by default to every new
// object placed in the specified bucket.
func (api objectAPIHandlers) GetBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketObjectLockConfig")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketObjectLockConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	config, err := api.BucketMetadataSys.GetObjectLockConfig(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseXML(ctx, w, configData)
}

// PutBucketTaggingHandler - PUT Bucket tagging.
// ----------
func (api objectAPIHandlers) PutBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketTagging")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketTaggingAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	tags, err := tags.ParseBucketXML(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ctx, ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(tags)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if err = api.BucketMetadataSys.Update(bucket, bucketTaggingConfig, configData); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(ctx, w)
}

// GetBucketTaggingHandler - GET Bucket tagging.
// ----------
func (api objectAPIHandlers) GetBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketTagging")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketTaggingAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	config, err := api.BucketMetadataSys.GetTaggingConfig(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseXML(ctx, w, configData)
}

// DeleteBucketTaggingHandler - DELETE Bucket tagging.
// ----------
func (api objectAPIHandlers) DeleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketTagging")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketTaggingAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	if err := api.BucketMetadataSys.Update(bucket, bucketTaggingConfig, nil); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(ctx, w)
}

// PutBucketReplicationConfigHandler - PUT Bucket replication configuration.
// ----------
// Add a replication configuration on the specified bucket as specified in https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketReplication.html
func (api objectAPIHandlers) PutBucketReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketReplicationConfig")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ctx, ErrNotImplemented), r.URL)
	return
}

// GetBucketReplicationConfigHandler - GET Bucket replication configuration.
// ----------
// Gets the replication configuration for a bucket.
func (api objectAPIHandlers) GetBucketReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketReplicationConfig")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetReplicationConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}
	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	config, err := api.BucketMetadataSys.GetReplicationConfig(ctx, bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseXML(ctx, w, configData)
}

// DeleteBucketReplicationConfigHandler - DELETE Bucket replication config.
// ----------
func (api objectAPIHandlers) DeleteBucketReplicationConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketReplicationConfig")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutReplicationConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}
	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	if err := api.BucketMetadataSys.Update(bucket, bucketReplicationConfig, nil); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(ctx, w)
}

// GetBucketReplicationMetricsHandler - GET Bucket replication metrics.
// ----------
// Gets the replication metrics for a bucket.
func (api objectAPIHandlers) GetBucketReplicationMetricsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketReplicationMetrics")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetReplicationConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	bucketStats := api.NotificationSys.GetClusterBucketStats(r.Context(), bucket)
	bucketReplStats := BucketReplicationStats{}
	// sum up metrics from each node in the cluster
	for _, bucketStat := range bucketStats {
		bucketReplStats.FailedCount += bucketStat.ReplicationStats.FailedCount
		bucketReplStats.FailedSize += bucketStat.ReplicationStats.FailedSize
		bucketReplStats.PendingCount += bucketStat.ReplicationStats.PendingCount
		bucketReplStats.PendingSize += bucketStat.ReplicationStats.PendingSize
		bucketReplStats.ReplicaSize += bucketStat.ReplicationStats.ReplicaSize
		bucketReplStats.ReplicatedSize += bucketStat.ReplicationStats.ReplicatedSize
	}

	if err := json.NewEncoder(w).Encode(&bucketReplStats); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	w.(http.Flusher).Flush()
}
