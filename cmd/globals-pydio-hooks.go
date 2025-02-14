/*
 * Copyright (c) 2019-2021. Abstrium SAS <team (at) pydio.com>
 * This file is part of Pydio Cells.
 *
 * Pydio Cells is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Pydio Cells is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Pydio Cells.  If not, see <http://www.gnu.org/licenses/>.
 *
 * The latest code can be found at <https://pydio.com>.
 */

package cmd

import (
	"context"
	"net/http"
	"net/url"

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/pkg/disk"
)

const (
	ErrPydioQuotaExceeded = APIErrorCode(1422)
	ErrTokenTimeMismatch  = APIErrorCode(1423)
)

type PydioQuotaExceeded GenericError

func (e PydioQuotaExceeded) Error() string {
	return "Quota exceeded for bucket: " + e.Bucket
}

func init() {
	errorCodes[ErrPydioQuotaExceeded] = APIError{Code: "QuotaExceeded", Description: "You have reached your authorized quota", HTTPStatusCode: http.StatusUnprocessableEntity}
	errorCodes[ErrTokenTimeMismatch] = APIError{Code: "TokenTimeMismatch", Description: "Token is invalid, the signature time seems to be mismatching. Check your proxy for reducing request buffering.", HTTPStatusCode: http.StatusGatewayTimeout}
}

type ReqParamExtractor func(req *http.Request, m map[string]string)

var (
	pydioReqParamHooks []ReqParamExtractor
)

func applyHooksExtractReqParams(req *http.Request, m map[string]string) {
	g := mustGlobalsFromContext(req.Context())
	for _, f := range g.ReqParamExtractors {
		f(req, m)
	}
	for _, f := range pydioReqParamHooks {
		f(req, m)
	}
}

// ExposedValidateRequestSignature validates request signature (v2, v4, signed or presigned) against passed globals (in context)
func ExposedValidateRequestSignature(r *http.Request) APIErrorCode {
	switch getRequestAuthType(r) {
	case authTypeUnknown:
		return ErrSignatureVersionNotSupported
	case authTypePresignedV2, authTypeSignedV2:
		return isReqAuthenticatedV2(r)
	case authTypeSigned, authTypePresigned, authTypeStreamingSigned:
		globals := mustGlobalsFromContext(r.Context())
		region := globals.ServerRegion
		return isReqAuthenticated(r.Context(), r, region, serviceS3)
	default:
		return ErrSignatureVersionNotSupported
	}
}

// ExposedUpdateContextWithCredentials updates Globals.ActiveCred inside the context (by creating a copy)
func ExposedUpdateContextWithCredentials(ctx context.Context, apiKey, apiSecret, region string) context.Context {
	globals := mustGlobalsFromContext(ctx)
	gl := *globals
	if apiKey != "" {
		gl.ActiveCred.AccessKey = apiKey
	}
	if apiSecret != "" {
		gl.ActiveCred.SecretKey = apiSecret
	}
	if region != "" {
		gl.ServerRegion = region
	}
	return passGlobalsInContext(ctx, &gl)
}

// ExposedExtractKeyFromSignature finds signature type and extract the Api Key (without further validation)
func ExposedExtractKeyFromSignature(r *http.Request) (string, APIErrorCode) {
	globals := mustGlobalsFromContext(r.Context())
	region := globals.ServerRegion

	switch getRequestAuthType(r) {
	case authTypeUnknown:
		return "", ErrSignatureVersionNotSupported
	case authTypePresignedV2, authTypeSignedV2:
		if accessKey := r.URL.Query().Get(xhttp.AmzAccessKeyID); accessKey != "" {
			return accessKey, ErrNone
		} else {
			return "", ErrSignatureDoesNotMatch
		}
	case authTypeSigned, authTypeStreamingSigned:
		if sv, er := parseSignV4(r.Header.Get("Authorization"), region, serviceS3); er != ErrNone {
			return "", er
		} else {
			return sv.Credential.accessKey, ErrNone
		}
	case authTypePresigned:
		_ = r.ParseForm()
		if sv, er := parsePreSignV4(r.Form, region, serviceS3); er != ErrNone {
			return "", er
		} else {
			return sv.Credential.accessKey, ErrNone
		}
	default:
		return "", ErrSignatureVersionNotSupported
	}

}

// ExposedWriteErrorResponse writes an error code in proper XML foramt
func ExposedWriteErrorResponse(ctx context.Context, w http.ResponseWriter, code APIErrorCode, reqURL *url.URL) {
	writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ctx, code), reqURL, false)
}

// ExposedDiskStats returns info about the disk
func ExposedDiskStats(ctx context.Context, fsPath string, health bool) (map[string]interface{}, error) {
	stats := map[string]interface{}{}
	i, e := disk.GetInfo(fsPath)
	if e != nil {
		return stats, e
	}
	stats["Total"] = i.Total
	stats["Free"] = i.Free
	stats["Used"] = i.Used
	stats["FSType"] = i.FSType

	if health {
		lat, tp, e := disk.GetHealthInfo(ctx, "", fsPath)
		if e != nil {
			return stats, e
		}
		stats["Health.Latency"] = lat
		stats["Health.Throughput"] = tp
	}
	return stats, nil
}
