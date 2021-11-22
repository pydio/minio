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
	"fmt"
	"github.com/gorilla/mux"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/disk"
	"net/http"
	"net/url"
)

type ReqParamExtractor func(req *http.Request, m map[string]string)

var (
	pydioReqParamHooks []ReqParamExtractor
)

// HookRegisterGlobalHandler provides an access point to enrich globalHandlers
func HookRegisterGlobalHandler(handlerFunc mux.MiddlewareFunc) {
	globalHandlers = append(globalHandlers, handlerFunc)
}

// HookExtractReqParams registers a request parameters extractor, used in the event system
func HookExtractReqParams(extractor ReqParamExtractor) {
	pydioReqParamHooks = append(pydioReqParamHooks, extractor)
}

func applyHooksExtractReqParams(req *http.Request, m map[string]string) {
	for _, f := range pydioReqParamHooks {
		f(req, m)
	}
}

// ExposedParseSignV4 parses a v4 signature and return the signature accessKey if it's valid.
func ExposedParseSignV4(v4auth string) (string, error) {
	val, code := parseSignV4(v4auth, globalServerRegion, "s3")
	if code != ErrNone {
		return "", fmt.Errorf("cannot parse signature - code is %d", code)
	} else {
		return val.Credential.accessKey, nil
	}
}

// ExposedWriteErrorResponse writes an error code in proper XML foramt
func ExposedWriteErrorResponse(ctx context.Context, w http.ResponseWriter, code APIErrorCode, reqURL *url.URL) {
	writeErrorResponse(ctx, w, errorCodes.ToAPIErr(code), reqURL)
}

// ExposedStoreTmpUserToIAMSys stores an idToken as valid accessKey in the globalIAMSys
func ExposedStoreTmpUserToIAMSys(idToken string) error {
	return globalIAMSys.SetTempUser(idToken, auth.Credentials{}, "")
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
