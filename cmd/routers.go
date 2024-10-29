/*
 * MinIO Cloud Storage, (C) 2015, 2016 MinIO, Inc.
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
	"github.com/gorilla/mux"
)

// List of some generic handlers which are applied for all incoming requests.
var globalHandlers = []mux.MiddlewareFunc{
	// filters HTTP headers which are treated as metadata and are reserved
	// for internal use only.
	filterReservedMetadata,
	// Enforce rules specific for TLS requests
	setSSETLSHandler,
	// Auth handler verifies incoming authorization headers and
	// routes them accordingly. Client receives a HTTP error for
	// invalid/unsupported signatures.
	setAuthHandler,
	// Validates all incoming requests to have a valid date header.
	setTimeValidityHandler,
	// Adds cache control for all browser requests.
	setBrowserCacheControlHandler,
	// Validates if incoming request is for restricted buckets.
	setReservedBucketHandler,
	// Redirect some pre-defined browser request paths to a static location prefix.
	setBrowserRedirectHandler,
	// Adds 'crossdomain.xml' policy handler to serve legacy flash clients.
	setCrossDomainPolicy,
	// Limits all header sizes to a maximum fixed limit
	setRequestHeaderSizeLimitHandler,
	// Limits all requests size to a maximum fixed limit
	setRequestSizeLimitHandler,
	// Network statistics
	setHTTPStatsHandler,
	// Validate all the incoming requests.
	setRequestValidityHandler,
	// set HTTP security headers such as Content-Security-Policy.
	addSecurityHeaders,
	// set x-amz-request-id header.
	addCustomHeaders,
	// add redirect handler to redirect
	// requests when object layer is not
	// initialized.
	setRedirectHandler,
	// Add new handlers here.
}
