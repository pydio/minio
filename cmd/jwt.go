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

import (
	"errors"
	"net/http"
	"time"

	jwtreq "github.com/golang-jwt/jwt/v4/request"

	xjwt "github.com/minio/minio/cmd/jwt"
)

const (
	jwtAlgorithm = "Bearer"

	// Inter-node JWT token expiry is 15 minutes.
	defaultInterNodeJWTExpiry = 15 * time.Minute
)

var (
	errInvalidAccessKeyID = errors.New("The access key ID you provided does not exist in our records")
	errAuthentication     = errors.New("Authentication failed, check your access credentials")
	errNoAuthToken        = errors.New("JWT token missing")
)

type jwtValidator struct {
	*Globals
}

// Callback function used for parsing
func (gl *jwtValidator) webTokenCallback(claims *xjwt.MapClaims) ([]byte, error) {
	if claims.AccessKey == gl.ActiveCred.AccessKey {
		return []byte(gl.ActiveCred.SecretKey), nil
	}
	ok, _, err := gl.IAMSys.IsTempUser(claims.AccessKey)
	if err != nil {
		if err == errNoSuchUser {
			return nil, errInvalidAccessKeyID
		}
		return nil, err
	}
	if ok {
		return []byte(gl.ActiveCred.SecretKey), nil
	}
	cred, ok := gl.IAMSys.GetUser(claims.AccessKey)
	if !ok {
		return nil, errInvalidAccessKeyID
	}
	return []byte(cred.SecretKey), nil

}

// Check if the request is authenticated.
// Returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func webRequestAuthenticate(req *http.Request) (*xjwt.MapClaims, bool, error) {
	token, err := jwtreq.AuthorizationHeaderExtractor.ExtractToken(req)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return nil, false, errNoAuthToken
		}
		return nil, false, err
	}
	globals := mustGlobalsFromContext(req.Context())
	claims := xjwt.NewMapClaims()
	verifier := &jwtValidator{Globals: globals}
	if err := xjwt.ParseWithClaims(token, claims, verifier.webTokenCallback); err != nil {
		return claims, false, errAuthentication
	}
	owner := claims.AccessKey == globals.ActiveCred.AccessKey
	return claims, owner, nil
}
