/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"crypto/tls"
	"net/http"
	"net/url"
	"runtime"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
)

// getLocalServerProperty - returns madmin.ServerProperties for only the
// local endpoints from given list of endpoints
func getLocalServerProperty(endpointServerPools EndpointServerPools, r *http.Request) madmin.ServerProperties {
	var localEndpoints Endpoints
	addr := r.Host
	network := make(map[string]string)
	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			nodeName := endpoint.Host
			if nodeName == "" {
				nodeName = r.Host
			}
			if endpoint.IsLocal {
				// Only proceed for local endpoints
				network[nodeName] = string(madmin.ItemOnline)
				localEndpoints = append(localEndpoints, endpoint)
				continue
			}
			_, present := network[nodeName]
			if !present {
				if err := isServerResolvable(endpoint, 2*time.Second); err == nil {
					network[nodeName] = string(madmin.ItemOnline)
				} else {
					network[nodeName] = string(madmin.ItemOffline)
					// log once the error
					logger.LogOnceIf(context.Background(), err, nodeName)
				}
			}
		}
	}

	props := madmin.ServerProperties{
		State:    string(madmin.ItemInitializing),
		Endpoint: addr,
		Uptime:   UTCNow().Unix() - globalBootTime.Unix(),
		Version:  Version,
		CommitID: CommitID,
		Network:  network,
	}
	runtime.ReadMemStats(&props.MemStats)

	objLayer := newObjectLayerFn()
	if objLayer != nil && !globalIsGateway {
		// only need Disks information in server mode.
		storageInfo, _ := objLayer.LocalStorageInfo(GlobalContext)
		props.State = string(madmin.ItemOnline)
		props.Disks = storageInfo.Disks
	}

	return props
}

// isServerResolvable - checks if the endpoint is resolvable
// by sending a naked HTTP request with liveness checks.
func isServerResolvable(endpoint Endpoint, timeout time.Duration) error {
	serverURL := &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
		Path:   pathJoin(healthCheckPathPrefix, healthCheckLivenessPath),
	}

	var tlsConfig *tls.Config
	if globalIsTLS {
		tlsConfig = &tls.Config{
			RootCAs: globalRootCAs,
		}
	}

	httpClient := &http.Client{
		Transport:
		// For more details about various values used here refer
		// https://golang.org/pkg/net/http/#Transport documentation
		&http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           xhttp.NewCustomDialContext(3 * time.Second),
			ResponseHeaderTimeout: 3 * time.Second,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 3 * time.Second,
			TLSClientConfig:       tlsConfig,
			// Go net/http automatically unzip if content-type is
			// gzip disable this feature, as we are always interested
			// in raw stream.
			DisableCompression: true,
		},
	}
	defer httpClient.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(GlobalContext, timeout)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL.String(), nil)
	if err != nil {
		cancel()
		return err
	}

	resp, err := httpClient.Do(req)
	cancel()
	if err != nil {
		return err
	}
	xhttp.DrainBody(resp.Body)

	return nil
}
