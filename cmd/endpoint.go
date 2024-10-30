/*
 * MinIO Cloud Storage, (C) 2017-2019 MinIO, Inc.
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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/mountinfo"
)

// EndpointType - enum for endpoint type.
type EndpointType int

const (
	// PathEndpointType - path style endpoint type enum.
	PathEndpointType EndpointType = iota + 1

	// URLEndpointType - URL style endpoint type enum.
	URLEndpointType
)

// ProxyEndpoint - endpoint used for proxy redirects
// See proxyRequest() for details.
type ProxyEndpoint struct {
	Endpoint
	Transport http.RoundTripper
}

// Endpoint - any type of endpoint.
type Endpoint struct {
	*url.URL
	IsLocal bool
}

func (endpoint Endpoint) String() string {
	if endpoint.Host == "" {
		return endpoint.Path
	}

	return endpoint.URL.String()
}

// Type - returns type of endpoint.
func (endpoint Endpoint) Type() EndpointType {
	if endpoint.Host == "" {
		return PathEndpointType
	}

	return URLEndpointType
}

// HTTPS - returns true if secure for URLEndpointType.
func (endpoint Endpoint) HTTPS() bool {
	return endpoint.Scheme == "https"
}

// UpdateIsLocal - resolves the host and updates if it is local or not.
func (endpoint *Endpoint) UpdateIsLocal(globalPort string) (err error) {
	if !endpoint.IsLocal {
		endpoint.IsLocal, err = isLocalHost(endpoint.Hostname(), endpoint.Port(), globalPort)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewEndpoint - returns new endpoint based on given arguments.
func NewEndpoint(arg string) (ep Endpoint, e error) {
	// isEmptyPath - check whether given path is not empty.
	isEmptyPath := func(path string) bool {
		return path == "" || path == SlashSeparator || path == `\`
	}

	if isEmptyPath(arg) {
		return ep, fmt.Errorf("empty or root endpoint is not supported")
	}

	var isLocal bool
	var host string
	u, err := url.Parse(arg)
	if err == nil && u.Host != "" {
		// URL style of endpoint.
		// Valid URL style endpoint is
		// - Scheme field must contain "http" or "https"
		// - All field should be empty except Host and Path.
		if !((u.Scheme == "http" || u.Scheme == "https") &&
			u.User == nil && u.Opaque == "" && !u.ForceQuery && u.RawQuery == "" && u.Fragment == "") {
			return ep, fmt.Errorf("invalid URL endpoint format")
		}

		var port string
		host, port, err = net.SplitHostPort(u.Host)
		if err != nil {
			if !strings.Contains(err.Error(), "missing port in address") {
				return ep, fmt.Errorf("invalid URL endpoint format: %w", err)
			}

			host = u.Host
		} else {
			var p int
			p, err = strconv.Atoi(port)
			if err != nil {
				return ep, fmt.Errorf("invalid URL endpoint format: invalid port number")
			} else if p < 1 || p > 65535 {
				return ep, fmt.Errorf("invalid URL endpoint format: port number must be between 1 to 65535")
			}
		}
		if i := strings.Index(host, "%"); i > -1 {
			host = host[:i]
		}

		if host == "" {
			return ep, fmt.Errorf("invalid URL endpoint format: empty host name")
		}

		// As this is path in the URL, we should use path package, not filepath package.
		// On MS Windows, filepath.Clean() converts into Windows path style ie `/foo` becomes `\foo`
		u.Path = path.Clean(u.Path)
		if isEmptyPath(u.Path) {
			return ep, fmt.Errorf("empty or root path is not supported in URL endpoint")
		}

		// On windows having a preceding SlashSeparator will cause problems, if the
		// command line already has C:/<export-folder/ in it. Final resulting
		// path on windows might become C:/C:/ this will cause problems
		// of starting minio server properly in distributed mode on windows.
		// As a special case make sure to trim the separator.

		// NOTE: It is also perfectly fine for windows users to have a path
		// without C:/ since at that point we treat it as relative path
		// and obtain the full filesystem path as well. Providing C:/
		// style is necessary to provide paths other than C:/,
		// such as F:/, D:/ etc.
		//
		// Another additional benefit here is that this style also
		// supports providing \\host\share support as well.
		if runtime.GOOS == globalWindowsOSName {
			if filepath.VolumeName(u.Path[1:]) != "" {
				u.Path = u.Path[1:]
			}
		}

	} else {
		// Only check if the arg is an ip address and ask for scheme since its absent.
		// localhost, example.com, any FQDN cannot be disambiguated from a regular file path such as
		// /mnt/export1. So we go ahead and start the minio server in FS modes in these cases.
		if isHostIP(arg) {
			return ep, fmt.Errorf("invalid URL endpoint format: missing scheme http or https")
		}
		absArg, err := filepath.Abs(arg)
		if err != nil {
			return Endpoint{}, fmt.Errorf("absolute path failed %s", err)
		}
		u = &url.URL{Path: path.Clean(absArg)}
		isLocal = true
	}

	return Endpoint{
		URL:     u,
		IsLocal: isLocal,
	}, nil
}

// PoolEndpoints represent endpoints in a given pool
// along with its setCount and setDriveCount.
type PoolEndpoints struct {
	SetCount     int
	DrivesPerSet int
	Endpoints    Endpoints
}

// EndpointServerPools - list of list of endpoints
type EndpointServerPools []PoolEndpoints

// GetLocalPoolIdx returns the pool which endpoint belongs to locally.
// if ep is remote this code will return -1 poolIndex
func (l EndpointServerPools) GetLocalPoolIdx(ep Endpoint) int {
	for i, zep := range l {
		for _, cep := range zep.Endpoints {
			if cep.IsLocal && ep.IsLocal {
				if reflect.DeepEqual(cep, ep) {
					return i
				}
			}
		}
	}
	return -1
}

// Add add pool endpoints
func (l *EndpointServerPools) Add(zeps PoolEndpoints) error {
	existSet := set.NewStringSet()
	for _, zep := range *l {
		for _, ep := range zep.Endpoints {
			existSet.Add(ep.String())
		}
	}
	// Validate if there are duplicate endpoints across serverPools
	for _, ep := range zeps.Endpoints {
		if existSet.Contains(ep.String()) {
			return fmt.Errorf("duplicate endpoints found")
		}
	}
	*l = append(*l, zeps)
	return nil
}

// Localhost - returns the local hostname from list of endpoints
func (l EndpointServerPools) Localhost() string {
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				u := &url.URL{
					Scheme: endpoint.Scheme,
					Host:   endpoint.Host,
				}
				return u.String()
			}
		}
	}
	return ""
}

// FirstLocal returns true if the first endpoint is local.
func (l EndpointServerPools) FirstLocal() bool {
	return l[0].Endpoints[0].IsLocal
}

// HTTPS - returns true if secure for URLEndpointType.
func (l EndpointServerPools) HTTPS() bool {
	return l[0].Endpoints.HTTPS()
}

// NEndpoints - returns all nodes count
func (l EndpointServerPools) NEndpoints() (count int) {
	for _, ep := range l {
		count += len(ep.Endpoints)
	}
	return count
}

// Hostnames - returns list of unique hostnames
func (l EndpointServerPools) Hostnames() []string {
	foundSet := set.NewStringSet()
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if foundSet.Contains(endpoint.Hostname()) {
				continue
			}
			foundSet.Add(endpoint.Hostname())
		}
	}
	return foundSet.ToSlice()
}

// Endpoints - list of same type of endpoint.
type Endpoints []Endpoint

// HTTPS - returns true if secure for URLEndpointType.
func (endpoints Endpoints) HTTPS() bool {
	return endpoints[0].HTTPS()
}

// GetString - returns endpoint string of i-th endpoint (0-based),
// and empty string for invalid indexes.
func (endpoints Endpoints) GetString(i int) string {
	if i < 0 || i >= len(endpoints) {
		return ""
	}
	return endpoints[i].String()
}

// GetAllStrings - returns allstring of all endpoints
func (endpoints Endpoints) GetAllStrings() (all []string) {
	for _, e := range endpoints {
		all = append(all, e.String())
	}
	return
}

func hostResolveToLocalhost(endpoint Endpoint) bool {
	hostIPs, err := getHostIP(endpoint.Hostname())
	if err != nil {
		// Log the message to console about the host resolving
		reqInfo := (&logger.ReqInfo{}).AppendTags(
			"host",
			endpoint.Hostname(),
		)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err, logger.Application)
		return false
	}
	var loopback int
	for _, hostIP := range hostIPs.ToSlice() {
		if net.ParseIP(hostIP).IsLoopback() {
			loopback++
		}
	}
	return loopback == len(hostIPs)
}

func (endpoints Endpoints) atleastOneEndpointLocal() bool {
	for _, endpoint := range endpoints {
		if endpoint.IsLocal {
			return true
		}
	}
	return false
}

// Checks if there are any cross device mounts.
func checkCrossDeviceMounts(endpoints Endpoints) (err error) {
	var absPaths []string
	for _, endpoint := range endpoints {
		if endpoint.IsLocal {
			var absPath string
			absPath, err = filepath.Abs(endpoint.Path)
			if err != nil {
				return err
			}
			absPaths = append(absPaths, absPath)
		}
	}
	return mountinfo.CheckCrossDevice(absPaths)
}

// CreateEndpoints - validates and creates new endpoints for given args.
func CreateEndpoints(serverAddr, globalPort string, foundLocal bool, args ...[]string) (Endpoints, SetupType, error) {
	var endpoints Endpoints
	var setupType SetupType
	var err error

	// Check whether serverAddr is valid for this host.
	if err = CheckLocalServerAddr(serverAddr); err != nil {
		return endpoints, setupType, err
	}

	// For single arg, return FS setup.
	if len(args) == 1 && len(args[0]) == 1 {
		var endpoint Endpoint
		endpoint, err = NewEndpoint(args[0][0])
		if err != nil {
			return endpoints, setupType, err
		}
		if err := endpoint.UpdateIsLocal(globalPort); err != nil {
			return endpoints, setupType, err
		}
		if endpoint.Type() != PathEndpointType {
			return endpoints, setupType, config.ErrInvalidFSEndpoint(nil).Msg("use path style endpoint for FS setup")
		}
		endpoints = append(endpoints, endpoint)
		setupType = FSSetupType

		// Check for cross device mounts if any.
		if err = checkCrossDeviceMounts(endpoints); err != nil {
			return endpoints, setupType, config.ErrInvalidFSEndpoint(nil).Msg(err.Error())
		}

		return endpoints, setupType, nil
	}

	return endpoints, setupType, config.ErrInvalidFSEndpoint(nil).Msg("only unique endpoint is supported")

}

// GetLocalPeer - returns local peer value, returns globalMinioAddr
// for FS and Erasure mode. In case of distributed server return
// the first element from the set of peers which indicate that
// they are local. There is always one entry that is local
// even with repeated server endpoints.
func GetLocalPeer(endpointServerPools EndpointServerPools, host, port string) (localPeer string) {
	peerSet := set.NewStringSet()
	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			if endpoint.Type() != URLEndpointType {
				continue
			}
			if endpoint.IsLocal && endpoint.Host != "" {
				peerSet.Add(endpoint.Host)
			}
		}
	}
	if peerSet.IsEmpty() {
		// Local peer can be empty in FS or Erasure coded mode.
		// If so, return globalMinioHost + globalMinioPort value.
		if host != "" {
			return net.JoinHostPort(host, port)
		}

		return net.JoinHostPort("127.0.0.1", port)
	}
	return peerSet.ToSlice()[0]
}
