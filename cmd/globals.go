/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/gorilla/mux"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/compress"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/bucket/bandwidth"
	"github.com/minio/minio/pkg/certs"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/pubsub"
)

// minio configuration related constants.
const (
	GlobalMinioDefaultPort = "9000"

	globalMinioDefaultRegion = ""
	// This is a sha256 output of ``arn:aws:iam::minio:user/admin``,
	// this is kept in present form to be compatible with S3 owner ID
	// requirements -
	//
	// ```
	//    The canonical user ID is the Amazon S3–only concept.
	//    It is 64-character obfuscated version of the account ID.
	// ```
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/example-walkthroughs-managing-access-example4.html
	globalMinioDefaultOwnerID      = "02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4"
	globalMinioDefaultStorageClass = "STANDARD"
	globalWindowsOSName            = "windows"
	globalMacOSName                = "darwin"
	globalMinioModeFS              = "mode-server-fs"
	globalMinioModeErasure         = "mode-server-xl"
	globalMinioModeDistErasure     = "mode-server-distributed-xl"
	globalMinioModeGatewayPrefix   = "mode-gateway-"
	globalDirSuffix                = "__XLDIR__"
	globalDirSuffixWithSlash       = globalDirSuffix + slashSeparator

	// Add new global values here.
)

const (
	// Limit fields size (except file) to 1Mib since Policy document
	// can reach that size according to https://aws.amazon.com/articles/1434
	maxFormFieldSize = int64(1 * humanize.MiByte)

	// Limit memory allocation to store multipart data
	maxFormMemory = int64(5 * humanize.MiByte)

	// The maximum allowed time difference between the incoming request
	// date and server date during signature verification.
	globalMaxSkewTime = 15 * time.Minute // 15 minutes skew allowed.

	// GlobalStaleUploadsExpiry - Expiry duration after which the uploads in multipart, tmp directory are deemed stale.
	GlobalStaleUploadsExpiry = time.Hour * 24 // 24 hrs.

	// GlobalStaleUploadsCleanupInterval - Cleanup interval when the stale uploads cleanup is initiated.
	GlobalStaleUploadsCleanupInterval = time.Hour * 12 // 12 hrs.

	// Refresh interval to update in-memory iam config cache.
	globalRefreshIAMInterval = 5 * time.Minute

	// Limit of location constraint XML for unauthenticated PUT bucket operations.
	maxLocationConstraintSize = 3 * humanize.MiByte
)

type CliContext struct {
	JSON, Quiet    bool
	Anonymous      bool
	Addr           string
	StrictS3Compat bool
	ConfigDir      *ConfigDir
	CertsDir       *ConfigDir
	CertsCADir     *ConfigDir
}

type Globals struct {
	// Inject a context to interrupt service
	Context context.Context

	// CliContext parses command line arguments
	CliContext *CliContext

	// Indicates if the running minio is in gateway mode.
	IsGateway bool

	// Name of gateway server, e.g S3, GCS, Azure, etc
	GatewayName string

	// This flag is set to 'true' by default
	BrowserEnabled bool

	// This flag is set to 'us-east-1' by default
	ServerRegion string

	// MinIO default port, can be changed through command line.
	MinioPort string
	// Holds the host that was passed using --address
	MinioHost string
	// Holds the possible host endpoint.
	MinioEndpoint string

	// ConfigSys server config system.
	ConfigSys *ConfigSys

	NotificationSys  *NotificationSys
	ConfigTargetList *event.TargetList
	// EnvTargetList has list of targets configured via env.
	EnvTargetList *event.TargetList

	BucketMetadataSys *BucketMetadataSys
	BucketMonitor     *bandwidth.Monitor
	PolicySys         *PolicySys
	IAMSys            *IAMSys

	BucketSSEConfigSys *BucketSSEConfigSys
	BucketTargetSys    *BucketTargetSys
	// APIConfig controls S3 API requests throttling,
	// healthcheck readiness deadlines and cors settings.
	APIConfig apiConfig

	// CA root certificates, a nil value means system certs pool will be used
	RootCAs *x509.CertPool

	// IsSSL indicates if the server is configured with SSL.
	IsTLS bool

	TLSCerts *certs.Manager

	HTTPServerExternal bool
	HTTPServer         *xhttp.Server
	HTTPServerErrorCh  chan error
	OSSignalCh         chan os.Signal

	//  Trace system to send HTTP request/response
	// and Storage/OS calls info to registered listeners.
	Trace *pubsub.PubSub

	//  Listen system to send S3 API events to registered listeners
	HTTPListen *pubsub.PubSub

	//  console system to send console logs to
	// registered listeners
	ConsoleSys *HTTPConsoleLoggerSys

	Endpoints EndpointServerPools

	// The name of this local node, fetched from arguments
	LocalNodeName string

	// Global server's network statistics
	ConnStats *ConnStats

	// Global HTTP request statisitics
	HTTPStats *HTTPStats

	// Time when the server is started
	BootTime time.Time

	ActiveCred auth.Credentials

	// Hold the old server credentials passed by the environment
	OldCred auth.Credentials

	// Indicates if config is to be encrypted
	ConfigEncrypted bool

	PublicCerts []*x509.Certificate

	DomainNames []string // Root domains for virtual host style requests

	OperationTimeout *dynamicTimeout

	BucketObjectLockSys *BucketObjectLockSys
	BucketQuotaSys      *BucketQuotaSys
	BucketVersioningSys *BucketVersioningSys

	// Is compression enabled?
	CompressConfigMu sync.Mutex
	CompressConfig   compress.Config

	// Some standard object extensions which we strictly dis-allow for compression.
	standardExcludeCompressExtensions []string

	// Some standard content-types which we strictly dis-allow for compression.
	standardExcludeCompressContentTypes []string

	// Deployment ID - unique per deployment
	DeploymentID string

	// If writes to FS backend should be O_SYNC.
	FSOSync bool

	DNSCache *xhttp.DNSCache

	ObjLayerMutex sync.RWMutex
	ObjectAPI     ObjectLayer

	ServerConfig   config.Config
	ServerConfigMu sync.RWMutex

	CustomHandlers     []mux.MiddlewareFunc
	ReqParamExtractors []ReqParamExtractor
}

func NewGlobals() *Globals {
	return &Globals{
		IsGateway:                           false,
		ServerRegion:                        globalMinioDefaultRegion,
		MinioPort:                           GlobalMinioDefaultPort,
		APIConfig:                           apiConfig{listQuorum: 3},
		Trace:                               pubsub.New(),
		HTTPListen:                          pubsub.New(),
		ConnStats:                           newConnStats(),
		HTTPStats:                           newHTTPStats(),
		BootTime:                            UTCNow(),
		ConfigEncrypted:                     false,
		OperationTimeout:                    newDynamicTimeout(10*time.Minute, 5*time.Minute),
		CompressConfigMu:                    sync.Mutex{},
		CompressConfig:                      compress.Config{},
		standardExcludeCompressExtensions:   []string{".gz", ".bz2", ".rar", ".zip", ".7z", ".xz", ".mp4", ".mkv", ".mov", ".jpg", ".png", ".gif"},
		standardExcludeCompressContentTypes: []string{"video/*", "audio/*", "application/zip", "application/x-gzip", "application/x-zip-compressed", " application/x-compress", "application/x-spoon"},
		DNSCache:                            xhttp.NewDNSCache(10*time.Minute, 5*time.Second, logger.LogOnceIf),
		HTTPServerErrorCh:                   make(chan error),
		OSSignalCh:                          make(chan os.Signal, 1),
	}
}

func (g *Globals) newObjectLayerFn() ObjectLayer {
	g.ObjLayerMutex.RLock()
	defer g.ObjLayerMutex.RUnlock()
	return g.ObjectAPI
}

func (g *Globals) setObjectLayer(o ObjectLayer) {
	g.ObjLayerMutex.Lock()
	g.ObjectAPI = o
	g.ObjLayerMutex.Unlock()
}

func (g *Globals) setHTTPServer(server *xhttp.Server) {
	g.ObjLayerMutex.Lock()
	g.HTTPServer = server
	g.ObjLayerMutex.Unlock()
}

func (g *Globals) getHTTPServer() *xhttp.Server {
	g.ObjLayerMutex.RLock()
	defer g.ObjLayerMutex.RUnlock()
	return g.HTTPServer
}

var (
	// global Trace system to send HTTP request/response
	// and Storage/OS calls info to registered listeners.
	globalTrace = pubsub.New()
)

var errSelfTestFailure = errors.New("self test failed. unsafe to start server")

func (g *Globals) getAPIEndpoints() (apiEndpoints []string) {
	var ipList []string
	if g.MinioHost == "" {
		ipList = sortIPs(mustGetLocalIP4().ToSlice())
		ipList = append(ipList, mustGetLocalIP6().ToSlice()...)
	} else {
		ipList = []string{g.MinioHost}
	}

	for _, ip := range ipList {
		endpoint := fmt.Sprintf("%s://%s", getURLScheme(g.IsTLS), net.JoinHostPort(ip, g.MinioPort))
		apiEndpoints = append(apiEndpoints, endpoint)
	}

	return apiEndpoints
}

// Prints the formatted startup message.
func (g *Globals) printGatewayStartupMessage(backendType string) {
	strippedAPIEndpoints := stripStandardPorts(g.getAPIEndpoints(), g.MinioHost)

	// Prints credential.
	printGatewayCommonMsg(g, strippedAPIEndpoints)

	// Prints `mc` cli configuration message chooses
	// first endpoint as default.
	printCLIAccessMsg(g, strippedAPIEndpoints[0], fmt.Sprintf("my%s", backendType))

	// Prints documentation message.
	printObjectAPIMsg()

	// SSL is configured reads certification chain, prints
	// authority and expiry.
	if color.IsTerminal() && !g.CliContext.Anonymous {
		if g.IsTLS {
			printCertificateMsg(g.PublicCerts)
		}
	}
}

// Prints the formatted startup message.
func (g *Globals) printStartupMessage(err error) {
	if err != nil {
		logStartupMessage(color.RedBold("Server startup failed with '%v'", err))
		logStartupMessage(color.RedBold("Not all features may be available on this server"))
		logStartupMessage(color.RedBold("Please use 'mc admin' commands to further investigate this issue"))
	}

	strippedAPIEndpoints := stripStandardPorts(g.getAPIEndpoints(), g.MinioHost)

	// Object layer is initialized then print StorageInfo.
	objAPI := g.newObjectLayerFn()
	if objAPI != nil {
		printStorageInfo(g, mustGetStorageInfo(objAPI))
	}

	// Prints credential, region and browser access.
	printServerCommonMsg(g, strippedAPIEndpoints)

	// Prints `mc` cli configuration message chooses
	// first endpoint as default.
	printCLIAccessMsg(g, strippedAPIEndpoints[0], "myminio")

	// Prints documentation message.
	printObjectAPIMsg()

	// SSL is configured reads certification chain, prints
	// authority and expiry.
	if color.IsTerminal() && !g.CliContext.Anonymous {
		if g.IsTLS {
			printCertificateMsg(g.PublicCerts)
		}
	}
}
