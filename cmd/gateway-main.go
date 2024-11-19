/*
 * MinIO Cloud Storage, (C) 2017-2020 MinIO, Inc.
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
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/minio/cli"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
)

var (
	gatewayCmd = cli.Command{
		Name:            "gateway",
		Usage:           "start object storage gateway",
		Flags:           append(ServerFlags, GlobalFlags...),
		HideHelpCommand: true,
	}
)

// GatewayLocker implements custom NewNSLock implementation
type GatewayLocker struct {
	ObjectLayer
	nsMutex *nsLockMap
}

// NewNSLock - implements gateway level locker
func (l *GatewayLocker) NewNSLock(bucket string, objects ...string) RWLocker {
	return l.nsMutex.NewNSLock(nil, bucket, objects...)
}

// Walk - implements common gateway level Walker, to walk on all objects recursively at a prefix
func (l *GatewayLocker) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	walk := func(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo) error {
		go func() {
			// Make sure the results channel is ready to be read when we're done.
			defer close(results)

			var marker string

			for {
				// set maxKeys to '0' to list maximum possible objects in single call.
				loi, err := l.ObjectLayer.ListObjects(ctx, bucket, prefix, marker, "", 0)
				if err != nil {
					logger.LogIf(ctx, err)
					return
				}
				marker = loi.NextMarker
				for _, obj := range loi.Objects {
					select {
					case results <- obj:
					case <-ctx.Done():
						return
					}
				}
				if !loi.IsTruncated {
					break
				}
			}
		}()
		return nil
	}

	if err := l.ObjectLayer.Walk(ctx, bucket, prefix, results, opts); err != nil {
		if _, ok := err.(NotImplemented); ok {
			return walk(ctx, bucket, prefix, results)
		}
		return err
	}

	return nil
}

// NewGatewayLayerWithLocker - initialize gateway with locker.
func NewGatewayLayerWithLocker(gwLayer ObjectLayer) ObjectLayer {
	return &GatewayLocker{ObjectLayer: gwLayer, nsMutex: newNSLock(false)}
}

// RegisterGatewayCommand registers a new command for gateway.
func RegisterGatewayCommand(cmd cli.Command) error {
	cmd.Flags = append(append(cmd.Flags, ServerFlags...), GlobalFlags...)
	gatewayCmd.Subcommands = append(gatewayCmd.Subcommands, cmd)
	return nil
}

// ParseGatewayEndpoint - Return endpoint.
func ParseGatewayEndpoint(arg string) (endPoint string, secure bool, err error) {
	schemeSpecified := len(strings.Split(arg, "://")) > 1
	if !schemeSpecified {
		// Default connection will be "secure".
		arg = "https://" + arg
	}

	u, err := url.Parse(arg)
	if err != nil {
		return "", false, err
	}

	switch u.Scheme {
	case "http":
		return u.Host, false, nil
	case "https":
		return u.Host, true, nil
	default:
		return "", false, fmt.Errorf("Unrecognized scheme %s", u.Scheme)
	}
}

// ValidateGatewayArguments - Validate gateway arguments.
func ValidateGatewayArguments(serverAddr, endpointAddr string) error {
	if err := CheckLocalServerAddr(serverAddr); err != nil {
		return err
	}

	if endpointAddr != "" {
		// Reject the endpoint if it points to the gateway handler itself.
		sameTarget, err := sameLocalAddrs(endpointAddr, serverAddr)
		if err != nil {
			return err
		}
		if sameTarget {
			return fmt.Errorf("endpoint points to the local gateway")
		}
	}
	return nil
}

func initGatewayConfig(globals *Globals) {
	// TODO: We need to move this code with globalConfigSys.Init()
	// for now keep it here such that "s3" gateway layer initializes
	// itself properly when KMS is set.

	// Initialize server config.
	srvCfg := globals.newServerConfig()

	// Override any values from ENVs.
	globals.lookupConfigs(srvCfg, nil)

	// hold the mutex lock before a new config is assigned.
	globals.ServerConfigMu.Lock()
	globals.ServerConfig = srvCfg
	globals.ServerConfigMu.Unlock()

}

func StartGateway(ctx *cli.Context, gw Gateway) {
	globals := NewGlobals()
	if gw == nil {
		logger.FatalIf(errUnexpected, "Gateway implementation not initialized")
	}

	// Handle common command args.
	handleCommonCmdArgs(ctx, globals)

	// Handle common env vars.
	handleCommonEnvVars(globals)

	StartGatewayWithGlobals(globals, gw)
}

func CreateGatewayRouter(globals *Globals) *mux.Router {
	return initRouter(globals)
}

// StartGatewayWithGlobals -  can be used to inject start parameter from outside
// It will no check for ENV arguments and command line arguments
func StartGatewayWithGlobals(globals *Globals, gw Gateway) {

	defer globals.DNSCache.Stop()

	if globals.CliContext.Quiet {
		logger.EnableQuiet()
	}
	if globals.CliContext.JSON {
		logger.EnableJSON()
	}

	// do not remove - this initializes some configuration defaults values
	initHelpOnce()

	if gw == nil {
		logger.FatalIf(errUnexpected, "Gateway implementation not initialized")
	}

	initTlsVars(globals)

	// This is only to uniquely identify each gateway deployments.
	globals.DeploymentID = env.Get("MINIO_GATEWAY_DEPLOYMENT_ID", mustGetUUID())
	logger.SetDeploymentID(globals.DeploymentID)
	globals.GatewayName = gw.Name()
	globals.IsGateway = true

	if !globals.ActiveCred.IsValid() {
		logger.Fatal(config.ErrInvalidCredentials(nil),
			"Unable to validate credentials inherited from the shell environment")
	}

	// Calls all New() for all sub-systems.
	newAllSubsystems(globals)

	// Set system resources to maximum.
	_ = setMaxResources()

	initGatewayConfig(globals)

	newObject, err := gw.NewGatewayLayer(globals, globals.ActiveCred)
	if err != nil {
		_ = globals.HTTPServer.Shutdown()
		logger.FatalIf(err, "Unable to initialize gateway backend")
	}
	// Wrap GW layer inside a locker
	newObject = NewGatewayLayerWithLocker(newObject)

	// Once endpoints are finalized, initialize the new object api in safe mode.
	globals.setObjectLayer(newObject)

	// Verify if object layer supports
	// - encryption
	// - compression
	globals.verifyObjectLayerFeatures("gateway "+globals.GatewayName, newObject)

	// Prints the formatted startup message once object layer is initialized.
	if !globals.CliContext.Quiet {
		globals.printGatewayStartupMessage(globals.GatewayName)
	}

	// Router & HTTP
	if !globals.HTTPServerExternal {
		router := CreateGatewayRouter(globals)
		// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
		// to IPv6 address ie minio will start listening on IPv6 address whereas another
		// (non-)minio process is listening on IPv4 of given port.
		// To avoid this error situation we check for port availability.
		globals.MinioHost, globals.MinioPort = mustSplitHostPort(globals.CliContext.Addr)
		logger.FatalIf(checkPortAvailability(globals.MinioHost, globals.MinioPort), "Unable to start the gateway")
		logger.Info("Starting HTTPServer for gateway " + globals.GatewayName)
		initHttpServer(globals, router)
	} else {
		logger.Info("Running gateway " + globals.GatewayName)
	}

	if globals.Context != nil {
		<-globals.Context.Done()
	} else {
		// watch OS signals
		signal.Notify(globals.OSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
		handleSignals(globals)
	}
}
