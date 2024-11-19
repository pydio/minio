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
	"context"
	"crypto/x509"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/gorilla/mux"
	dns2 "github.com/miekg/dns"
	"github.com/minio/cli"

	"github.com/minio/minio/cmd/config"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/certs"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/minio/pkg/env"
)

// serverDebugLog will enable debug printing
var (
	serverDebugLog = env.Get("_MINIO_SERVER_DEBUG", config.EnableOff) == config.EnableOn
	ihOnce         = sync.Once{}
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())

	logger.Init(GOPATH, GOROOT)
	logger.RegisterError(config.FmtError)

	// Inject into config package.
	config.Logger.Info = logger.Info
	config.Logger.LogIf = logger.LogIf

	initGlobalContext()

	console.SetColor("Debug", color.New())

	gob.Register(StorageErr(""))

}

func (g *Globals) verifyObjectLayerFeatures(name string, objAPI ObjectLayer) {
	g.CompressConfigMu.Lock()
	if g.CompressConfig.Enabled && !objAPI.IsCompressionSupported() {
		logger.Fatal(errInvalidArgument,
			"Compression support is requested but '%s' does not support compression", name)
	}
	g.CompressConfigMu.Unlock()
}

func newConfigDirFromCtx(ctx *cli.Context, option string, getDefaultDir func() string) (*ConfigDir, bool) {
	var dir string
	var dirSet bool

	switch {
	case ctx.IsSet(option):
		dir = ctx.String(option)
		dirSet = true
	case ctx.GlobalIsSet(option):
		dir = ctx.GlobalString(option)
		dirSet = true
		// cli package does not expose parent's option option.  Below code is workaround.
		if dir == "" || dir == getDefaultDir() {
			dirSet = false // Unset to false since GlobalIsSet() true is a false positive.
			if ctx.Parent().GlobalIsSet(option) {
				dir = ctx.Parent().GlobalString(option)
				dirSet = true
			}
		}
	default:
		// Neither local nor global option is provided.  In this case, try to use
		// default directory.
		dir = getDefaultDir()
		if dir == "" {
			logger.FatalIf(errInvalidArgument, "%s option must be provided", option)
		}
	}

	if dir == "" {
		logger.FatalIf(errors.New("empty directory"), "%s directory cannot be empty", option)
	}

	// Disallow relative paths, figure out absolute paths.
	dirAbs, err := filepath.Abs(dir)
	logger.FatalIf(err, "Unable to fetch absolute path for %s=%s", option, dir)

	logger.FatalIf(mkdirAllIgnorePerm(dirAbs), "Unable to create directory specified %s=%s", option, dir)

	return &ConfigDir{path: dirAbs}, dirSet
}

func handleCommonCmdArgs(ctx *cli.Context, globals *Globals) {

	cliCtx := &CliContext{}
	// Get "json" flag from command line argument and
	// enable json and quite modes if json flag is turned on.
	cliCtx.JSON = ctx.IsSet("json") || ctx.GlobalIsSet("json")
	if cliCtx.JSON {
		logger.EnableJSON()
	}

	// Get quiet flag from command line argument.
	cliCtx.Quiet = ctx.IsSet("quiet") || ctx.GlobalIsSet("quiet")
	if cliCtx.Quiet {
		logger.EnableQuiet()
	}

	// Get anonymous flag from command line argument.
	cliCtx.Anonymous = ctx.IsSet("anonymous") || ctx.GlobalIsSet("anonymous")
	if cliCtx.Anonymous {
		logger.EnableAnonymous()
	}

	// Fetch address option
	cliCtx.Addr = ctx.GlobalString("address")
	if cliCtx.Addr == "" || cliCtx.Addr == ":"+GlobalMinioDefaultPort {
		cliCtx.Addr = ctx.String("address")
	}
	logger.FatalIf(CheckLocalServerAddr(cliCtx.Addr), "Unable to validate passed arguments")

	// Check "no-compat" flag from command line argument.
	cliCtx.StrictS3Compat = true
	if ctx.IsSet("no-compat") || ctx.GlobalIsSet("no-compat") {
		cliCtx.StrictS3Compat = false
	}

	// Set all config, certs and CAs directories.
	var configSet, certsSet bool
	cliCtx.ConfigDir, configSet = newConfigDirFromCtx(ctx, "config-dir", defaultConfigDir.Get)
	cliCtx.CertsDir, certsSet = newConfigDirFromCtx(ctx, "certs-dir", defaultCertsDir.Get)

	// Remove this code when we deprecate and remove config-dir.
	// This code is to make sure we inherit from the config-dir
	// option if certs-dir is not provided.
	if !certsSet && configSet {
		cliCtx.CertsDir = &ConfigDir{path: filepath.Join(cliCtx.ConfigDir.Get(), certsDir)}
	}

	cliCtx.CertsCADir = &ConfigDir{path: filepath.Join(cliCtx.CertsDir.Get(), certsCADir)}

	logger.FatalIf(mkdirAllIgnorePerm(cliCtx.CertsCADir.Get()), "Unable to create certs CA directory at %s", cliCtx.CertsCADir.Get())

	globals.CliContext = cliCtx

}

func initHelpOnce() {
	ihOnce.Do(func() {
		initHelp()
	})
}

func initRouter(g *Globals) *mux.Router {
	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	// normalizing URL path minio/minio#3256
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()
	registerAPIRouter(g, router)

	//hh := append(globalHandlers, injectGlobalsHandler(globals))
	hh := append([]mux.MiddlewareFunc{injectGlobalsHandler(g)}, globalHandlers...)
	hh = append(hh, g.CustomHandlers...)
	router.Use(hh...)
	return router
}

func initHttpServer(g *Globals, router *mux.Router) {
	var getCert certs.GetCertificateFunc
	if g.TLSCerts != nil {
		getCert = g.TLSCerts.GetCertificate
	}

	httpServer := xhttp.NewServer([]string{g.CliContext.Addr}, criticalErrorHandler{g.corsHandler(router)}, getCert)

	ctx := GlobalContext
	if g.Context != nil {
		ctx = g.Context
	}
	httpServer.BaseContext = func(listener net.Listener) context.Context {
		return ctx
	}
	go func() {
		g.HTTPServerErrorCh <- httpServer.Start()
	}()

	g.setHTTPServer(httpServer)

}

func initTlsVars(g *Globals) {
	var err error

	// Check and load TLS certificates.
	g.PublicCerts, g.TLSCerts, g.IsTLS, err = g.getTLSConfig()
	logger.FatalIf(err, "Unable to load the TLS configuration")

	// Check and load Root CAs.
	g.RootCAs, err = certs.GetRootCAs(g.CliContext.CertsCADir.Get())
	logger.FatalIf(err, "Failed to read root CAs (%v)", err)

	// Add the global public crts as part of global root CAs
	for _, publicCrt := range g.PublicCerts {
		g.RootCAs.AddCert(publicCrt)
	}

	// Register root CAs for remote ENVs
	env.RegisterGlobalCAs(g.RootCAs)

}

func handleCommonEnvVars(g *Globals) {

	var err error

	g.FSOSync, err = config.ParseBool(env.Get(config.EnvFSOSync, config.EnableOff))
	if err != nil {
		logger.Fatal(config.ErrInvalidFSOSyncValue(err), "Invalid MINIO_FS_OSYNC value in environment variable")
	}

	domains := env.Get(config.EnvDomain, "")
	if len(domains) != 0 {
		for _, domainName := range strings.Split(domains, config.ValueSeparator) {
			if _, ok := dns2.IsDomainName(domainName); !ok {
				logger.Fatal(config.ErrInvalidDomainValue(nil).Msg("Unknown value `%s`", domainName),
					"Invalid MINIO_DOMAIN value in environment variable")
			}
			g.DomainNames = append(g.DomainNames, domainName)
		}
		sort.Strings(g.DomainNames)
		lcpSuf := lcpSuffix(g.DomainNames)
		for _, domainName := range g.DomainNames {
			if domainName == lcpSuf && len(g.DomainNames) > 1 {
				logger.Fatal(config.ErrOverlappingDomainValue(nil).Msg("Overlapping domains `%s` not allowed", g.DomainNames),
					"Invalid MINIO_DOMAIN value in environment variable")
			}
		}
	}

	if env.IsSet(config.EnvAccessKey) || env.IsSet(config.EnvSecretKey) {
		cred, err := auth.CreateCredentials(env.Get(config.EnvAccessKey, ""), env.Get(config.EnvSecretKey, ""))
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the shell environment")
		}
		g.ActiveCred = cred
		g.ConfigEncrypted = true
	}

	if env.IsSet(config.EnvRootUser) || env.IsSet(config.EnvRootPassword) {
		cred, err := auth.CreateCredentials(env.Get(config.EnvRootUser, ""), env.Get(config.EnvRootPassword, ""))
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the shell environment")
		}
		g.ActiveCred = cred
		g.ConfigEncrypted = true
	}

	if env.IsSet(config.EnvAccessKeyOld) && env.IsSet(config.EnvSecretKeyOld) {
		oldCred, err := auth.CreateCredentials(env.Get(config.EnvAccessKeyOld, ""), env.Get(config.EnvSecretKeyOld, ""))
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate the old credentials inherited from the shell environment")
		}
		g.OldCred = oldCred
		os.Unsetenv(config.EnvAccessKeyOld)
		os.Unsetenv(config.EnvSecretKeyOld)
	}

	if env.IsSet(config.EnvRootUserOld) && env.IsSet(config.EnvRootPasswordOld) {
		oldCred, err := auth.CreateCredentials(env.Get(config.EnvRootUserOld, ""), env.Get(config.EnvRootPasswordOld, ""))
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate the old credentials inherited from the shell environment")
		}
		g.OldCred = oldCred
		os.Unsetenv(config.EnvRootUserOld)
		os.Unsetenv(config.EnvRootPasswordOld)
	}
}

func logStartupMessage(msg string) {
	//if globalConsoleSys != nil {
	//	globalConsoleSys.Send(msg, string(logger.All))
	//}
	logger.StartupMessage(msg)
}

func (g *Globals) getTLSConfig() (x509Certs []*x509.Certificate, manager *certs.Manager, secureConn bool, err error) {
	if !(isFile(g.getPublicCertFile()) && isFile(g.getPrivateKeyFile())) {
		return nil, nil, false, nil
	}

	if x509Certs, err = config.ParsePublicCertFile(g.getPublicCertFile()); err != nil {
		return nil, nil, false, err
	}

	manager, err = certs.NewManager(GlobalContext, g.getPublicCertFile(), g.getPrivateKeyFile(), config.LoadX509KeyPair)
	if err != nil {
		return nil, nil, false, err
	}

	// MinIO has support for multiple certificates. It expects the following structure:
	//  certs/
	//   │
	//   ├─ public.crt
	//   ├─ private.key
	//   │
	//   ├─ example.com/
	//   │   │
	//   │   ├─ public.crt
	//   │   └─ private.key
	//   └─ foobar.org/
	//      │
	//      ├─ public.crt
	//      └─ private.key
	//   ...
	//
	// Therefore, we read all filenames in the cert directory and check
	// for each directory whether it contains a public.crt and private.key.
	// If so, we try to add it to certificate manager.
	root, err := os.Open(g.CliContext.CertsDir.Get())
	if err != nil {
		return nil, nil, false, err
	}
	defer root.Close()

	files, err := root.Readdir(-1)
	if err != nil {
		return nil, nil, false, err
	}
	for _, file := range files {
		// Ignore all
		// - regular files
		// - "CAs" directory
		// - any directory which starts with ".."
		if file.Mode().IsRegular() || file.Name() == "CAs" || strings.HasPrefix(file.Name(), "..") {
			continue
		}
		if file.Mode()&os.ModeSymlink == os.ModeSymlink {
			file, err = os.Stat(filepath.Join(root.Name(), file.Name()))
			if err != nil {
				// not accessible ignore
				continue
			}
			if !file.IsDir() {
				continue
			}
		}

		var (
			certFile = filepath.Join(root.Name(), file.Name(), publicCertFile)
			keyFile  = filepath.Join(root.Name(), file.Name(), privateKeyFile)
		)
		if !isFile(certFile) || !isFile(keyFile) {
			continue
		}
		if err = manager.AddCertificate(certFile, keyFile); err != nil {
			err = fmt.Errorf("Unable to load TLS certificate '%s,%s': %w", certFile, keyFile, err)
			logger.LogIf(GlobalContext, err, logger.Minio)
		}
	}
	secureConn = true
	return x509Certs, manager, secureConn, nil
}

// contextCanceled returns whether a context is canceled.
func contextCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
