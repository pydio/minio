/*
 * MinIO Cloud Storage, (C) 2015-2019 MinIO, Inc.
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
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/minio/cli"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/bucket/bandwidth"
	"github.com/minio/minio/pkg/color"
)

// ServerFlags - server command specific flags
var ServerFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":" + GlobalMinioDefaultPort,
		Usage: "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
	},
}

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "start object storage server",
	Flags:  append(ServerFlags, GlobalFlags...),
	Action: serverMain,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR1 [DIR2..]
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64}
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64} DIR{65...128}

DIR:
  DIR points to a directory on a filesystem. When you want to combine
  multiple drives into a single large system, pass one directory per
  filesystem separated by space. You may also use a '...' convention
  to abbreviate the directory arguments. Remote directories in a
  distributed setup are encoded as HTTP(s) URIs.
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXAMPLES:
  1. Start minio server on "/home/shared" directory.
     {{.Prompt}} {{.HelpName}} /home/shared

  2. Start single node server with 64 local drives "/mnt/data1" to "/mnt/data64".
     {{.Prompt}} {{.HelpName}} /mnt/data{1...64}

  3. Start distributed minio server on an 32 node setup with 32 drives each, run following command on all the nodes
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}minio
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}miniostorage
     {{.Prompt}} {{.HelpName}} http://node{1...32}.example.com/mnt/export{1...32}

  4. Start distributed minio server in an expanded setup, run the following command on all the nodes
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}minio
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}miniostorage
     {{.Prompt}} {{.HelpName}} http://node{1...16}.example.com/mnt/export{1...32} \
            http://node{17...64}.example.com/mnt/export{1...64}
`,
}

func newAllSubsystems(globals *Globals) {

	// Create new notification system and initialize notification targets
	globals.NotificationSys = NewNotificationSys(globals)

	// Create new bucket metadata system.
	globals.BucketMetadataSys = NewBucketMetadataSys(globals)

	// Create the bucket bandwidth monitor
	globals.BucketMonitor = bandwidth.NewMonitor(GlobalServiceDoneCh)

	// Create a new config system.
	globals.ConfigSys = NewConfigSys(globals)

	// Create new IAM system.
	globals.IAMSys = NewIAMSys(globals)

	// Create new policy system.
	globals.PolicySys = NewPolicySys(globals)

	// Create new bucket encryption subsystem
	globals.BucketSSEConfigSys = NewBucketSSEConfigSys(globals)

	// Create new bucket object lock subsystem
	globals.BucketObjectLockSys = NewBucketObjectLockSys(globals)

	// Create new bucket quota subsystem
	globals.BucketQuotaSys = NewBucketQuotaSys(globals)

	// Create new bucket versioning subsystem
	if globals.BucketVersioningSys == nil {
		globals.BucketVersioningSys = NewBucketVersioningSys(globals)
	} else {
		globals.BucketVersioningSys.Reset()
	}

	// Create new bucket replication subsytem
	globals.BucketTargetSys = NewBucketTargetSys(globals)

}

func configRetriableErrors(err error) bool {
	// Initializing sub-systems needs a retry mechanism for
	// the following reasons:

	// One of these retriable errors shall be retried.
	return errors.Is(err, errDiskNotFound) ||
		errors.Is(err, errConfigNotFound) ||
		errors.Is(err, context.DeadlineExceeded) ||
		isErrBucketNotFound(err) ||
		errors.Is(err, os.ErrDeadlineExceeded)
}

func initServerAndConfig(ctx context.Context, globals *Globals, newObject ObjectLayer) error {

	// Make sure to hold lock for entire migration to avoid
	// such that only one server should migrate the entire config
	// at a given time, this big transaction lock ensures this
	// appropriately. This is also true for rotation of encrypted
	// content.
	txnLk := newObject.NewNSLock(minioMetaBucket, minioConfigPrefix+"/transaction.lock")

	// ****  WARNING ****
	// Migrating to encrypted backend should happen before initialization of any
	// sub-systems, make sure that we do not move the above codeblock elsewhere.

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	lockTimeout := newDynamicTimeout(5*time.Second, 3*time.Second)

	var err error
	for {
		select {
		case <-ctx.Done():
			// Retry was canceled successfully.
			return fmt.Errorf("Initializing sub-systems stopped gracefully %w", ctx.Err())
		default:
		}

		// let one of the server acquire the lock, if not let them timeout.
		// which shall be retried again by this loop.
		if _, err = txnLk.GetLock(ctx, lockTimeout); err != nil {
			logger.Info("Waiting for all MinIO sub-systems to be initialized.. trying to acquire lock")

			time.Sleep(time.Duration(r.Float64() * float64(5*time.Second)))
			continue
		}

		// Migrate all backend configs to encrypted backend configs, optionally
		// handles rotating keys for encryption, if there is any retriable failure
		// that shall be retried if there is an error.
		if err = globals.handleEncryptedConfigBackend(newObject); err == nil {
			// Upon success migrating the config, initialize all sub-systems
			// if all sub-systems initialized successfully return right away
			if err = initAllSubsystems(ctx, globals, newObject); err == nil {
				txnLk.Unlock()
				return nil
			}
		}

		txnLk.Unlock() // Unlock the transaction lock and allow other nodes to acquire the lock if possible.

		if configRetriableErrors(err) {
			logger.Info("Waiting for all MinIO sub-systems to be initialized.. possible cause (%v)", err)
			time.Sleep(time.Duration(r.Float64() * float64(5*time.Second)))
			continue
		}

		// Any other unhandled return right here.
		return fmt.Errorf("Unable to initialize sub-systems: %w", err)
	}
}

func initAllSubsystems(ctx context.Context, globals *Globals, newObject ObjectLayer) (err error) {
	// %w is used by all error returns here to make sure
	// we wrap the underlying error, make sure when you
	// are modifying this code that you do so, if and when
	// you want to add extra context to your error. This
	// ensures top level retry works accordingly.
	// List buckets to heal, and be re-used for loading configs.

	buckets, err := newObject.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("Unable to list buckets to heal: %w", err)
	}

	// Initialize config system.
	if err = globals.ConfigSys.Init(newObject); err != nil {
		if configRetriableErrors(err) {
			return fmt.Errorf("Unable to initialize config system: %w", err)
		}
		// Any other config errors we simply print a message and proceed forward.
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize config, some features may be missing %w", err))
	}

	// Initialize bucket metadata sub-system.
	if er := globals.BucketMetadataSys.Init(ctx, buckets, newObject); er != nil {
		return er
	}

	// Initialize notification system.
	if er := globals.NotificationSys.Init(ctx, buckets, newObject); er != nil {
		return er
	}

	// Initialize bucket targets sub-system.
	if er := globals.BucketTargetSys.Init(ctx, buckets, newObject); er != nil {
		return er
	}

	return nil
}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	globals := NewGlobals()

	// Handle common command args.
	handleCommonCmdArgs(ctx, globals)

	// Handle common env vars.
	handleCommonEnvVars(globals)

	StartServerWithGlobals(globals, ctx.Args()...)
}

// StartServerWithGlobals can launch a server with instanciated globals configuration
func StartServerWithGlobals(globals *Globals, folderNames ...string) {
	defer globals.DNSCache.Stop()

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error situation we check for port availability.
	globals.MinioHost, globals.MinioPort = mustSplitHostPort(globals.CliContext.Addr)
	logger.FatalIf(checkPortAvailability(globals.MinioHost, globals.MinioPort), "Unable to start the server")

	if globals.CliContext.Quiet {
		logger.EnableQuiet()
	}
	if globals.CliContext.JSON {
		logger.EnableJSON()
	}

	setDefaultProfilerRates()

	// Perform any self-tests
	bitrotSelfTest()

	compressSelfTest()

	initHelpOnce() // this initialize some configuration defaults values

	initTlsVars(globals)

	var err error
	globals.Endpoints, _, err = createServerEndpoints(globals.CliContext.Addr, globals.MinioPort, folderNames...)
	logger.FatalIf(err, "Invalid command line arguments")
	globals.LocalNodeName = GetLocalPeer(globals.Endpoints, globals.MinioHost, globals.MinioPort)

	// Initialize all sub-systems
	newAllSubsystems(globals)

	// Set system resources to maximum.
	_ = setMaxResources()

	// Router & HTTP
	router := initRouter(globals)
	initHttpServer(globals, router)

	newObject, err := NewFSObjectLayer(globals, globals.Endpoints[0].Endpoints[0].Path)
	if err != nil {
		logFatalErrs(err, Endpoint{}, true)
	}

	logger.SetDeploymentID(globals.DeploymentID)

	initDataScanner(globals.Context, globals, newObject)

	// Once the config is fully loaded, initialize the new object layer.
	globals.setObjectLayer(newObject)

	if err = initServerAndConfig(globals.Context, globals, newObject); err != nil {
		var cerr config.Err
		// For any config error, we don't need to drop into safe-mode
		// instead its a user error and should be fixed by user.
		if errors.As(err, &cerr) {
			logger.FatalIf(err, "Unable to initialize the server")
		}

		// If context was canceled
		if errors.Is(err, context.Canceled) {
			logger.FatalIf(err, "Server startup canceled upon user request")
		}
	}

	// Initialize users credentials and policies in background right after config has initialized.
	go globals.IAMSys.Init(globals.Context, newObject)

	// Prints the formatted startup message, if err is not nil then it prints additional information as well.
	globals.printStartupMessage(err)

	if globals.ActiveCred.Equal(auth.DefaultCredentials) {
		msg := fmt.Sprintf("Detected default credentials '%s', please change the credentials immediately using 'MINIO_ROOT_USER' and 'MINIO_ROOT_PASSWORD'", globals.ActiveCred)
		logger.StartupMessage(color.RedBold(msg))
	}

	if globals.Context != nil {
		<-globals.Context.Done()
		if globals.NotificationSys != nil {
			globals.NotificationSys.RemoveAllRemoteTargets()
		}

		if httpServer := globals.getHTTPServer(); httpServer != nil {
			err = httpServer.Shutdown()
			if !errors.Is(err, http.ErrServerClosed) {
				logger.LogIf(context.Background(), err)
			}
			logger.Info("Stopped Minio service on " + globals.MinioPort)
		}

		if objAPI := globals.newObjectLayerFn(); objAPI != nil {
			oerr := objAPI.Shutdown(context.Background())
			logger.LogIf(context.Background(), oerr)
		}
	} else {
		// watch OS signals
		signal.Notify(globals.OSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
		handleSignals(globals)
	}
}
