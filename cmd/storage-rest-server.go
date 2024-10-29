/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"fmt"
	"os/user"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
)

// A single function to write certain errors to be fatal
// or informative based on the `exit` flag, please look
// at each implementation of error for added hints.
//
// FIXME: This is an unusual function but serves its purpose for
// now, need to revist the overall erroring structure here.
// Do not like it :-(
func logFatalErrs(err error, endpoint Endpoint, exit bool) {
	if errors.Is(err, errMinDiskSize) {
		logger.Fatal(config.ErrUnableToWriteInBackend(err).Hint(err.Error()), "Unable to initialize backend")
	} else if errors.Is(err, errUnsupportedDisk) {
		var hint string
		if endpoint.URL != nil {
			hint = fmt.Sprintf("Disk '%s' does not support O_DIRECT flags, MinIO erasure coding requires filesystems with O_DIRECT support", endpoint.Path)
		} else {
			hint = "Disks do not support O_DIRECT flags, MinIO erasure coding requires filesystems with O_DIRECT support"
		}
		logger.Fatal(config.ErrUnsupportedBackend(err).Hint(hint), "Unable to initialize backend")
	} else if errors.Is(err, errDiskNotDir) {
		var hint string
		if endpoint.URL != nil {
			hint = fmt.Sprintf("Disk '%s' is not a directory, MinIO erasure coding needs a directory", endpoint.Path)
		} else {
			hint = "Disks are not directories, MinIO erasure coding needs directories"
		}
		logger.Fatal(config.ErrUnableToWriteInBackend(err).Hint(hint), "Unable to initialize backend")
	} else if errors.Is(err, errFileAccessDenied) {
		// Show a descriptive error with a hint about how to fix it.
		var username string
		if u, err := user.Current(); err == nil {
			username = u.Username
		} else {
			username = "<your-username>"
		}
		var hint string
		if endpoint.URL != nil {
			hint = fmt.Sprintf("Run the following command to add write permissions: `sudo chown -R %s %s && sudo chmod u+rxw %s`",
				username, endpoint.Path, endpoint.Path)
		} else {
			hint = fmt.Sprintf("Run the following command to add write permissions: `sudo chown -R %s. <path> && sudo chmod u+rxw <path>`", username)
		}
		logger.Fatal(config.ErrUnableToWriteInBackend(err).Hint(hint), "Unable to initialize backend")
	} else if errors.Is(err, errFaultyDisk) {
		if !exit {
			logger.LogIf(GlobalContext, fmt.Errorf("disk is faulty at %s, please replace the drive - disk will be offline", endpoint))
		} else {
			logger.Fatal(err, "Unable to initialize backend")
		}
	} else if errors.Is(err, errDiskFull) {
		if !exit {
			logger.LogIf(GlobalContext, fmt.Errorf("disk is already full at %s, incoming I/O will fail - disk will be offline", endpoint))
		} else {
			logger.Fatal(err, "Unable to initialize backend")
		}
	} else {
		if !exit {
			logger.LogIf(GlobalContext, fmt.Errorf("disk returned an unexpected error at %s, please investigate - disk will be offline", endpoint))
		} else {
			logger.Fatal(err, "Unable to initialize backend")
		}
	}
}
