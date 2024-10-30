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

package notify

import (
	"context"
	"errors"
	"net/http"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/event"
)

const (
	formatNamespace = "namespace"
)

// ErrTargetsOffline - Indicates single/multiple target failures.
var ErrTargetsOffline = errors.New("one or more targets are offline. Please use `mc admin info --json` to check the offline targets")

// TestNotificationTargets is similar to GetNotificationTargets()
// avoids explicit registration.
func TestNotificationTargets(ctx context.Context, cfg config.Config, transport *http.Transport, targetIDs []event.TargetID) error {
	test := true
	returnOnTargetError := true
	targets, err := RegisterNotificationTargets(ctx, cfg, transport, targetIDs, test, returnOnTargetError)
	if err == nil {
		// Close all targets since we are only testing connections.
		for _, t := range targets.TargetMap() {
			_ = t.Close()
		}
	}

	return err
}

// GetNotificationTargets registers and initializes all notification
// targets, returns error if any.
func GetNotificationTargets(ctx context.Context, cfg config.Config, transport *http.Transport, test bool) (*event.TargetList, error) {
	returnOnTargetError := false
	return RegisterNotificationTargets(ctx, cfg, transport, nil, test, returnOnTargetError)
}

// RegisterNotificationTargets - returns TargetList which contains enabled targets in serverConfig.
// A new notification target is added like below
// * Add a new target in pkg/event/target package.
// * Add newly added target configuration to serverConfig.Notify.<TARGET_NAME>.
// * Handle the configuration in this function to create/add into TargetList.
func RegisterNotificationTargets(ctx context.Context, cfg config.Config, transport *http.Transport, targetIDs []event.TargetID, test bool, returnOnTargetError bool) (*event.TargetList, error) {
	targetList, err := FetchRegisteredTargets(ctx, cfg, transport, test, returnOnTargetError)
	if err != nil {
		return targetList, err
	}

	if test {
		// Verify if user is trying to disable already configured
		// notification targets, based on their target IDs
		for _, targetID := range targetIDs {
			if !targetList.Exists(targetID) {
				return nil, config.Errorf(
					"Unable to disable configured targets '%v'",
					targetID)
			}
		}
	}

	return targetList, nil
}

// FetchRegisteredTargets - Returns a set of configured TargetList
// If `returnOnTargetError` is set to true, The function returns when a target initialization fails
// Else, the function will return a complete TargetList irrespective of errors
func FetchRegisteredTargets(ctx context.Context, cfg config.Config, transport *http.Transport, test bool, returnOnTargetError bool) (_ *event.TargetList, err error) {
	targetList := event.NewTargetList()

	defer func() {
		// Automatically close all connections to targets when an error occur.
		// Close all the targets if returnOnTargetError is set
		// Else, close only the failed targets
		if err != nil && returnOnTargetError {
			for _, t := range targetList.TargetMap() {
				_ = t.Close()
			}
		}
	}()

	if err = checkValidNotificationKeys(cfg); err != nil {
		return nil, err
	}

	return targetList, nil
}

// DefaultNotificationKVS - default notification list of kvs.
var (
	DefaultNotificationKVS = map[string]config.KVS{}
)

func checkValidNotificationKeys(cfg config.Config) error {
	for subSys, tgt := range cfg {
		validKVS, ok := DefaultNotificationKVS[subSys]
		if !ok {
			continue
		}
		for tname, kv := range tgt {
			subSysTarget := subSys
			if tname != config.Default {
				subSysTarget = subSys + config.SubSystemSeparator + tname
			}
			if v, ok := kv.Lookup(config.Enable); ok && v == config.EnableOn {
				if err := config.CheckValidKeys(subSysTarget, kv, validKVS); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
