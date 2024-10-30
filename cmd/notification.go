/*
 * MinIO Cloud Storage, (C) 2018, 2019 MinIO, Inc.
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
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	bandwidth "github.com/minio/minio/pkg/bandwidth"
	bucketBandwidth "github.com/minio/minio/pkg/bucket/bandwidth"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

// NotificationSys - notification system.
type NotificationSys struct {
	sync.RWMutex
	*Globals
	targetList                 *event.TargetList
	targetResCh                chan event.TargetIDResult
	bucketRulesMap             map[string]event.RulesMap
	bucketRemoteTargetRulesMap map[string]map[event.TargetID]event.RulesMap
}

// GetARNList - returns available ARNs.
func (sys *NotificationSys) GetARNList(onlyActive bool) []string {
	arns := []string{}
	if sys == nil {
		return arns
	}
	region := sys.Globals.ServerRegion
	for targetID, target := range sys.targetList.TargetMap() {
		// httpclient target is part of ListenNotification
		// which doesn't need to be listed as part of the ARN list
		// This list is only meant for external targets, filter
		// this out pro-actively.
		if !strings.HasPrefix(targetID.ID, "httpclient+") {
			if onlyActive && !target.HasQueueStore() {
				if _, err := target.IsActive(); err != nil {
					continue
				}
			}
			arns = append(arns, targetID.ToARN(region).String())
		}
	}

	return arns
}

// NotificationPeerErr returns error associated for a remote peer.
type NotificationPeerErr struct {
	Host xnet.Host // Remote host on which the rpc call was initiated
	Err  error     // Error returned by the remote peer for an rpc call
}

// A NotificationGroup is a collection of goroutines working on subtasks that are part of
// the same overall task.
//
// A zero NotificationGroup is valid and does not cancel on error.
type NotificationGroup struct {
	wg   sync.WaitGroup
	errs []NotificationPeerErr
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the slice of errors from all function calls.
func (g *NotificationGroup) Wait() []NotificationPeerErr {
	g.wg.Wait()
	return g.errs
}

// Go calls the given function in a new goroutine.
//
// The first call to return a non-nil error will be
// collected in errs slice and returned by Wait().
func (g *NotificationGroup) Go(ctx context.Context, f func() error, index int, addr xnet.Host) {
	g.wg.Add(1)

	go func() {
		defer g.wg.Done()
		g.errs[index] = NotificationPeerErr{
			Host: addr,
		}
		for i := 0; i < 3; i++ {
			if err := f(); err != nil {
				g.errs[index].Err = err
				// Last iteration log the error.
				if i == 2 {
					reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", addr.String())
					ctx := logger.SetReqInfo(ctx, reqInfo)
					logger.LogIf(ctx, err)
				}
				// Wait for one second and no need wait after last attempt.
				if i < 2 {
					time.Sleep(1 * time.Second)
				}
				continue
			}
			break
		}
	}()
}

// updateBloomFilter will cycle all servers to the current index and
// return a merged bloom filter if a complete one can be retrieved.
func (sys *NotificationSys) updateBloomFilter(ctx context.Context, current uint64) (*bloomFilter, error) {
	var req = bloomFilterRequest{
		Current: current,
		Oldest:  current - dataUsageUpdateDirCycles,
	}
	if current < dataUsageUpdateDirCycles {
		req.Oldest = 0
	}

	// Load initial state from local...
	var bf *bloomFilter
	bfr, err := intDataUpdateTracker.cycleFilter(ctx, req)
	logger.LogIf(ctx, err)
	if err == nil && bfr.Complete {
		nbf := intDataUpdateTracker.newBloomFilter()
		bf = &nbf
		_, err = bf.ReadFrom(bytes.NewReader(bfr.Filter))
		logger.LogIf(ctx, err)
	}

	return bf, nil
}

// LoadBucketMetadata - calls LoadBucketMetadata call on all peers
func (sys *NotificationSys) LoadBucketMetadata(ctx context.Context, bucketName string) {
}

// DeleteBucketMetadata - calls DeleteBucketMetadata call on all peers
func (sys *NotificationSys) DeleteBucketMetadata(ctx context.Context, bucketName string) {
	sys.Globals.BucketMetadataSys.Remove(bucketName)
}

// GetClusterBucketStats - calls GetClusterBucketStats call on all peers for a cluster statistics view.
func (sys *NotificationSys) GetClusterBucketStats(ctx context.Context, bucketName string) (bucketStats []BucketStats) {
	return bucketStats
}

// Loads notification policies for all buckets into NotificationSys.
func (sys *NotificationSys) load(buckets []BucketInfo) {
	for _, bucket := range buckets {
		ctx := logger.SetReqInfo(GlobalContext, &logger.ReqInfo{BucketName: bucket.Name})
		config, err := sys.Globals.BucketMetadataSys.GetNotificationConfig(bucket.Name)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		config.SetRegion(sys.Globals.ServerRegion)
		if err = config.Validate(sys.Globals.ServerRegion, sys.Globals.NotificationSys.targetList); err != nil {
			if _, ok := err.(*event.ErrARNNotFound); !ok {
				logger.LogIf(ctx, err)
			}
			continue
		}
		sys.AddRulesMap(bucket.Name, config.ToRulesMap())
	}
}

// Init - initializes notification system from notification.xml and listenxl.meta of all buckets.
func (sys *NotificationSys) Init(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, notifications are not supported - except NAS gateway.
	if sys.Globals.IsGateway && !objAPI.IsNotificationSupported() {
		return nil
	}

	logger.LogIf(ctx, sys.targetList.Add(sys.Globals.ConfigTargetList.Targets()...))

	go func() {
		for res := range sys.targetResCh {
			if res.Err != nil {
				reqInfo := &logger.ReqInfo{}
				reqInfo.AppendTags("targetID", res.ID.Name)
				logger.LogOnceIf(logger.SetReqInfo(GlobalContext, reqInfo), res.Err, res.ID)
			}
		}
	}()

	go sys.load(buckets)
	return nil
}

// AddRulesMap - adds rules map for bucket name.
func (sys *NotificationSys) AddRulesMap(bucketName string, rulesMap event.RulesMap) {
	sys.Lock()
	defer sys.Unlock()

	rulesMap = rulesMap.Clone()

	for _, targetRulesMap := range sys.bucketRemoteTargetRulesMap[bucketName] {
		rulesMap.Add(targetRulesMap)
	}

	// Do not add for an empty rulesMap.
	if len(rulesMap) == 0 {
		delete(sys.bucketRulesMap, bucketName)
	} else {
		sys.bucketRulesMap[bucketName] = rulesMap
	}
}

// ConfiguredTargetIDs - returns list of configured target id's
func (sys *NotificationSys) ConfiguredTargetIDs() []event.TargetID {
	if sys == nil {
		return nil
	}

	sys.RLock()
	defer sys.RUnlock()

	var targetIDs []event.TargetID
	for _, rmap := range sys.bucketRulesMap {
		for _, rules := range rmap {
			for _, targetSet := range rules {
				for id := range targetSet {
					targetIDs = append(targetIDs, id)
				}
			}
		}
	}
	// Filter out targets configured via env
	var tIDs []event.TargetID
	for _, targetID := range targetIDs {
		if !sys.Globals.EnvTargetList.Exists(targetID) {
			tIDs = append(tIDs, targetID)
		}
	}
	return tIDs
}

// RemoveAllRemoteTargets - closes and removes all notification targets.
func (sys *NotificationSys) RemoveAllRemoteTargets() {
	sys.Lock()
	defer sys.Unlock()

	for _, targetMap := range sys.bucketRemoteTargetRulesMap {
		targetIDSet := event.NewTargetIDSet()
		for k := range targetMap {
			targetIDSet[k] = struct{}{}
		}
		sys.targetList.Remove(targetIDSet)
	}
}

// Send - sends event data to all matching targets.
func (sys *NotificationSys) Send(args eventArgs) {
	sys.RLock()
	targetIDSet := sys.bucketRulesMap[args.BucketName].Match(args.EventName, args.Object.Name)
	sys.RUnlock()

	if len(targetIDSet) == 0 {
		return
	}

	sys.targetList.Send(args.ToEvent(true, sys.Globals), targetIDSet, sys.targetResCh)
}

// NewNotificationSys - creates new notification system object.
func NewNotificationSys(globals *Globals) *NotificationSys {
	return &NotificationSys{
		Globals:                    globals,
		targetList:                 event.NewTargetList(),
		targetResCh:                make(chan event.TargetIDResult),
		bucketRulesMap:             make(map[string]event.RulesMap),
		bucketRemoteTargetRulesMap: make(map[string]map[event.TargetID]event.RulesMap),
	}
}

// GetPeerOnlineCount gets the count of online and offline nodes.
func GetPeerOnlineCount() (nodesOnline, nodesOffline int) {
	nodesOnline = 1 // Self is always online.
	nodesOffline = 0
	return
}

type eventArgs struct {
	EventName    event.Name
	BucketName   string
	Object       ObjectInfo
	ReqParams    map[string]string
	RespElements map[string]string
	Host         string
	UserAgent    string
}

// ToEvent - converts to notification event.
func (args eventArgs) ToEvent(escape bool, globals *Globals) event.Event {
	eventTime := UTCNow()
	uniqueID := fmt.Sprintf("%X", eventTime.UnixNano())

	respElements := map[string]string{
		"x-amz-request-id": args.RespElements["requestId"],
	}
	// Add deployment as part of
	if globals.DeploymentID != "" {
		respElements["x-minio-deployment-id"] = globals.DeploymentID
	}
	if args.RespElements["content-length"] != "" {
		respElements["content-length"] = args.RespElements["content-length"]
	}
	keyName := args.Object.Name
	if escape {
		keyName = url.QueryEscape(args.Object.Name)
	}
	newEvent := event.Event{
		EventVersion:      "2.0",
		EventSource:       "minio:s3",
		AwsRegion:         args.ReqParams["region"],
		EventTime:         eventTime.Format(event.AMZTimeFormat),
		EventName:         args.EventName,
		UserIdentity:      event.Identity{PrincipalID: args.ReqParams["principalId"]},
		RequestParameters: args.ReqParams,
		ResponseElements:  respElements,
		S3: event.Metadata{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: event.Bucket{
				Name:          args.BucketName,
				OwnerIdentity: event.Identity{PrincipalID: args.ReqParams["principalId"]},
				ARN:           policy.ResourceARNPrefix + args.BucketName,
			},
			Object: event.Object{
				Key:       keyName,
				VersionID: args.Object.VersionID,
				Sequencer: uniqueID,
			},
		},
		Source: event.Source{
			Host:      args.Host,
			UserAgent: args.UserAgent,
		},
	}

	if args.EventName != event.ObjectRemovedDelete && args.EventName != event.ObjectRemovedDeleteMarkerCreated {
		newEvent.S3.Object.ETag = args.Object.ETag
		newEvent.S3.Object.Size = args.Object.Size
		newEvent.S3.Object.ContentType = args.Object.ContentType
		newEvent.S3.Object.UserMetadata = args.Object.UserDefined
	}

	return newEvent
}

func sendEvent(globals *Globals, args eventArgs) {
	args.Object.Size, _ = args.Object.GetActualSize()

	// avoid generating a notification for REPLICA creation event.
	if _, ok := args.ReqParams[xhttp.MinIOSourceReplicationRequest]; ok {
		return
	}
	// remove sensitive encryption entries in metadata.
	crypto.RemoveSensitiveEntries(args.Object.UserDefined)
	crypto.RemoveInternalEntries(args.Object.UserDefined)

	// globalNotificationSys is not initialized in gateway mode.
	if globals.NotificationSys == nil {
		return
	}

	if globals.HTTPListen.NumSubscribers() > 0 {
		globals.HTTPListen.Publish(args.ToEvent(false, globals))
	}

	globals.NotificationSys.Send(args)
}

// GetBandwidthReports - gets the bandwidth report from all nodes including self.
func (sys *NotificationSys) GetBandwidthReports(ctx context.Context, buckets ...string) bandwidth.Report {
	var reports []*bandwidth.Report
	reports = append(reports, sys.Globals.BucketMonitor.GetReport(bucketBandwidth.SelectBuckets(buckets...)))
	consolidatedReport := bandwidth.Report{
		BucketStats: make(map[string]bandwidth.Details),
	}
	for _, report := range reports {
		if report == nil || report.BucketStats == nil {
			continue
		}
		for bucket := range report.BucketStats {
			d, ok := consolidatedReport.BucketStats[bucket]
			if !ok {
				consolidatedReport.BucketStats[bucket] = bandwidth.Details{}
				d = consolidatedReport.BucketStats[bucket]
				d.LimitInBytesPerSecond = report.BucketStats[bucket].LimitInBytesPerSecond
			}
			if d.LimitInBytesPerSecond < report.BucketStats[bucket].LimitInBytesPerSecond {
				d.LimitInBytesPerSecond = report.BucketStats[bucket].LimitInBytesPerSecond
			}
			d.CurrentBandwidthInBytesPerSecond += report.BucketStats[bucket].CurrentBandwidthInBytesPerSecond
			consolidatedReport.BucketStats[bucket] = d
		}
	}
	return consolidatedReport
}
