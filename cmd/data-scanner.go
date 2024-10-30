/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/willf/bloom"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
)

const (
	dataScannerSleepPerFolder = time.Millisecond // Time to wait between folders.
	dataUsageUpdateDirCycles  = 16               // Visit all folders every n cycles.

	healDeleteDangling    = true
	healFolderIncludeProb = 32  // Include a clean folder one in n cycles.
	healObjectSelectProb  = 512 // Overall probability of a file being scanned; one in n.
)

var (
	dataScannerLeaderLockTimeout = newDynamicTimeout(30*time.Second, 10*time.Second)
	// Sleeper values are updated when config is loaded.
	scannerSleeper = newDynamicSleeper(10, 10*time.Second)
	scannerCycle   = &safeDuration{
		t: time.Minute, // Use a default, otherwise if scannerCycle.Update is never called, it will return a 0 time
	}
)

// initDataScanner will start the scanner in the background.
func initDataScanner(ctx context.Context, g *Globals, objAPI ObjectLayer) {
	go runDataScanner(ctx, g, objAPI)
}

type safeDuration struct {
	sync.Mutex
	t time.Duration
}

func (s *safeDuration) Update(t time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.t = t
}

func (s *safeDuration) Get() time.Duration {
	s.Lock()
	defer s.Unlock()
	return s.t
}

// runDataScanner will start a data scanner.
// The function will block until the context is canceled.
// There should only ever be one scanner running per cluster.
func runDataScanner(ctx context.Context, g *Globals, objAPI ObjectLayer) {
	var err error
	// Make sure only 1 scanner is running on the cluster.
	locker := objAPI.NewNSLock(minioMetaBucket, "runDataScanner.lock")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		ctx, err = locker.GetLock(ctx, dataScannerLeaderLockTimeout)
		if err != nil {
			time.Sleep(time.Duration(r.Float64() * float64(scannerCycle.Get())))
			continue
		}
		break
		// No unlock for "leader" lock.
	}

	// Load current bloom cycle
	nextBloomCycle := intDataUpdateTracker.current() + 1

	br, err := objAPI.GetObjectNInfo(ctx, dataUsageBucket, dataUsageBloomName, nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		if !isErrObjectNotFound(err) && !isErrBucketNotFound(err) {
			logger.LogIf(ctx, err)
		}
	} else {
		if br.ObjInfo.Size == 8 {
			if err = binary.Read(br, binary.LittleEndian, &nextBloomCycle); err != nil {
				logger.LogIf(ctx, err)
			}
		}
		br.Close()
	}

	scannerTimer := time.NewTimer(scannerCycle.Get())
	defer scannerTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-scannerTimer.C:
			// Reset the timer for next cycle.
			scannerTimer.Reset(scannerCycle.Get())

			if intDataUpdateTracker.debug {
				console.Debugln("starting scanner cycle")
			}

			// Wait before starting next cycle and wait on startup.
			results := make(chan madmin.DataUsageInfo, 1)
			go storeDataUsageInBackend(ctx, objAPI, results)
			bf, err := g.NotificationSys.updateBloomFilter(ctx, nextBloomCycle)
			logger.LogIf(ctx, err)
			err = objAPI.NSScanner(ctx, bf, results)
			close(results)
			logger.LogIf(ctx, err)
			if err == nil {
				// Store new cycle...
				nextBloomCycle++
				var tmp [8]byte
				binary.LittleEndian.PutUint64(tmp[:], nextBloomCycle)
				r, err := hash.NewReader(bytes.NewReader(tmp[:]), int64(len(tmp)), "", "", int64(len(tmp)))
				if err != nil {
					logger.LogIf(ctx, err)
					continue
				}

				_, err = objAPI.PutObject(ctx, dataUsageBucket, dataUsageBloomName, NewPutObjReader(r), ObjectOptions{})
				if !isErrBucketNotFound(err) {
					logger.LogIf(ctx, err)
				}
			}
		}
	}
}

type cachedFolder struct {
	name              string
	parent            *dataUsageHash
	objectHealProbDiv uint32
}

type folderScanner struct {
	root       string
	getSize    getSizeFn
	oldCache   dataUsageCache
	newCache   dataUsageCache
	withFilter *bloomFilter

	dataUsageScannerDebug bool
	healFolderInclude     uint32 // Include a clean folder one in n cycles.
	healObjectSelect      uint32 // Do a heal check on an object once every n cycles. Must divide into healFolderInclude

	newFolders      []cachedFolder
	existingFolders []cachedFolder
	disks           []StorageAPI
}

// scanDataFolder will scanner the basepath+cache.Info.Name and return an updated cache.
// The returned cache will always be valid, but may not be updated from the existing.
// Before each operation sleepDuration is called which can be used to temporarily halt the scanner.
// If the supplied context is canceled the function will return at the first chance.
func scanDataFolder(ctx context.Context, basePath string, cache dataUsageCache, getSize getSizeFn) (dataUsageCache, error) {
	t := UTCNow()

	logPrefix := color.Green("data-usage: ")
	logSuffix := color.Blue("- %v + %v", basePath, cache.Info.Name)
	if intDataUpdateTracker.debug {
		defer func() {
			console.Debugf(logPrefix+" Scanner time: %v %s\n", time.Since(t), logSuffix)
		}()

	}

	switch cache.Info.Name {
	case "", dataUsageRoot:
		return cache, errors.New("internal error: root scan attempted")
	}

	skipHeal := cache.Info.SkipHealing

	s := folderScanner{
		root:                  basePath,
		getSize:               getSize,
		oldCache:              cache,
		newCache:              dataUsageCache{Info: cache.Info},
		newFolders:            nil,
		existingFolders:       nil,
		dataUsageScannerDebug: intDataUpdateTracker.debug,
		healFolderInclude:     0,
		healObjectSelect:      0,
	}

	if len(cache.Info.BloomFilter) > 0 {
		s.withFilter = &bloomFilter{BloomFilter: &bloom.BloomFilter{}}
		_, err := s.withFilter.ReadFrom(bytes.NewReader(cache.Info.BloomFilter))
		if err != nil {
			logger.LogIf(ctx, err, logPrefix+"Error reading bloom filter")
			s.withFilter = nil
		}
	}
	if s.dataUsageScannerDebug {
		console.Debugf(logPrefix+"Start scanning. Bloom filter: %v %s\n", s.withFilter != nil, logSuffix)
	}

	done := ctx.Done()
	var flattenLevels = 2

	if s.dataUsageScannerDebug {
		console.Debugf(logPrefix+"Cycle: %v, Entries: %v %s\n", cache.Info.NextCycle, len(cache.Cache), logSuffix)
	}

	// Always scan flattenLevels deep. Cache root is level 0.
	todo := []cachedFolder{{name: cache.Info.Name, objectHealProbDiv: 1}}
	for i := 0; i < flattenLevels; i++ {
		if s.dataUsageScannerDebug {
			console.Debugf(logPrefix+"Level %v, scanning %v directories. %s\n", i, len(todo), logSuffix)
		}
		select {
		case <-done:
			return cache, ctx.Err()
		default:
		}
		var err error
		todo, err = s.scanQueuedLevels(ctx, todo, i == flattenLevels-1, skipHeal)
		if err != nil {
			// No useful information...
			return cache, err
		}
	}

	if s.dataUsageScannerDebug {
		console.Debugf(logPrefix+"New folders: %v %s\n", s.newFolders, logSuffix)
	}

	// Add new folders first
	for _, folder := range s.newFolders {
		select {
		case <-done:
			return s.newCache, ctx.Err()
		default:
		}
		du, err := s.deepScanFolder(ctx, folder, skipHeal)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		if du == nil {
			console.Debugln(logPrefix + "no disk usage provided" + logSuffix)
			continue
		}

		s.newCache.replace(folder.name, "", *du)
		// Add to parent manually
		if folder.parent != nil {
			parent := s.newCache.Cache[folder.parent.Key()]
			parent.addChildString(folder.name)
		}
	}

	if s.dataUsageScannerDebug {
		console.Debugf(logPrefix+"Existing folders: %v %s\n", len(s.existingFolders), logSuffix)
	}

	// Do selective scanning of existing folders.
	for _, folder := range s.existingFolders {
		select {
		case <-done:
			return s.newCache, ctx.Err()
		default:
		}
		h := hashPath(folder.name)
		if !h.mod(s.oldCache.Info.NextCycle, dataUsageUpdateDirCycles) {
			if !h.mod(s.oldCache.Info.NextCycle, s.healFolderInclude/folder.objectHealProbDiv) {
				s.newCache.replaceHashed(h, folder.parent, s.oldCache.Cache[h.Key()])
				continue
			} else {
				folder.objectHealProbDiv = s.healFolderInclude
			}
			folder.objectHealProbDiv = dataUsageUpdateDirCycles
		}
		if s.withFilter != nil {
			_, prefix := path2BucketObjectWithBasePath(basePath, folder.name)
			if s.oldCache.Info.lifeCycle == nil || !s.oldCache.Info.lifeCycle.HasActiveRules(prefix, true) {
				// If folder isn't in filter, skip it completely.
				if !s.withFilter.containsDir(folder.name) {
					if !h.mod(s.oldCache.Info.NextCycle, s.healFolderInclude/folder.objectHealProbDiv) {
						if s.dataUsageScannerDebug {
							console.Debugf(logPrefix+"Skipping non-updated folder: %v %s\n", folder, logSuffix)
						}
						s.newCache.replaceHashed(h, folder.parent, s.oldCache.Cache[h.Key()])
						continue
					} else {
						if s.dataUsageScannerDebug {
							console.Debugf(logPrefix+"Adding non-updated folder to heal check: %v %s\n", folder.name, logSuffix)
						}
						// Update probability of including objects
						folder.objectHealProbDiv = s.healFolderInclude
					}
				}
			}
		}

		// Update on this cycle...
		du, err := s.deepScanFolder(ctx, folder, skipHeal)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		if du == nil {
			logger.LogIf(ctx, errors.New("data-usage: no disk usage provided"))
			continue
		}
		s.newCache.replaceHashed(h, folder.parent, *du)
	}
	if s.dataUsageScannerDebug {
		console.Debugf(logPrefix+"Finished scanner, %v entries %s\n", len(s.newCache.Cache), logSuffix)
	}
	s.newCache.Info.LastUpdate = UTCNow()
	s.newCache.Info.NextCycle++
	return s.newCache, nil
}

// scanQueuedLevels will scan the provided folders.
// Files found in the folders will be added to f.newCache.
// If final is provided folders will be put into f.newFolders or f.existingFolders.
// If final is not provided the folders found are returned from the function.
func (f *folderScanner) scanQueuedLevels(ctx context.Context, folders []cachedFolder, final bool, skipHeal bool) ([]cachedFolder, error) {
	var nextFolders []cachedFolder
	done := ctx.Done()
	scannerLogPrefix := color.Green("folder-scanner:")
	for _, folder := range folders {
		select {
		case <-done:
			return nil, ctx.Err()
		default:
		}
		thisHash := hashPath(folder.name)
		existing := f.oldCache.findChildrenCopy(thisHash)

		// If there are lifecycle rules for the prefix, remove the filter.
		filter := f.withFilter
		_, prefix := path2BucketObjectWithBasePath(f.root, folder.name)
		var activeLifeCycle *lifecycle.Lifecycle
		if f.oldCache.Info.lifeCycle != nil && f.oldCache.Info.lifeCycle.HasActiveRules(prefix, true) {
			if f.dataUsageScannerDebug {
				console.Debugf(scannerLogPrefix+" Prefix %q has active rules\n", prefix)
			}
			activeLifeCycle = f.oldCache.Info.lifeCycle
			filter = nil
		}
		if _, ok := f.oldCache.Cache[thisHash.Key()]; filter != nil && ok {
			// If folder isn't in filter and we have data, skip it completely.
			if folder.name != dataUsageRoot && !filter.containsDir(folder.name) {
				if !thisHash.mod(f.oldCache.Info.NextCycle, f.healFolderInclude/folder.objectHealProbDiv) {
					f.newCache.copyWithChildren(&f.oldCache, thisHash, folder.parent)
					if f.dataUsageScannerDebug {
						console.Debugf(scannerLogPrefix+" Skipping non-updated folder: %v\n", folder.name)
					}
					continue
				} else {
					if f.dataUsageScannerDebug {
						console.Debugf(scannerLogPrefix+" Adding non-updated folder to heal check: %v\n", folder.name)
					}
					// If probability was already scannerHealFolderInclude, keep it.
					folder.objectHealProbDiv = f.healFolderInclude
				}
			}
		}
		scannerSleeper.Sleep(ctx, dataScannerSleepPerFolder)

		cache := dataUsageEntry{}

		err := readDirFn(path.Join(f.root, folder.name), func(entName string, typ os.FileMode) error {
			// Parse
			entName = pathClean(path.Join(folder.name, entName))
			if entName == "" {
				if f.dataUsageScannerDebug {
					console.Debugf(scannerLogPrefix+" no bucket (%s,%s)\n", f.root, entName)
				}
				return errDoneForNow
			}
			bucket, prefix := path2BucketObjectWithBasePath(f.root, entName)
			if bucket == "" {
				if f.dataUsageScannerDebug {
					console.Debugf(scannerLogPrefix+" no bucket (%s,%s)\n", f.root, entName)
				}
				return errDoneForNow
			}

			if isReservedOrInvalidBucket(bucket, false) {
				if f.dataUsageScannerDebug {
					console.Debugf(scannerLogPrefix+" invalid bucket: %v, entry: %v\n", bucket, entName)
				}
				return errDoneForNow
			}

			select {
			case <-done:
				return errDoneForNow
			default:
			}

			if typ&os.ModeDir != 0 {
				h := hashPath(entName)
				_, exists := f.oldCache.Cache[h.Key()]
				cache.addChildString(entName)

				this := cachedFolder{name: entName, parent: &thisHash, objectHealProbDiv: folder.objectHealProbDiv}

				delete(existing, h.Key()) // h.Key() already accounted for.

				cache.addChild(h)

				if final {
					if exists {
						f.existingFolders = append(f.existingFolders, this)
					} else {
						f.newFolders = append(f.newFolders, this)
					}
				} else {
					nextFolders = append(nextFolders, this)
				}
				return nil
			}

			// Dynamic time delay.
			wait := scannerSleeper.Timer(ctx)

			// Get file size, ignore errors.
			item := scannerItem{
				Path:       path.Join(f.root, entName),
				Typ:        typ,
				bucket:     bucket,
				prefix:     path.Dir(prefix),
				objectName: path.Base(entName),
				debug:      f.dataUsageScannerDebug,
				lifeCycle:  activeLifeCycle,
				heal:       false,
			}

			// if the drive belongs to an erasure set
			// that is already being healed, skip the
			// healing attempt on this drive.
			item.heal = item.heal && !skipHeal

			sizeSummary, err := f.getSize(item)
			if err == errSkipFile {
				wait() // wait to proceed to next entry.

				return nil
			}

			// successfully read means we have a valid object.

			// Remove filename i.e is the meta file to construct object name
			item.transformMetaDir()

			// Object already accounted for, remove from heal map,
			// simply because getSize() function already heals the
			// object.
			delete(existing, path.Join(item.bucket, item.objectPath()))

			cache.addSizes(sizeSummary)
			cache.Objects++
			cache.ObjSizes.add(sizeSummary.totalSize)

			wait() // wait to proceed to next entry.

			return nil
		})
		if err != nil {
			return nil, err
		}

		if f.healObjectSelect == 0 {
			// If we are not scanning, return now.
			f.newCache.replaceHashed(thisHash, folder.parent, cache)
			continue
		}

	}
	return nextFolders, nil
}

// deepScanFolder will deep scan a folder and return the size if no error occurs.
func (f *folderScanner) deepScanFolder(ctx context.Context, folder cachedFolder, skipHeal bool) (*dataUsageEntry, error) {
	var cache dataUsageEntry

	done := ctx.Done()

	var addDir func(entName string, typ os.FileMode) error
	var dirStack = []string{f.root, folder.name}

	deepScannerLogPrefix := color.Green("deep-scanner:")
	addDir = func(entName string, typ os.FileMode) error {
		select {
		case <-done:
			return errDoneForNow
		default:
		}

		if typ&os.ModeDir != 0 {
			dirStack = append(dirStack, entName)
			err := readDirFn(path.Join(dirStack...), addDir)
			dirStack = dirStack[:len(dirStack)-1]
			scannerSleeper.Sleep(ctx, dataScannerSleepPerFolder)
			return err
		}

		// Dynamic time delay.
		wait := scannerSleeper.Timer(ctx)

		// Get file size, ignore errors.
		dirStack = append(dirStack, entName)
		fileName := path.Join(dirStack...)
		dirStack = dirStack[:len(dirStack)-1]

		bucket, prefix := path2BucketObjectWithBasePath(f.root, fileName)
		var activeLifeCycle *lifecycle.Lifecycle
		if f.oldCache.Info.lifeCycle != nil && f.oldCache.Info.lifeCycle.HasActiveRules(prefix, false) {
			if f.dataUsageScannerDebug {
				console.Debugf(deepScannerLogPrefix+" Prefix %q has active rules\n", prefix)
			}
			activeLifeCycle = f.oldCache.Info.lifeCycle
		}

		item := scannerItem{
			Path:       fileName,
			Typ:        typ,
			bucket:     bucket,
			prefix:     path.Dir(prefix),
			objectName: path.Base(entName),
			debug:      f.dataUsageScannerDebug,
			lifeCycle:  activeLifeCycle,
			heal:       false,
		}

		// if the drive belongs to an erasure set
		// that is already being healed, skip the
		// healing attempt on this drive.
		item.heal = item.heal && !skipHeal

		sizeSummary, err := f.getSize(item)
		if err == errSkipFile {
			// Wait to throttle IO
			wait()

			return nil
		}

		logger.LogIf(ctx, err)
		cache.addSizes(sizeSummary)
		cache.Objects++
		cache.ObjSizes.add(sizeSummary.totalSize)

		// Wait to throttle IO
		wait()

		return nil
	}
	err := readDirFn(path.Join(dirStack...), addDir)
	if err != nil {
		return nil, err
	}
	return &cache, nil
}

// scannerItem represents each file while walking.
type scannerItem struct {
	Path string
	Typ  os.FileMode

	bucket     string // Bucket.
	prefix     string // Only the prefix if any, does not have final object name.
	objectName string // Only the object name without prefixes.
	lifeCycle  *lifecycle.Lifecycle
	heal       bool // Has the object been selected for heal check?
	debug      bool
}

type sizeSummary struct {
	totalSize      int64
	replicatedSize int64
	pendingSize    int64
	failedSize     int64
	replicaSize    int64
	pendingCount   uint64
	failedCount    uint64
}

type getSizeFn func(item scannerItem) (sizeSummary, error)

// transformMetaDir will transform a directory to prefix/file.ext
func (i *scannerItem) transformMetaDir() {
	split := strings.Split(i.prefix, SlashSeparator)
	if len(split) > 1 {
		i.prefix = path.Join(split[:len(split)-1]...)
	} else {
		i.prefix = ""
	}
	// Object name is last element
	i.objectName = split[len(split)-1]
}

// actionMeta contains information used to apply actions.
type actionMeta struct {
	oi         ObjectInfo
	bitRotScan bool // indicates if bitrot check was requested.
}

var applyActionsLogPrefix = color.Green("applyActions:")

// applyActions will apply lifecycle checks on to a scanned item.
// The resulting size on disk will always be returned.
// The metadata will be compared to consensus on the object layer before any changes are applied.
// If no metadata is supplied, -1 is returned if no action is taken.
func (i *scannerItem) applyActions(ctx context.Context, o ObjectLayer, meta actionMeta) int64 {
	size, _ := meta.oi.GetActualSize()
	return size
}

// objectPath returns the prefix and object name.
func (i *scannerItem) objectPath() string {
	return path.Join(i.prefix, i.objectName)
}

type dynamicSleeper struct {
	mu sync.RWMutex

	// Sleep factor
	factor float64

	// maximum sleep cap,
	// set to <= 0 to disable.
	maxSleep time.Duration

	// Don't sleep at all, if time taken is below this value.
	// This is to avoid too small costly sleeps.
	minSleep time.Duration

	// cycle will be closed
	cycle chan struct{}
}

// newDynamicSleeper
func newDynamicSleeper(factor float64, maxWait time.Duration) *dynamicSleeper {
	return &dynamicSleeper{
		factor:   factor,
		cycle:    make(chan struct{}),
		maxSleep: maxWait,
		minSleep: 100 * time.Microsecond,
	}
}

// Timer returns a timer that has started.
// When the returned function is called it will wait.
func (d *dynamicSleeper) Timer(ctx context.Context) func() {
	t := time.Now()
	return func() {
		doneAt := time.Now()
		for {
			// Grab current values
			d.mu.RLock()
			minWait, maxWait := d.minSleep, d.maxSleep
			factor := d.factor
			cycle := d.cycle
			d.mu.RUnlock()
			elapsed := doneAt.Sub(t)
			// Don't sleep for really small amount of time
			wantSleep := time.Duration(float64(elapsed) * factor)
			if wantSleep <= minWait {
				return
			}
			if maxWait > 0 && wantSleep > maxWait {
				wantSleep = maxWait
			}
			timer := time.NewTimer(wantSleep)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
				return
			case <-cycle:
				if !timer.Stop() {
					// We expired.
					<-timer.C
					return
				}
			}
		}
	}
}

// Sleep sleeps the specified time multiplied by the sleep factor.
// If the factor is updated the sleep will be done again with the new factor.
func (d *dynamicSleeper) Sleep(ctx context.Context, base time.Duration) {
	for {
		// Grab current values
		d.mu.RLock()
		minWait, maxWait := d.minSleep, d.maxSleep
		factor := d.factor
		cycle := d.cycle
		d.mu.RUnlock()
		// Don't sleep for really small amount of time
		wantSleep := time.Duration(float64(base) * factor)
		if wantSleep <= minWait {
			return
		}
		if maxWait > 0 && wantSleep > maxWait {
			wantSleep = maxWait
		}
		timer := time.NewTimer(wantSleep)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			return
		case <-cycle:
			if !timer.Stop() {
				// We expired.
				<-timer.C
				return
			}
		}
	}
}

// Update the current settings and cycle all waiting.
// Parameters are the same as in the contructor.
func (d *dynamicSleeper) Update(factor float64, maxWait time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if math.Abs(d.factor-factor) < 1e-10 && d.maxSleep == maxWait {
		return nil
	}
	// Update values and cycle waiting.
	close(d.cycle)
	d.factor = factor
	d.maxSleep = maxWait
	d.cycle = make(chan struct{})
	return nil
}
