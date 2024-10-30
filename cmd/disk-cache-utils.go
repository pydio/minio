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
	"container/list"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/minio/minio/cmd/crypto"
)

var timeSentinel = time.Unix(0, 0).UTC()

// CacheStatusType - whether the request was served from cache.
type CacheStatusType string

const (
	// CacheHit - whether object was served from cache.
	CacheHit CacheStatusType = "HIT"

	// CacheMiss - object served from backend.
	CacheMiss CacheStatusType = "MISS"
)

func (c CacheStatusType) String() string {
	if c != "" {
		return string(c)
	}
	return string(CacheMiss)
}

// IsCacheable returns if the object should be saved in the cache.
func (o ObjectInfo) IsCacheable() bool {
	_, ok := crypto.IsEncrypted(o.UserDefined)
	return !ok
}

type fileScorer struct {
	saveBytes uint64
	now       int64
	maxHits   int
	// 1/size for consistent score.
	sizeMult float64

	// queue is a linked list of files we want to delete.
	// The list is kept sorted according to score, highest at top, lowest at bottom.
	queue       list.List
	queuedBytes uint64
	seenBytes   uint64
}

type queuedFile struct {
	name      string
	versionID string
	size      uint64
	score     float64
}

// newFileScorer allows to collect files to save a specific number of bytes.
// Each file is assigned a score based on its age, size and number of hits.
// A list of files is maintained
func newFileScorer(saveBytes uint64, now int64, maxHits int) (*fileScorer, error) {
	if saveBytes == 0 {
		return nil, errors.New("newFileScorer: saveBytes = 0")
	}
	if now < 0 {
		return nil, errors.New("newFileScorer: now < 0")
	}
	if maxHits <= 0 {
		return nil, errors.New("newFileScorer: maxHits <= 0")
	}
	f := fileScorer{saveBytes: saveBytes, maxHits: maxHits, now: now, sizeMult: 1 / float64(saveBytes)}
	f.queue.Init()
	return &f, nil
}

func (f *fileScorer) addFile(name string, accTime time.Time, size int64, hits int) {
	f.addFileWithObjInfo(ObjectInfo{
		Name:    name,
		AccTime: accTime,
		Size:    size,
	}, hits)
}

func (f *fileScorer) addFileWithObjInfo(objInfo ObjectInfo, hits int) {
	// Calculate how much we want to delete this object.
	file := queuedFile{
		name:      objInfo.Name,
		versionID: objInfo.VersionID,
		size:      uint64(objInfo.Size),
	}
	f.seenBytes += uint64(objInfo.Size)

	var score float64
	if objInfo.ModTime.IsZero() {
		// Mod time is not available with disk cache use atime.
		score = float64(f.now - objInfo.AccTime.Unix())
	} else {
		// if not used mod time when mod time is available.
		score = float64(f.now - objInfo.ModTime.Unix())
	}

	// Size as fraction of how much we want to save, 0->1.
	szWeight := math.Max(0, (math.Min(1, float64(file.size)*f.sizeMult)))
	// 0 at f.maxHits, 1 at 0.
	hitsWeight := (1.0 - math.Max(0, math.Min(1.0, float64(hits)/float64(f.maxHits))))
	file.score = score * (1 + 0.25*szWeight + 0.25*hitsWeight)
	// If we still haven't saved enough, just add the file
	if f.queuedBytes < f.saveBytes {
		f.insertFile(file)
		f.trimQueue()
		return
	}
	// If we score less than the worst, don't insert.
	worstE := f.queue.Back()
	if worstE != nil && file.score < worstE.Value.(queuedFile).score {
		return
	}
	f.insertFile(file)
	f.trimQueue()
}

// adjustSaveBytes allows to adjust the number of bytes to save.
// This can be used to adjust the count on the fly.
// Returns true if there still is a need to delete files (n+saveBytes >0),
// false if no more bytes needs to be saved.
func (f *fileScorer) adjustSaveBytes(n int64) bool {
	if int64(f.saveBytes)+n <= 0 {
		f.saveBytes = 0
		f.trimQueue()
		return false
	}
	if n < 0 {
		f.saveBytes -= ^uint64(n - 1)
	} else {
		f.saveBytes += uint64(n)
	}
	if f.saveBytes == 0 {
		f.queue.Init()
		f.saveBytes = 0
		return false
	}
	if n < 0 {
		f.trimQueue()
	}
	return true
}

// insertFile will insert a file into the list, sorted by its score.
func (f *fileScorer) insertFile(file queuedFile) {
	e := f.queue.Front()
	for e != nil {
		v := e.Value.(queuedFile)
		if v.score < file.score {
			break
		}
		e = e.Next()
	}
	f.queuedBytes += file.size
	// We reached the end.
	if e == nil {
		f.queue.PushBack(file)
		return
	}
	f.queue.InsertBefore(file, e)
}

// trimQueue will trim the back of queue and still keep below wantSave.
func (f *fileScorer) trimQueue() {
	for {
		e := f.queue.Back()
		if e == nil {
			return
		}
		v := e.Value.(queuedFile)
		if f.queuedBytes-v.size < f.saveBytes {
			return
		}
		f.queue.Remove(e)
		f.queuedBytes -= v.size
	}
}

// fileObjInfos returns all queued file object infos
func (f *fileScorer) fileObjInfos() []ObjectInfo {
	res := make([]ObjectInfo, 0, f.queue.Len())
	e := f.queue.Front()
	for e != nil {
		qfile := e.Value.(queuedFile)
		res = append(res, ObjectInfo{
			Name:      qfile.name,
			Size:      int64(qfile.size),
			VersionID: qfile.versionID,
		})
		e = e.Next()
	}
	return res
}

func (f *fileScorer) purgeFunc(p func(qfile queuedFile)) {
	e := f.queue.Front()
	for e != nil {
		p(e.Value.(queuedFile))
		e = e.Next()
	}
}

// fileNames returns all queued file names.
func (f *fileScorer) fileNames() []string {
	res := make([]string, 0, f.queue.Len())
	e := f.queue.Front()
	for e != nil {
		res = append(res, e.Value.(queuedFile).name)
		e = e.Next()
	}
	return res
}

func (f *fileScorer) reset() {
	f.queue.Init()
	f.queuedBytes = 0
}

func (f *fileScorer) queueString() string {
	var res strings.Builder
	e := f.queue.Front()
	i := 0
	for e != nil {
		v := e.Value.(queuedFile)
		if i > 0 {
			res.WriteByte('\n')
		}
		res.WriteString(fmt.Sprintf("%03d: %s (score: %.3f, bytes: %d)", i, v.name, v.score, v.size))
		i++
		e = e.Next()
	}
	return res.String()
}

// bytesToClear() returns the number of bytes to clear to reach low watermark
// w.r.t quota given disk total and free space, quota in % allocated to cache
// and low watermark % w.r.t allowed quota.
// If the high watermark hasn't been reached 0 will be returned.
func bytesToClear(total, free int64, quotaPct, lowWatermark, highWatermark uint64) uint64 {
	used := total - free
	quotaAllowed := total * (int64)(quotaPct) / 100
	highWMUsage := total * (int64)(highWatermark*quotaPct) / (100 * 100)
	if used < highWMUsage {
		return 0
	}
	// Return bytes needed to reach low watermark.
	lowWMUsage := total * (int64)(lowWatermark*quotaPct) / (100 * 100)
	return (uint64)(math.Min(float64(quotaAllowed), math.Max(0.0, float64(used-lowWMUsage))))
}
