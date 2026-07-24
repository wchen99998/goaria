package goaria

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type httpStatusError struct {
	Method     string
	URL        string
	StatusCode int
	Status     string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.Method, e.URL, e.Status)
}

type rangeBodyError struct {
	err       error
	completed int64
}

func (e *rangeBodyError) Error() string {
	return e.err.Error()
}

func (e *rangeBodyError) Unwrap() error {
	return e.err
}

var copyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 32*1024)
		return &buf
	},
}

const largeCopyBufferSize = 256 * 1024

var largeCopyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, largeCopyBufferSize)
		return &buf
	},
}

const smallCopyBufferSize = 4 * 1024

var smallCopyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, smallCopyBufferSize)
		return &buf
	},
}

const (
	metadataProbeRangeEnd  = 1023
	defaultHTTPSegmentSize = 50 * 1024 * 1024
	maxHTTPSegmentCount    = 64 * 1024
)

type remoteMeta struct {
	Length       int64
	AcceptRange  bool
	FinalURI     string
	Filename     string
	LastModified string
	ETag         string
	NotModified  bool
}

type chunkRange struct {
	start int64
	end   int64
}

type segmentedRangeTask struct {
	index            int
	r                chunkRange
	attempts         int
	notBefore        time.Time
	launchLimit      int
	launchGeneration int
}

type segmentedRangeResult struct {
	task      segmentedRangeTask
	err       error
	maxTries  int
	retryWait time.Duration
}

type segmentedConcurrencyController struct {
	effectiveLimit     int
	successesAtLimit   int
	rejectedProbeLimit int
	generation         int
}

func newSegmentedConcurrencyController(ceiling int) *segmentedConcurrencyController {
	c := &segmentedConcurrencyController{effectiveLimit: 1}
	c.clampCeiling(ceiling)
	return c
}

func (c *segmentedConcurrencyController) limit() int {
	if c.effectiveLimit < 1 {
		return 1
	}
	return c.effectiveLimit
}

func (c *segmentedConcurrencyController) launchState() (int, int) {
	return c.limit(), c.generation
}

func (c *segmentedConcurrencyController) clampCeiling(ceiling int) {
	ceiling = c.adjustedCeiling(ceiling)
	oldLimit := c.effectiveLimit
	if c.effectiveLimit < 1 {
		c.effectiveLimit = 1
	}
	if c.effectiveLimit > ceiling {
		c.effectiveLimit = ceiling
	}
	if c.effectiveLimit != oldLimit {
		c.successesAtLimit = 0
		c.generation++
	}
}

func (c *segmentedConcurrencyController) onSuccess(launchLimit, launchGeneration, ceiling int) {
	c.clampCeiling(ceiling)
	if launchGeneration != c.generation || launchLimit != c.effectiveLimit {
		return
	}
	c.successesAtLimit++
	if c.successesAtLimit < c.effectiveLimit {
		return
	}
	c.successesAtLimit = 0
	ceiling = c.adjustedCeiling(ceiling)
	if c.effectiveLimit < ceiling {
		c.effectiveLimit++
		c.generation++
	}
}

func (c *segmentedConcurrencyController) onThrottle(launchLimit, launchGeneration, ceiling int) {
	c.clampCeiling(ceiling)
	if launchGeneration != c.generation {
		return
	}
	if launchLimit < 1 {
		launchLimit = c.limit()
	}
	if launchLimit > 1 && (c.rejectedProbeLimit == 0 || launchLimit < c.rejectedProbeLimit) {
		c.rejectedProbeLimit = launchLimit
	}
	nextLimit := launchLimit - 1
	if nextLimit < 1 {
		nextLimit = 1
	}
	ceiling = c.adjustedCeiling(ceiling)
	if nextLimit > ceiling {
		nextLimit = ceiling
	}
	c.effectiveLimit = nextLimit
	c.successesAtLimit = 0
	c.generation++
}

func (c *segmentedConcurrencyController) onRetryableError(launchLimit, launchGeneration, ceiling int) {
	c.clampCeiling(ceiling)
	if launchGeneration != c.generation || launchLimit != c.effectiveLimit {
		return
	}
	c.successesAtLimit = 0
	c.generation++
}

func (c *segmentedConcurrencyController) adjustedCeiling(ceiling int) int {
	if ceiling < 1 {
		ceiling = 1
	}
	if c.rejectedProbeLimit > 0 && ceiling >= c.rejectedProbeLimit {
		ceiling = c.rejectedProbeLimit - 1
		if ceiling < 1 {
			ceiling = 1
		}
	}
	return ceiling
}

type downloadSpeedTracker struct {
	d     *Download
	bytes atomic.Int64
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once
}

func startDownloadSpeedTracker(ctx context.Context, d *Download) *downloadSpeedTracker {
	return startDownloadSpeedTrackerWithInterval(ctx, d, time.Second)
}

func startDownloadSpeedTrackerWithInterval(ctx context.Context, d *Download, interval time.Duration) *downloadSpeedTracker {
	if interval <= 0 {
		interval = time.Second
	}
	t := &downloadSpeedTracker{
		d:    d,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go t.run(ctx, interval)
	return t
}

func (t *downloadSpeedTracker) add(n int64) {
	if t != nil && n > 0 {
		t.bytes.Add(n)
	}
}

func (t *downloadSpeedTracker) stopAndWait() {
	if t == nil {
		return
	}
	t.once.Do(func() {
		close(t.stop)
		<-t.done
	})
}

func (t *downloadSpeedTracker) run(ctx context.Context, interval time.Duration) {
	defer close(t.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastBytes := int64(0)
	lastAt := time.Now()
	update := func(now time.Time) {
		current := t.bytes.Load()
		elapsed := now.Sub(lastAt)
		if elapsed <= 0 {
			return
		}
		delta := current - lastBytes
		if delta < 0 {
			delta = 0
		}
		t.d.setDownloadBPS(bytesPerSecond(delta, elapsed))
		lastBytes = current
		lastAt = now
	}
	for {
		select {
		case now := <-ticker.C:
			update(now)
		case <-ctx.Done():
			update(time.Now())
			return
		case <-t.stop:
			update(time.Now())
			return
		}
	}
}

type rateLimiter struct {
	mu        sync.Mutex
	limit     int64
	limitFunc func() int64
	next      time.Time
	parent    *rateLimiter
}

func newRateLimiter(limit int64) *rateLimiter {
	if limit <= 0 {
		return nil
	}
	return &rateLimiter{limit: limit}
}

func newDynamicRateLimiter(limit func() int64) *rateLimiter {
	return &rateLimiter{limitFunc: limit}
}

func (l *rateLimiter) wait(ctx context.Context, n int64) error {
	if l == nil || n <= 0 {
		return nil
	}
	for n > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := l.reservationChunk(n)
		deadline := l.reservationDeadline(time.Now(), chunk)
		if delay := time.Until(deadline); delay > 0 {
			if err := sleepContext(ctx, delay); err != nil {
				return err
			}
		}
		n -= chunk
	}
	return nil
}

func (l *rateLimiter) reservationChunk(n int64) int64 {
	chunk := n
	for current := l; current != nil; current = current.parent {
		var limit int64
		if current.limitFunc != nil {
			limit = current.limitFunc()
		} else {
			current.mu.Lock()
			limit = current.limit
			current.mu.Unlock()
		}
		if limit <= 0 {
			continue
		}
		// Bound a canceled reservation's effect on other users of a parent
		// limiter. At sub-10 B/s the minimum one-byte reservation is at most 1s.
		candidate := limit / 10
		if candidate < 1 {
			candidate = 1
		}
		if candidate < chunk {
			chunk = candidate
		}
	}
	return chunk
}

func (l *rateLimiter) reservationDeadline(now time.Time, n int64) time.Time {
	deadline := now
	for current := l; current != nil; current = current.parent {
		if reserved := current.reserve(now, n); reserved.After(deadline) {
			deadline = reserved
		}
	}
	return deadline
}

func (l *rateLimiter) reserve(now time.Time, n int64) time.Time {
	dynamic := l.limitFunc != nil
	dynamicLimit := int64(0)
	if dynamic {
		dynamicLimit = l.limitFunc()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	limit := l.limit
	if dynamic {
		limit = dynamicLimit
	}
	if dynamic && limit != l.limit {
		l.limit = limit
		l.next = now
	}
	if limit <= 0 {
		return now
	}
	if l.next.Before(now) {
		l.next = now
	}
	l.next = l.next.Add(rateLimitDuration(n, limit))
	return l.next
}

func rateLimitDuration(n, limit int64) time.Duration {
	if n <= 0 || limit <= 0 {
		return 0
	}
	nanoseconds := math.Ceil(float64(n) * float64(time.Second) / float64(limit))
	if nanoseconds >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	if nanoseconds < 1 {
		return 1
	}
	return time.Duration(nanoseconds)
}

func bytesPerSecond(n int64, elapsed time.Duration) int64 {
	if n <= 0 || elapsed <= 0 {
		return 0
	}
	rate := float64(n) * float64(time.Second) / float64(elapsed)
	if rate >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(rate)
}

func (e *Engine) runDownload(d *Download) error {
	d.mu.RLock()
	kind := d.kind
	d.mu.RUnlock()
	if kind == downloadKindTorrent {
		return e.runTorrentDownload(d)
	}

	var oneURI [1]URIInfo
	var uris []URIInfo
	d.mu.RLock()
	ctx := d.ctx
	opts := d.options
	if len(d.uris) == 1 {
		oneURI[0] = d.uris[0]
		uris = oneURI[:]
	} else {
		uris = cloneURIs(d.uris)
	}
	d.mu.RUnlock()
	if ctx == nil {
		return context.Canceled
	}
	maxTries := optionInt(opts, "max-tries", 5)
	if maxTries < 0 {
		maxTries = 1
	}
	retryWait := time.Duration(optionInt(opts, "retry-wait", 0)) * time.Second
	maxNotFound := optionInt(opts, "max-file-not-found", 0)
	var lastErr error
	fileNotFound := 0
	attempt := 0
	for maxTries == 0 || attempt < maxTries {
		roundRetryable := false
		for _, u := range uris {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if maxTries > 0 && attempt >= maxTries {
				break
			}
			attempt++
			if len(uris) > 1 {
				d.setURIUsed(u.URI)
			}
			err := e.downloadURI(ctx, d, u.URI, opts)
			if err == nil {
				return nil
			}
			lastErr = err
			if isFileNotFoundError(err) {
				fileNotFound++
				if maxNotFound > 0 && fileNotFound >= maxNotFound {
					return err
				}
			}
			if ce := e.log.Check(zap.WarnLevel, "download URI failed"); ce != nil {
				ce.Write(zap.String("gid", d.gid), zap.String("uri", u.URI), zap.Int("attempt", attempt), zap.Error(err))
			}
			if isRetryableDownloadError(err) {
				roundRetryable = true
			}
			if retryWait > 0 && isRetryableDownloadError(err) && (maxTries == 0 || attempt < maxTries) {
				if err := sleepContext(ctx, retryWait); err != nil {
					return err
				}
			}
		}
		if !roundRetryable {
			break
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no usable URI")
	}
	return lastErr
}

func (e *Engine) downloadURI(ctx context.Context, d *Download, rawURI string, opts Options) error {
	split := optionInt(opts, "split", 1)
	if split < 1 {
		split = 1
	}
	maxConn := optionInt(opts, "max-connection-per-server", split)
	if maxConn < 1 {
		maxConn = 1
	}
	initialConcurrency := minInt(split, maxConn)
	segmentedRequested := split > 1
	minSplit := optionBytes(opts, "min-split-size", 1<<20)
	limiter := newDynamicRateLimiter(func() int64 {
		return optionBytes(d.optionSnapshot(), "max-download-limit", 0)
	})
	limiter.parent = e.overallLimiter
	dirOpt := optionString(opts, "dir")
	outOpt := optionString(opts, "out")

	if canUseSingleGETProbe(opts, segmentedRequested, initialConcurrency) {
		meta, outPath, err := e.downloadSingleWithGETProbe(ctx, d, rawURI, opts, dirOpt, outOpt, limiter)
		if err != nil {
			return err
		}
		if meta.NotModified {
			return nil
		}
		if err := verifyChecksum(outPath, opts); err != nil {
			return err
		}
		return applyRemoteTime(outPath, meta, opts)
	}

	provisionalName := filenameFromURI(rawURI)
	provisionalPath := resolveOutputPath(dirOpt, outOpt, provisionalName, rawURI)
	probeOpts := opts
	if segmentedRequested {
		probeOpts = transportOptionsForSegmented(opts)
	}
	meta, err := e.probe(ctx, rawURI, probeOpts, provisionalPath)
	if err != nil {
		return err
	}
	outPath := provisionalPath
	if outOpt == "" && meta.Filename != "" && meta.Filename != provisionalName {
		outPath = resolveOutputPath(dirOpt, "", meta.Filename, rawURI)
	}
	if optionBool(opts, "dry-run") || meta.NotModified {
		d.setMetadata(meta, outPath)
		return nil
	}
	selectedPath := d.outputPath()
	segmented := meta.Length > 0 && meta.AcceptRange && segmentedRequested &&
		meetsSegmentedMinimum(meta.Length, minSplit, optionExplicit(opts, "min-split-size"))
	if selectedPath != "" {
		outPath = selectedPath
	} else {
		outPath, err = resolveExistingOutputPath(outPath, opts, canResumeExistingOutput(outPath, opts, meta, segmented))
		if err != nil {
			return err
		}
	}
	if err := e.ensureDownloadDir(filepath.Dir(outPath)); err != nil {
		return err
	}
	autoRename := selectedPath == "" && !canResumeExistingOutput(outPath, opts, meta, segmented) &&
		!optionBool(opts, "allow-overwrite") && optionBoolDefault(opts, "auto-file-renaming", true)
	outPath, err = e.reserveOutputPath(outPath, d.gid, autoRename)
	if err != nil {
		return err
	}

	d.setMetadata(meta, outPath)
	if segmented {
		opts = transportOptionsForSegmented(opts)
		chunks := makeChunks(meta.Length, httpSegmentSize(opts, minSplit))
		err = e.downloadSegmented(ctx, d, meta, outPath, opts, chunks, limiter)
		if shouldDowngradeSegmentedDownload(err) {
			// A server can advertise ranges and then ignore Range, most notably
			// when an If-Range validator no longer matches. Restarting one full
			// transfer is safe because downloadSingle truncates the preallocated
			// segmented destination before writing.
			meta.AcceptRange = false
			err = e.downloadSingle(ctx, d, meta, outPath, opts, limiter)
		}
	} else {
		err = e.downloadSingle(ctx, d, meta, outPath, opts, limiter)
	}
	if err != nil {
		return err
	}
	if err := verifyChecksum(outPath, opts); err != nil {
		return err
	}
	return applyRemoteTime(outPath, meta, opts)
}

func meetsSegmentedMinimum(length, minSplit int64, explicit bool) bool {
	if length <= 0 || minSplit < 0 || minSplit > math.MaxInt64/2 {
		return false
	}
	minimum := minSplit * 2
	if !explicit && minimum < 8<<20 {
		minimum = 8 << 20
	}
	return length >= minimum
}

func shouldDowngradeSegmentedDownload(err error) bool {
	var status *httpStatusError
	return errors.As(err, &status) && status.StatusCode == http.StatusOK
}

func canUseSingleGETProbe(opts Options, segmentedRequested bool, initialConcurrency int) bool {
	if optionBool(opts, "dry-run") {
		return false
	}
	useSingleGET := optionString(opts, "out") != "" || !optionBool(opts, "continue")
	if !segmentedRequested {
		return useSingleGET
	}
	if initialConcurrency > 1 {
		return false
	}
	if optionBool(opts, "http-accept-gzip") {
		return useSingleGET
	}
	if optionString(opts, "out") != "" && optionBool(opts, "continue") {
		return true
	}
	return false
}

func transportOptionsForSegmented(opts Options) Options {
	if normalizeHTTPVersion(optionString(opts, "http-version")) != "auto" {
		return opts
	}
	out := cloneOptions(opts)
	out["http-version"] = "1.1"
	return out
}

func httpSegmentSize(opts Options, minSplit int64) int64 {
	size := optionBytes(opts, "goaria-http-segment-size", defaultHTTPSegmentSize)
	if size <= 0 {
		size = defaultHTTPSegmentSize
	}
	if minSplit > size {
		return minSplit
	}
	return size
}

func (e *Engine) probe(ctx context.Context, rawURI string, opts Options, conditionalPath string) (remoteMeta, error) {
	var headMeta remoteMeta
	headMetaOK := false
	if optionBoolDefault(opts, "use-head", true) {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURI, nil)
		if err != nil {
			return remoteMeta{}, err
		}
		applyMetadataRequestOptions(req, opts)
		if !optionBool(opts, "continue") {
			applyConditionalHeaders(req, opts, conditionalPath)
		}
		resp, err := e.do(req, opts)
		if err == nil {
			if resp.StatusCode == http.StatusNotModified {
				_ = resp.Body.Close()
				return notModifiedMeta(rawURI, conditionalPath), nil
			}
			if isHTTPTransferSuccess(resp.StatusCode) {
				meta := metaFromResponse(resp, rawURI, opts)
				if resp.ContentLength >= 0 {
					_ = resp.Body.Close()
					return meta, nil
				}
				headMeta = meta
				headMetaOK = true
			} else if resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusNotImplemented && resp.StatusCode != http.StatusForbidden {
				_ = resp.Body.Close()
				return remoteMeta{}, &httpStatusError{Method: http.MethodHead, URL: rawURI, StatusCode: resp.StatusCode, Status: resp.Status}
			}
			_ = resp.Body.Close()
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURI, nil)
	if err != nil {
		return remoteMeta{}, err
	}
	applyRangeRequestOptions(req, opts, fmt.Sprintf("bytes=0-%d", metadataProbeRangeEnd))
	if !optionBool(opts, "continue") {
		applyConditionalHeaders(req, opts, conditionalPath)
	}
	resp, err := e.do(req, opts)
	if err != nil {
		if headMetaOK {
			return headMeta, nil
		}
		return remoteMeta{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return notModifiedMeta(rawURI, conditionalPath), nil
	}
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		if total, ok := completeLengthFromUnsatisfiedRange(resp); ok {
			meta := metaFromResponse(resp, rawURI, opts)
			meta.Length = total
			meta.AcceptRange = true
			return meta, nil
		}
	}
	if !isHTTPTransferSuccess(resp.StatusCode) {
		if headMetaOK {
			return headMeta, nil
		}
		return remoteMeta{}, &httpStatusError{Method: http.MethodGet, URL: rawURI, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	meta := metaFromResponse(resp, rawURI, opts)
	if resp.StatusCode == http.StatusPartialContent {
		cr, err := validateProbeRangeResponse(resp, 0, metadataProbeRangeEnd)
		if err != nil {
			if headMetaOK {
				return headMeta, nil
			}
			return remoteMeta{}, err
		}
		meta.AcceptRange = true
		if cr.totalKnown {
			meta.Length = cr.total
		} else {
			meta.Length = 0
		}
	} else {
		meta.AcceptRange = false
	}
	return meta, nil
}

func (e *Engine) downloadSegmented(ctx context.Context, d *Download, meta remoteMeta, path string, opts Options, chunks []chunkRange, limiter *rateLimiter) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := file.Truncate(meta.Length); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	d.resetProgress(meta.Length, chunks)
	tracker := startDownloadSpeedTracker(ctx, d)
	defer tracker.stopAndWait()

	segCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	tasks := make([]segmentedRangeTask, 0, len(chunks))
	for i, ch := range chunks {
		tasks = append(tasks, segmentedRangeTask{index: i, r: ch})
	}

	results := make(chan segmentedRangeResult, 1)
	var wg sync.WaitGroup
	active := 0
	controller := newSegmentedConcurrencyController(e.segmentedWorkerLimit(d, opts, len(chunks)))
	var firstErr error
	remainingChunks := func(includeResult bool) int {
		remaining := active + len(tasks)
		if includeResult {
			remaining++
		}
		return remaining
	}

	startTask := func(task segmentedRangeTask) {
		rangeOpts := transportOptionsForSegmented(d.optionSnapshot())
		maxTries := optionInt(rangeOpts, "max-tries", 5)
		if maxTries < 0 {
			maxTries = 1
		}
		retryWait := time.Duration(optionInt(rangeOpts, "retry-wait", 0)) * time.Second
		task.attempts++
		task.launchLimit, task.launchGeneration = controller.launchState()
		active++
		d.setConnections(active)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := e.downloadRange(segCtx, d, meta, path, rangeOpts, task.r, limiter, tracker)
			results <- segmentedRangeResult{task: task, err: err, maxTries: maxTries, retryWait: retryWait}
		}()
	}

	startReady := func(now time.Time) {
		if firstErr != nil {
			return
		}
		ceiling := e.segmentedWorkerLimit(d, opts, remainingChunks(false))
		controller.clampCeiling(ceiling)
		for active < controller.limit() {
			idx := readySegmentedTaskIndex(tasks, now)
			if idx < 0 {
				return
			}
			task := tasks[idx]
			tasks = append(tasks[:idx], tasks[idx+1:]...)
			startTask(task)
		}
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	ctxDone := ctx.Done()
	for {
		startReady(time.Now())
		if len(tasks) == 0 && active == 0 {
			break
		}
		if firstErr != nil && active == 0 {
			break
		}
		select {
		case result := <-results:
			active--
			d.setConnections(active)
			if result.err == nil {
				d.markPieceComplete(result.task.index)
				controller.onSuccess(result.task.launchLimit, result.task.launchGeneration, e.segmentedWorkerLimit(d, opts, remainingChunks(false)))
				continue
			}
			var bodyErr *rangeBodyError
			if errors.As(result.err, &bodyErr) {
				d.rollbackCompleted(bodyErr.completed)
			}
			if isAdaptiveRangeThrottleError(result.err) {
				canRetry := canRetryRangeAttempt(result.task.attempts, result.maxTries)
				controller.onThrottle(result.task.launchLimit, result.task.launchGeneration, e.segmentedWorkerLimit(d, opts, remainingChunks(canRetry)))
				if canRetry {
					result.task.notBefore = time.Now().Add(result.retryWait)
					tasks = append(tasks, result.task)
					continue
				}
			} else if isRetryableRangeError(result.err) {
				canRetry := canRetryRangeAttempt(result.task.attempts, result.maxTries)
				if canRetry {
					controller.onRetryableError(result.task.launchLimit, result.task.launchGeneration, e.segmentedWorkerLimit(d, opts, remainingChunks(true)))
					result.task.notBefore = time.Now().Add(result.retryWait)
					tasks = append(tasks, result.task)
					continue
				}
			}
			if firstErr == nil {
				firstErr = result.err
				cancel()
			}
		case <-ticker.C:
		case <-ctxDone:
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			cancel()
			ctxDone = nil
		}
	}
	cancel()
	wg.Wait()
	d.setConnections(0)
	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func readySegmentedTaskIndex(tasks []segmentedRangeTask, now time.Time) int {
	for i, task := range tasks {
		if task.notBefore.IsZero() || !task.notBefore.After(now) {
			return i
		}
	}
	return -1
}

func canRetryRangeAttempt(attempts, maxTries int) bool {
	if maxTries == 0 {
		return true
	}
	if maxTries < 0 {
		maxTries = 1
	}
	return attempts < maxTries
}

func (e *Engine) segmentedWorkerLimit(d *Download, fallback Options, max int) int {
	if max < 1 {
		max = 1
	}
	opts := e.dynamicSegmentedOptionSnapshot(d)
	if opts == nil {
		opts = fallback
	}
	split := optionInt(opts, "split", optionInt(fallback, "split", 1))
	if split < 1 {
		split = 1
	}
	maxConn := optionInt(opts, "max-connection-per-server", split)
	if maxConn < 1 {
		maxConn = 1
	}
	limit := minInt(split, maxConn)
	if limit > max {
		limit = max
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

func (e *Engine) dynamicSegmentedOptionSnapshot(d *Download) Options {
	opts := d.optionSnapshot()
	e.mu.RLock()
	global := cloneOptions(e.global)
	e.mu.RUnlock()
	for _, key := range []string{"split", "max-connection-per-server"} {
		if !optionExplicit(opts, key) {
			opts[key] = normalizeOptionValue(optionString(global, key))
		}
	}
	return opts
}

func isRetryableRangeError(err error) bool {
	return isRetryableDownloadError(err)
}

func isAdaptiveRangeThrottleError(err error) bool {
	var status *httpStatusError
	if !errors.As(err, &status) {
		return false
	}
	if status.StatusCode == http.StatusForbidden || status.StatusCode == http.StatusTooManyRequests {
		return true
	}
	return status.StatusCode >= http.StatusInternalServerError && status.StatusCode <= 599
}

func (e *Engine) downloadRange(ctx context.Context, d *Download, meta remoteMeta, path string, opts Options, r chunkRange, limiter *rateLimiter, tracker *downloadSpeedTracker) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, meta.FinalURI, nil)
	if err != nil {
		return err
	}
	applyRangeRequestOptions(req, opts, fmt.Sprintf("bytes=%d-%d", r.start, r.end))
	applyIfRange(req, meta)
	resp, err := e.do(req, opts)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return &httpStatusError{Method: http.MethodGet, URL: meta.FinalURI, StatusCode: resp.StatusCode, Status: fmt.Sprintf("range %d-%d: %s", r.start, r.end, resp.Status)}
	}
	if _, err := validateExactRangeResponse(resp, r.start, r.end, meta.Length); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Seek(r.start, io.SeekStart); err != nil {
		return errors.Join(err, file.Close())
	}
	completed, err := copyExactWithProgressCount(ctx, file, resp.Body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0), r.end-r.start+1, tracker)
	closeErr := file.Close()
	if err != nil {
		return &rangeBodyError{err: errors.Join(err, closeErr), completed: completed}
	}
	return closeErr
}

func (e *Engine) downloadSingle(ctx context.Context, d *Download, meta remoteMeta, path string, opts Options, limiter *rateLimiter) (retErr error) {
	start := int64(0)
	if optionBool(opts, "continue") && meta.Length > 0 {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 && st.Size() < meta.Length && meta.AcceptRange {
			start = st.Size()
		}
	}
	flag := os.O_CREATE | os.O_WRONLY
	if start > 0 {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	file, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if file != nil {
			retErr = errors.Join(retErr, file.Close())
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, meta.FinalURI, nil)
	if err != nil {
		return err
	}
	if start > 0 {
		applyRangeRequestOptions(req, opts, fmt.Sprintf("bytes=%d-", start))
		applyIfRange(req, meta)
	} else {
		applyRequestOptions(req, opts)
	}
	resp, err := e.do(req, opts)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if start > 0 && resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		if total, ok := completeLengthFromUnsatisfiedRange(resp); ok && total == start && (meta.Length <= 0 || total == meta.Length) {
			d.resetSingleProgress(total, total)
			return nil
		}
	}
	if !isHTTPTransferSuccess(resp.StatusCode) || !isHTTPBodySuccess(resp.StatusCode) {
		return &httpStatusError{Method: http.MethodGet, URL: meta.FinalURI, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	rangeLength := int64(-1)
	if start > 0 && resp.StatusCode != http.StatusPartialContent {
		start = 0
		if err := file.Truncate(0); err != nil {
			return err
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
	} else if start > 0 {
		cr, err := validateOpenRangeResponse(resp, start, meta.Length)
		if err != nil {
			return err
		}
		rangeLength = cr.end - cr.start + 1
	} else if resp.StatusCode == http.StatusPartialContent {
		return newHTTPProtocolError("unexpected partial response without an internal Range request")
	} else {
		respMeta := metaFromResponse(resp, meta.FinalURI, opts)
		if resp.ContentLength >= 0 || responseBodyDecoded(resp, opts) || meta.Length <= 0 {
			meta.Length = respMeta.Length
		}
		meta.LastModified = respMeta.LastModified
		meta.FinalURI = respMeta.FinalURI
		meta.AcceptRange = respMeta.AcceptRange
		d.setMetadata(meta, path)
	}
	d.resetSingleProgress(meta.Length, start)
	d.setConnections(1)
	var body io.Reader = resp.Body
	closeBody := func() {}
	if rangeLength < 0 {
		var err error
		body, closeBody, err = responseReader(resp, opts)
		if err != nil {
			d.setConnections(0)
			return err
		}
	}
	defer closeBody()
	tracker := startDownloadSpeedTracker(ctx, d)
	if rangeLength >= 0 {
		err = copyExactWithProgress(ctx, file, body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0), rangeLength, tracker)
	} else {
		var n int64
		n, err = copyCountingWithProgress(ctx, file, body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0), resp.ContentLength, tracker)
		if err == nil && !responseBodyDecoded(resp, opts) && meta.Length > 0 && n != meta.Length {
			err = bodyLengthMismatchError(n, meta.Length)
		}
	}
	tracker.stopAndWait()
	d.setConnections(0)
	return err
}

func (e *Engine) downloadSingleWithGETProbe(ctx context.Context, d *Download, rawURI string, opts Options, dirOpt, outOpt string, limiter *rateLimiter) (retMeta remoteMeta, retPath string, retErr error) {
	provisionalName := filenameFromURI(rawURI)
	provisionalPath := resolveOutputPath(dirOpt, outOpt, provisionalName, rawURI)
	selectedPath := d.outputPath()
	if selectedPath != "" {
		provisionalPath = selectedPath
	}
	start := int64(0)
	var file *os.File
	createdNew := false
	if canCreateSingleDestinationBeforeGET(opts, outOpt) {
		if err := e.ensureDownloadDir(filepath.Dir(provisionalPath)); err != nil {
			return remoteMeta{}, "", err
		}
		reservedPath, reserveErr := e.reserveOutputPath(provisionalPath, d.gid, false)
		if reserveErr != nil {
			return remoteMeta{}, "", reserveErr
		}
		provisionalPath = reservedPath
		f, err := os.OpenFile(provisionalPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			file = f
			createdNew = true
		} else if os.IsExist(err) {
			if st, statErr := os.Stat(provisionalPath); statErr == nil && st.Size() > 0 {
				start = st.Size()
			}
		} else {
			return remoteMeta{}, "", err
		}
	} else if optionBool(opts, "continue") {
		if st, err := os.Stat(provisionalPath); err == nil && st.Size() > 0 {
			start = st.Size()
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURI, nil)
	if err != nil {
		return remoteMeta{}, "", err
	}
	if start > 0 {
		applyRangeRequestOptions(req, opts, fmt.Sprintf("bytes=%d-", start))
	} else {
		applyRequestOptions(req, opts)
	}
	if start == 0 {
		applyConditionalHeaders(req, opts, provisionalPath)
	}
	resp, err := e.do(req, opts)
	if err != nil {
		if createdNew {
			_ = file.Close()
			_ = os.Remove(provisionalPath)
		}
		return remoteMeta{}, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		if createdNew {
			_ = file.Close()
			_ = os.Remove(provisionalPath)
		}
		meta := notModifiedMeta(rawURI, provisionalPath)
		d.setMetadata(meta, provisionalPath)
		return meta, provisionalPath, nil
	}
	if start > 0 && resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		if total, ok := completeLengthFromUnsatisfiedRange(resp); ok && total == start {
			if createdNew {
				_ = file.Close()
				_ = os.Remove(provisionalPath)
			}
			meta := metaFromResponse(resp, rawURI, opts)
			meta.Length = total
			meta.AcceptRange = true
			d.setMetadata(meta, provisionalPath)
			return meta, provisionalPath, nil
		}
	}
	if !isHTTPTransferSuccess(resp.StatusCode) || !isHTTPBodySuccess(resp.StatusCode) {
		if createdNew {
			_ = file.Close()
			_ = os.Remove(provisionalPath)
		}
		return remoteMeta{}, "", &httpStatusError{Method: http.MethodGet, URL: rawURI, StatusCode: resp.StatusCode, Status: resp.Status}
	}

	meta := metaFromResponse(resp, rawURI, opts)
	rangeLength := int64(-1)
	if resp.StatusCode == http.StatusPartialContent {
		if start == 0 {
			if createdNew {
				_ = file.Close()
				_ = os.Remove(provisionalPath)
			}
			return remoteMeta{}, "", newHTTPProtocolError("unexpected partial response without an internal Range request")
		}
		cr, err := validateOpenRangeResponse(resp, start, -1)
		if err != nil {
			if createdNew {
				_ = file.Close()
				_ = os.Remove(provisionalPath)
			}
			return remoteMeta{}, "", err
		}
		rangeLength = cr.end - cr.start + 1
		meta.AcceptRange = true
		if cr.totalKnown {
			meta.Length = cr.total
		} else if cr.end < math.MaxInt64 {
			// An open-ended internal range consumes the response through cr.end.
			// Preserve the resulting file length even when the server cannot state
			// the representation total in Content-Range.
			meta.Length = cr.end + 1
		}
	}
	outPath := provisionalPath
	if selectedPath == "" && outOpt == "" && meta.Filename != "" && meta.Filename != provisionalName {
		outPath = resolveOutputPath(dirOpt, "", meta.Filename, rawURI)
	}
	if file == nil {
		if outPath != provisionalPath || resp.StatusCode != http.StatusPartialContent {
			start = 0
		}
		if selectedPath == "" && start == 0 {
			// A zero-byte file contains nothing to preserve or resume. With an
			// explicit continue target, restart that file in place instead of
			// silently choosing an auto-renamed path.
			reuseRequestedOutput := false
			if outOpt != "" && optionBool(opts, "continue") {
				if st, statErr := os.Stat(outPath); statErr == nil && !st.IsDir() && st.Size() == 0 {
					reuseRequestedOutput = true
				}
			}
			outPath, err = resolveExistingOutputPath(outPath, opts, reuseRequestedOutput)
			if err != nil {
				return remoteMeta{}, "", err
			}
		}
		if err := e.ensureDownloadDir(filepath.Dir(outPath)); err != nil {
			return remoteMeta{}, "", err
		}
		autoRename := selectedPath == "" && start == 0 && !optionBool(opts, "allow-overwrite") && optionBoolDefault(opts, "auto-file-renaming", true)
		outPath, err = e.reserveOutputPath(outPath, d.gid, autoRename)
		if err != nil {
			return remoteMeta{}, "", err
		}
	}

	if file == nil {
		flag := os.O_CREATE | os.O_WRONLY
		if start > 0 {
			flag |= os.O_APPEND
		} else {
			start = 0
			flag |= os.O_TRUNC
		}
		file, err = os.OpenFile(outPath, flag, 0o644)
		if err != nil {
			return remoteMeta{}, "", err
		}
	} else if start > 0 && resp.StatusCode != http.StatusPartialContent {
		start = 0
		if err := file.Truncate(0); err != nil {
			_ = file.Close()
			_ = os.Remove(outPath)
			return remoteMeta{}, "", err
		}
	}
	defer func() {
		if file != nil {
			retErr = errors.Join(retErr, file.Close())
		}
	}()

	d.setMetadata(meta, outPath)
	d.resetSingleProgress(meta.Length, start)
	d.setConnections(1)
	var body io.Reader = resp.Body
	closeBody := func() {}
	if rangeLength < 0 {
		var err error
		body, closeBody, err = responseReader(resp, opts)
		if err != nil {
			d.setConnections(0)
			if createdNew {
				closeErr := file.Close()
				file = nil
				removeErr := os.Remove(outPath)
				return remoteMeta{}, "", errors.Join(err, closeErr, removeErr)
			}
			return remoteMeta{}, "", err
		}
	}
	defer closeBody()
	tracker := startDownloadSpeedTracker(ctx, d)
	if rangeLength >= 0 {
		err = copyExactWithProgress(ctx, file, body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0), rangeLength, tracker)
	} else {
		var n int64
		n, err = copyCountingWithProgress(ctx, file, body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0), resp.ContentLength, tracker)
		if err == nil && !responseBodyDecoded(resp, opts) && meta.Length > 0 && n != meta.Length {
			err = bodyLengthMismatchError(n, meta.Length)
		}
	}
	tracker.stopAndWait()
	d.setConnections(0)
	if err != nil {
		return remoteMeta{}, "", err
	}
	return meta, outPath, nil
}

func canCreateSingleDestinationBeforeGET(opts Options, out string) bool {
	return out != "" && optionBool(opts, "continue") && !optionBool(opts, "conditional-get")
}

type countingWriter struct {
	dst io.Writer
	n   int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.dst.Write(p)
	w.n += int64(n)
	return n, err
}

func copyCountingWithProgress(ctx context.Context, dst io.Writer, src io.Reader, d *Download, limiter *rateLimiter, lowestSpeed int64, contentLength int64, tracker *downloadSpeedTracker) (int64, error) {
	counting := &countingWriter{dst: dst}
	err := copyWithProgress(ctx, counting, src, d, limiter, lowestSpeed, contentLength, tracker)
	return counting.n, err
}

func copyExactWithProgress(ctx context.Context, dst io.Writer, src io.Reader, d *Download, limiter *rateLimiter, lowestSpeed int64, expected int64, tracker *downloadSpeedTracker) error {
	_, err := copyExactWithProgressCount(ctx, dst, src, d, limiter, lowestSpeed, expected, tracker)
	return err
}

func copyExactWithProgressCount(ctx context.Context, dst io.Writer, src io.Reader, d *Download, limiter *rateLimiter, lowestSpeed int64, expected int64, tracker *downloadSpeedTracker) (int64, error) {
	limited := &io.LimitedReader{R: src, N: expected}
	n, err := copyCountingWithProgress(ctx, dst, limited, d, limiter, lowestSpeed, expected, tracker)
	if err != nil {
		return n, err
	}
	if n != expected || limited.N != 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

func bodyLengthMismatchError(got, want int64) error {
	if got < want {
		return io.ErrUnexpectedEOF
	}
	return newHTTPProtocolError("response body length %d does not match expected length %d", got, want)
}

func copyWithProgress(ctx context.Context, dst io.Writer, src io.Reader, d *Download, limiter *rateLimiter, lowestSpeed int64, contentLength int64, tracker *downloadSpeedTracker) error {
	if contentLength > 0 && contentLength <= smallCopyBufferSize {
		bufp := smallCopyBufferPool.Get().(*[]byte)
		buf := *bufp
		defer smallCopyBufferPool.Put(bufp)
		return copyWithProgressBuffer(ctx, dst, src, d, limiter, lowestSpeed, buf, tracker)
	}
	if contentLength >= 512<<10 {
		bufp := largeCopyBufferPool.Get().(*[]byte)
		buf := *bufp
		defer largeCopyBufferPool.Put(bufp)
		return copyWithProgressBuffer(ctx, dst, src, d, limiter, lowestSpeed, buf, tracker)
	}
	bufp := copyBufferPool.Get().(*[]byte)
	buf := *bufp
	defer copyBufferPool.Put(bufp)
	return copyWithProgressBuffer(ctx, dst, src, d, limiter, lowestSpeed, buf, tracker)
}

func copyWithProgressBuffer(ctx context.Context, dst io.Writer, src io.Reader, d *Download, limiter *rateLimiter, lowestSpeed int64, buf []byte, tracker *downloadSpeedTracker) error {
	const progressFlushBytes = 1 * 1024 * 1024

	started := time.Now()
	written := int64(0)
	pendingProgress := int64(0)
	flushProgress := func() {
		if pendingProgress > 0 {
			d.addCompleted(pendingProgress)
			pendingProgress = 0
		}
	}
	checkSpeed := func(now time.Time) error {
		if lowestSpeed > 0 {
			if elapsed := now.Sub(started); elapsed >= time.Second {
				avg := bytesPerSecond(written, elapsed)
				if avg <= lowestSpeed {
					return fmt.Errorf("download speed %d is below lowest-speed-limit %d", avg, lowestSpeed)
				}
			}
		}
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			flushProgress()
			return err
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			if err := limiter.wait(ctx, int64(nr)); err != nil {
				flushProgress()
				return err
			}
			nw, ew := dst.Write(buf[:nr])
			if nw > 0 {
				n := int64(nw)
				pendingProgress += n
				written += n
				tracker.add(n)
				if pendingProgress >= progressFlushBytes || lowestSpeed > 0 {
					flushProgress()
					if err := checkSpeed(time.Now()); err != nil {
						return err
					}
				}
			}
			if ew != nil {
				flushProgress()
				return ew
			}
			if nr != nw {
				flushProgress()
				return io.ErrShortWrite
			}
		}
		if er != nil {
			if errors.Is(er, io.EOF) {
				flushProgress()
				return nil
			}
			flushProgress()
			return er
		}
	}
}
