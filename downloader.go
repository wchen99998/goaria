package goaria

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	err error
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

const metadataProbeRangeEnd = 1023

type remoteMeta struct {
	Length       int64
	AcceptRange  bool
	FinalURI     string
	Filename     string
	LastModified string
	NotModified  bool
}

type chunkRange struct {
	start int64
	end   int64
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
		t.d.setDownloadBPS(delta * int64(time.Second) / int64(elapsed))
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
	mu    sync.Mutex
	limit int64
	start time.Time
	bytes int64
}

func newRateLimiter(limit int64) *rateLimiter {
	if limit <= 0 {
		return nil
	}
	return &rateLimiter{limit: limit, start: time.Now()}
}

func (l *rateLimiter) wait(n int64) {
	if l == nil || n <= 0 {
		return
	}
	l.mu.Lock()
	l.bytes += n
	expected := time.Duration(float64(l.bytes) / float64(l.limit) * float64(time.Second))
	sleep := l.start.Add(expected).Sub(time.Now())
	l.mu.Unlock()
	if sleep > 0 {
		time.Sleep(sleep)
	}
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
			if retryWait > 0 && isRetryableDownloadError(err) {
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
	maxConn := optionInt(opts, "max-connection-per-server", split)
	if split < 1 {
		split = 1
	}
	if maxConn < 1 {
		maxConn = 1
	}
	concurrency := minInt(split, maxConn)
	minSplit := optionBytes(opts, "min-split-size", 1<<20)
	limit := optionBytes(opts, "max-download-limit", 0)
	limiter := newRateLimiter(limit)
	dirOpt := optionString(opts, "dir")
	outOpt := optionString(opts, "out")

	if canUseSingleGETProbe(opts, concurrency) {
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
	if concurrency > 1 {
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
	segmentedMin := minSplit * 2
	if !optionExplicit(opts, "min-split-size") && segmentedMin < 8<<20 {
		segmentedMin = 8 << 20
	}
	segmented := meta.Length > 0 && meta.AcceptRange && concurrency > 1 && meta.Length >= segmentedMin
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

	d.setMetadata(meta, outPath)
	if segmented {
		opts = transportOptionsForSegmented(opts)
		chunks := makeChunks(meta.Length, concurrency, minSplit)
		err = e.downloadSegmented(ctx, d, meta, outPath, opts, chunks, limiter)
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

func canUseSingleGETProbe(opts Options, concurrency int) bool {
	if concurrency > 1 || optionBool(opts, "dry-run") {
		return false
	}
	return optionString(opts, "out") != "" || !optionBool(opts, "continue")
}

func transportOptionsForSegmented(opts Options) Options {
	if normalizeHTTPVersion(optionString(opts, "http-version")) != "auto" {
		return opts
	}
	out := cloneOptions(opts)
	out["http-version"] = "1.1"
	return out
}

func (e *Engine) probe(ctx context.Context, rawURI string, opts Options, conditionalPath string) (remoteMeta, error) {
	var headMeta remoteMeta
	headMetaOK := false
	if optionBoolDefault(opts, "use-head", true) {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURI, nil)
		if err != nil {
			return remoteMeta{}, err
		}
		applyRequestOptions(req, opts)
		applyConditionalHeaders(req, opts, conditionalPath)
		resp, err := e.do(req, opts)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusNotModified {
				return notModifiedMeta(rawURI, conditionalPath), nil
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				meta := metaFromResponse(resp, rawURI)
				if resp.ContentLength >= 0 {
					return meta, nil
				}
				headMeta = meta
				headMetaOK = true
			} else if resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusNotImplemented && resp.StatusCode != http.StatusForbidden {
				return remoteMeta{}, &httpStatusError{Method: http.MethodHead, URL: rawURI, StatusCode: resp.StatusCode, Status: resp.Status}
			}
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURI, nil)
	if err != nil {
		return remoteMeta{}, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", metadataProbeRangeEnd))
	applyRequestOptions(req, opts)
	applyConditionalHeaders(req, opts, conditionalPath)
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
	if resp.StatusCode >= 400 {
		if headMetaOK {
			return headMeta, nil
		}
		return remoteMeta{}, &httpStatusError{Method: http.MethodGet, URL: rawURI, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	meta := metaFromResponse(resp, rawURI)
	if resp.StatusCode == http.StatusPartialContent {
		meta.AcceptRange = true
		if total := parseContentRangeTotal(resp.Header.Get("Content-Range")); total >= 0 {
			meta.Length = total
		} else {
			meta.Length = 0
		}
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

	errCh := make(chan error, len(chunks))
	var wg sync.WaitGroup
	maxTries := optionInt(opts, "max-tries", 5)
	if maxTries < 0 {
		maxTries = 1
	}
	retryWait := time.Duration(optionInt(opts, "retry-wait", 0)) * time.Second
	d.setConnections(len(chunks))
	for _, ch := range chunks {
		wg.Add(1)
		go func(r chunkRange) {
			defer wg.Done()
			errCh <- e.downloadRangeWithRetries(ctx, d, meta.FinalURI, path, opts, r, limiter, tracker, maxTries, retryWait)
		}(ch)
	}
	wg.Wait()
	close(errCh)
	d.setConnections(0)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) downloadRangeWithRetries(ctx context.Context, d *Download, rawURI, path string, opts Options, r chunkRange, limiter *rateLimiter, tracker *downloadSpeedTracker, maxTries int, retryWait time.Duration) error {
	var lastErr error
	for attempt := 0; maxTries == 0 || attempt < maxTries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := e.downloadRange(ctx, d, rawURI, path, opts, r, limiter, tracker)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryableRangeSetupError(err) {
			return err
		}
		if retryWait > 0 {
			if err := sleepContext(ctx, retryWait); err != nil {
				return err
			}
		}
	}
	return lastErr
}

func isRetryableRangeSetupError(err error) bool {
	var bodyErr *rangeBodyError
	if errors.As(err, &bodyErr) {
		return false
	}
	return isRetryableDownloadError(err)
}

func (e *Engine) downloadRange(ctx context.Context, d *Download, rawURI, path string, opts Options, r chunkRange, limiter *rateLimiter, tracker *downloadSpeedTracker) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURI, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r.start, r.end))
	applyRequestOptions(req, opts)
	resp, err := e.do(req, opts)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return &httpStatusError{Method: http.MethodGet, URL: rawURI, StatusCode: resp.StatusCode, Status: fmt.Sprintf("range %d-%d: %s", r.start, r.end, resp.Status)}
	}
	body, closeBody, err := responseReader(resp, opts)
	if err != nil {
		return err
	}
	defer closeBody()
	file, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Seek(r.start, io.SeekStart); err != nil {
		return err
	}
	if err := copyWithProgress(ctx, file, body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0), resp.ContentLength, tracker); err != nil {
		return &rangeBodyError{err: err}
	}
	return nil
}

func (e *Engine) downloadSingle(ctx context.Context, d *Download, meta remoteMeta, path string, opts Options, limiter *rateLimiter) error {
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
	defer file.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, meta.FinalURI, nil)
	if err != nil {
		return err
	}
	if start > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	}
	applyRequestOptions(req, opts)
	resp, err := e.do(req, opts)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return &httpStatusError{Method: http.MethodGet, URL: meta.FinalURI, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	if start > 0 && resp.StatusCode != http.StatusPartialContent {
		start = 0
		if err := file.Truncate(0); err != nil {
			return err
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}
	d.resetSingleProgress(meta.Length, start)
	d.setConnections(1)
	body, closeBody, err := responseReader(resp, opts)
	if err != nil {
		d.setConnections(0)
		return err
	}
	defer closeBody()
	tracker := startDownloadSpeedTracker(ctx, d)
	err = copyWithProgress(ctx, file, body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0), resp.ContentLength, tracker)
	tracker.stopAndWait()
	d.setConnections(0)
	return err
}

func (e *Engine) downloadSingleWithGETProbe(ctx context.Context, d *Download, rawURI string, opts Options, dirOpt, outOpt string, limiter *rateLimiter) (remoteMeta, string, error) {
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
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	}
	applyRequestOptions(req, opts)
	applyConditionalHeaders(req, opts, provisionalPath)
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
	if resp.StatusCode >= 400 {
		if createdNew {
			_ = file.Close()
			_ = os.Remove(provisionalPath)
		}
		return remoteMeta{}, "", &httpStatusError{Method: http.MethodGet, URL: rawURI, StatusCode: resp.StatusCode, Status: resp.Status}
	}

	meta := metaFromResponse(resp, rawURI)
	if resp.StatusCode == http.StatusPartialContent {
		meta.AcceptRange = true
		if total := parseContentRangeTotal(resp.Header.Get("Content-Range")); total >= 0 {
			meta.Length = total
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
			outPath, err = resolveExistingOutputPath(outPath, opts, false)
			if err != nil {
				return remoteMeta{}, "", err
			}
		}
		if err := e.ensureDownloadDir(filepath.Dir(outPath)); err != nil {
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
	defer file.Close()

	d.setMetadata(meta, outPath)
	d.resetSingleProgress(meta.Length, start)
	d.setConnections(1)
	body, closeBody, err := responseReader(resp, opts)
	if err != nil {
		d.setConnections(0)
		if createdNew {
			_ = os.Remove(outPath)
		}
		return remoteMeta{}, "", err
	}
	defer closeBody()
	tracker := startDownloadSpeedTracker(ctx, d)
	err = copyWithProgress(ctx, file, body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0), resp.ContentLength, tracker)
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
				avg := written * int64(time.Second) / int64(elapsed)
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
			nw, ew := dst.Write(buf[:nr])
			if nw > 0 {
				n := int64(nw)
				pendingProgress += n
				written += n
				tracker.add(n)
				limiter.wait(n)
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
