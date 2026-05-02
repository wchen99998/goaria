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

var copyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 32*1024)
		return &buf
	},
}

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

type offsetWriter struct {
	file *os.File
	off  int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.file.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
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
	ctx := d.ctx
	opts := cloneOptions(d.options)
	uris := cloneURIs(d.uris)
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
			d.setURIUsed(u.URI)
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
			e.log.Warn("download URI failed", zap.String("gid", d.gid), zap.String("uri", u.URI), zap.Int("attempt", attempt), zap.Error(err))
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
	meta, err := e.probe(ctx, rawURI, opts)
	if err != nil {
		return err
	}
	outPath := resolveOutputPath(optionString(opts, "dir"), optionString(opts, "out"), meta.Filename, rawURI)
	if optionBool(opts, "dry-run") || meta.NotModified {
		d.setMetadata(meta, outPath)
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}

	d.setMetadata(meta, outPath)
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

	if meta.Length > 0 && meta.AcceptRange && concurrency > 1 && meta.Length >= minSplit*2 {
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

func (e *Engine) probe(ctx context.Context, rawURI string, opts Options) (remoteMeta, error) {
	conditionalPath := resolveOutputPath(optionString(opts, "dir"), optionString(opts, "out"), filenameFromURI(rawURI), rawURI)
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
				return metaFromResponse(resp, rawURI), nil
			}
			if resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusNotImplemented && resp.StatusCode != http.StatusForbidden {
				return remoteMeta{}, &httpStatusError{Method: http.MethodHead, URL: rawURI, StatusCode: resp.StatusCode, Status: resp.Status}
			}
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURI, nil)
	if err != nil {
		return remoteMeta{}, err
	}
	req.Header.Set("Range", "bytes=0-0")
	applyRequestOptions(req, opts)
	applyConditionalHeaders(req, opts, conditionalPath)
	resp, err := e.do(req, opts)
	if err != nil {
		return remoteMeta{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return notModifiedMeta(rawURI, conditionalPath), nil
	}
	if resp.StatusCode >= 400 {
		return remoteMeta{}, &httpStatusError{Method: http.MethodGet, URL: rawURI, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	meta := metaFromResponse(resp, rawURI)
	if resp.StatusCode == http.StatusPartialContent {
		meta.AcceptRange = true
		if total := parseContentRangeTotal(resp.Header.Get("Content-Range")); total >= 0 {
			meta.Length = total
		}
	}
	return meta, nil
}

func (e *Engine) downloadSegmented(ctx context.Context, d *Download, meta remoteMeta, path string, opts Options, chunks []chunkRange, limiter *rateLimiter) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(meta.Length); err != nil {
		return err
	}
	d.resetProgress(meta.Length, chunks)

	errCh := make(chan error, len(chunks))
	var wg sync.WaitGroup
	d.setConnections(len(chunks))
	for _, ch := range chunks {
		wg.Add(1)
		go func(r chunkRange) {
			defer wg.Done()
			errCh <- e.downloadRange(ctx, d, meta.FinalURI, opts, file, r, limiter)
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

func (e *Engine) downloadRange(ctx context.Context, d *Download, rawURI string, opts Options, file *os.File, r chunkRange, limiter *rateLimiter) error {
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
	return copyWithProgress(ctx, &offsetWriter{file: file, off: r.start}, body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0))
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
	err = copyWithProgress(ctx, file, body, d, limiter, optionBytes(opts, "lowest-speed-limit", 0))
	d.setConnections(0)
	return err
}

func copyWithProgress(ctx context.Context, dst io.Writer, src io.Reader, d *Download, limiter *rateLimiter, lowestSpeed int64) error {
	bufp := copyBufferPool.Get().(*[]byte)
	buf := *bufp
	defer copyBufferPool.Put(bufp)
	started := time.Now()
	written := int64(0)
	speedWindowStart := started
	speedWindowBytes := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			if nw > 0 {
				d.addCompleted(int64(nw))
				written += int64(nw)
				speedWindowBytes += int64(nw)
				limiter.wait(int64(nw))
				elapsed := time.Since(started)
				windowElapsed := time.Since(speedWindowStart)
				if windowElapsed >= time.Second {
					d.setDownloadBPS(speedWindowBytes * int64(time.Second) / int64(windowElapsed))
					speedWindowStart = time.Now()
					speedWindowBytes = 0
				}
				if lowestSpeed > 0 && elapsed >= time.Second {
					avg := written * int64(time.Second) / int64(elapsed)
					if avg <= lowestSpeed {
						return fmt.Errorf("download speed %d is below lowest-speed-limit %d", avg, lowestSpeed)
					}
				}
			}
			if ew != nil {
				return ew
			}
			if nr != nw {
				return io.ErrShortWrite
			}
		}
		if er != nil {
			if errors.Is(er, io.EOF) {
				return nil
			}
			return er
		}
	}
}
