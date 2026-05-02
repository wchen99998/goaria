package goaria

import (
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

	done := make(chan struct{})
	go d.trackSpeed(ctx, done)
	defer close(done)

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
				limiter.wait(int64(nw))
				if lowestSpeed > 0 && time.Since(started) >= time.Second {
					avg := written * int64(time.Second) / int64(time.Since(started))
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

func (d *Download) setURIUsed(raw string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.uris {
		if d.uris[i].URI == raw {
			d.uris[i].Status = URIStatusUsed
		} else {
			d.uris[i].Status = URIStatusWaiting
		}
	}
}

func (d *Download) setMetadata(meta remoteMeta, path string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.path = path
	d.dir = filepath.Dir(path)
	d.out = filepath.Base(path)
	d.currentURI = meta.FinalURI
	d.totalLength = meta.Length
	if meta.Length > 0 && d.completedLen > meta.Length {
		d.completedLen = 0
	}
	d.pieceLength = 0
	d.numPieces = 0
	d.bitfield = ""
}

func (d *Download) resetProgress(total int64, chunks []chunkRange) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.totalLength = total
	d.completedLen = 0
	if len(chunks) > 0 {
		d.pieceLength = chunks[0].end - chunks[0].start + 1
		d.numPieces = int64(len(chunks))
	} else {
		d.pieceLength = total
		d.numPieces = 1
	}
	d.bitfield = bitfieldFor(d.totalLength, d.completedLen, d.pieceLength)
}

func (d *Download) resetSingleProgress(total, completed int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.totalLength = total
	d.completedLen = completed
	if total > 0 {
		d.pieceLength = total
		d.numPieces = 1
		d.bitfield = bitfieldFor(total, completed, total)
	}
}

func (d *Download) addCompleted(n int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.completedLen += n
	if d.totalLength > 0 && d.completedLen > d.totalLength {
		d.completedLen = d.totalLength
	}
	if d.pieceLength > 0 {
		d.bitfield = bitfieldFor(d.totalLength, d.completedLen, d.pieceLength)
	}
}

func (d *Download) setConnections(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.connections = n
}

func (d *Download) trackSpeed(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	last := int64(0)
	d.mu.RLock()
	last = d.completedLen
	d.mu.RUnlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			d.mu.Lock()
			now := d.completedLen
			d.downloadBPS = now - last
			last = now
			d.mu.Unlock()
		}
	}
}

func applyRequestOptions(req *http.Request, opts Options) {
	ua := optionString(opts, "user-agent")
	if ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	for _, h := range optionStringList(opts, "header") {
		name, value, ok := strings.Cut(h, ":")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		req.Header.Add(name, strings.TrimSpace(value))
	}
	if ref := optionString(opts, "referer"); ref != "" {
		if ref == "*" {
			ref = req.URL.String()
		}
		req.Header.Set("Referer", ref)
	}
	if optionBool(opts, "http-no-cache") {
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Pragma", "no-cache")
	}
	if optionBool(opts, "http-accept-gzip") {
		req.Header.Set("Accept-Encoding", "gzip")
	}
	if !optionBool(opts, "http-auth-challenge") {
		if user := optionString(opts, "http-user"); user != "" {
			req.SetBasicAuth(user, optionString(opts, "http-passwd"))
		} else if req.URL.User != nil {
			user := req.URL.User.Username()
			pass, _ := req.URL.User.Password()
			req.SetBasicAuth(user, pass)
		}
	}
}

func applyConditionalHeaders(req *http.Request, opts Options, path string) {
	if !optionBool(opts, "conditional-get") {
		return
	}
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return
	}
	req.Header.Set("If-Modified-Since", st.ModTime().UTC().Format(http.TimeFormat))
}

func responseReader(resp *http.Response, opts Options) (io.Reader, func(), error) {
	if optionBool(opts, "http-accept-gzip") && strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, func() {}, err
		}
		return zr, func() { _ = zr.Close() }, nil
	}
	return resp.Body, func() {}, nil
}

func metaFromResponse(resp *http.Response, original string) remoteMeta {
	length := resp.ContentLength
	filename := filenameFromResponse(resp, original)
	finalURI := original
	if resp.Request != nil && resp.Request.URL != nil {
		finalURI = resp.Request.URL.String()
	}
	return remoteMeta{
		Length:       length,
		AcceptRange:  strings.Contains(strings.ToLower(resp.Header.Get("Accept-Ranges")), "bytes"),
		FinalURI:     finalURI,
		Filename:     filename,
		LastModified: resp.Header.Get("Last-Modified"),
	}
}

func notModifiedMeta(rawURI, path string) remoteMeta {
	size := int64(0)
	lastModified := ""
	if st, err := os.Stat(path); err == nil {
		size = st.Size()
		lastModified = st.ModTime().UTC().Format(http.TimeFormat)
	}
	return remoteMeta{
		Length:       size,
		AcceptRange:  true,
		FinalURI:     rawURI,
		Filename:     filepath.Base(path),
		LastModified: lastModified,
		NotModified:  true,
	}
}

func filenameFromResponse(resp *http.Response, original string) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if name := params["filename"]; name != "" {
				return filepath.Base(name)
			}
		}
	}
	if resp.Request != nil && resp.Request.URL != nil {
		if name := filepath.Base(resp.Request.URL.Path); name != "." && name != "/" && name != "" {
			return name
		}
	}
	return filenameFromURI(original)
}

func parseContentRangeTotal(s string) int64 {
	if s == "" {
		return -1
	}
	_, after, ok := strings.Cut(s, "/")
	if !ok || after == "*" {
		return -1
	}
	n, err := strconv.ParseInt(after, 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

func resolveOutputPath(dir, out, filename, rawURI string) string {
	if dir == "" {
		dir = "."
	}
	if out == "" {
		out = filename
	}
	if out == "" {
		out = filenameFromURI(rawURI)
	}
	if filepath.IsAbs(out) {
		return filepath.Clean(out)
	}
	return filepath.Join(dir, filepath.Clean(out))
}

func makeChunks(total int64, concurrency int, minSplit int64) []chunkRange {
	if concurrency < 1 {
		concurrency = 1
	}
	if minSplit <= 0 {
		minSplit = 1 << 20
	}
	maxChunks := int(math.Ceil(float64(total) / float64(minSplit)))
	if maxChunks < 1 {
		maxChunks = 1
	}
	count := minInt(concurrency, maxChunks)
	chunkSize := int64(math.Ceil(float64(total) / float64(count)))
	chunks := make([]chunkRange, 0, count)
	for start := int64(0); start < total; start += chunkSize {
		end := start + chunkSize - 1
		if end >= total {
			end = total - 1
		}
		chunks = append(chunks, chunkRange{start: start, end: end})
	}
	return chunks
}

func bitfieldFor(total, completed, pieceLength int64) string {
	if total <= 0 || pieceLength <= 0 {
		return ""
	}
	pieces := int(math.Ceil(float64(total) / float64(pieceLength)))
	if pieces <= 0 {
		return ""
	}
	bytesLen := (pieces + 7) / 8
	bits := make([]byte, bytesLen)
	completePieces := int(completed / pieceLength)
	if completed >= total {
		completePieces = pieces
	}
	for i := 0; i < completePieces; i++ {
		byteIndex := i / 8
		bit := uint(7 - (i % 8))
		bits[byteIndex] |= 1 << bit
	}
	return fmt.Sprintf("%x", bits)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func isRetryableDownloadError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var status *httpStatusError
	if errors.As(err, &status) {
		switch status.StatusCode {
		case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
			http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrShortWrite) {
		return true
	}
	return true
}

func isFileNotFoundError(err error) bool {
	var status *httpStatusError
	return errors.As(err, &status) && status.StatusCode == http.StatusNotFound
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func applyRemoteTime(path string, meta remoteMeta, opts Options) error {
	if !optionBool(opts, "remote-time") || meta.LastModified == "" {
		return nil
	}
	t, err := http.ParseTime(meta.LastModified)
	if err != nil {
		return nil
	}
	return os.Chtimes(path, t, t)
}

func verifyChecksum(path string, opts Options) error {
	spec := optionString(opts, "checksum")
	if spec == "" {
		return nil
	}
	algo, expected, ok := strings.Cut(spec, "=")
	if !ok {
		return fmt.Errorf("invalid checksum format")
	}
	var h hash.Hash
	switch strings.ToLower(strings.ReplaceAll(algo, "-", "")) {
	case "md5":
		h = md5.New()
	case "sha1":
		h = sha1.New()
	case "sha224":
		h = sha256.New224()
	case "sha256":
		h = sha256.New()
	case "sha384":
		h = sha512.New384()
	case "sha512":
		h = sha512.New()
	default:
		return fmt.Errorf("unsupported checksum algorithm %q", algo)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	expected = strings.ToLower(strings.TrimSpace(expected))
	if got != expected {
		return fmt.Errorf("checksum mismatch: got %s want %s", got, expected)
	}
	return nil
}
