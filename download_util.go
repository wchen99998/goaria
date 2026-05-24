package goaria

import (
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
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type httpProtocolError struct {
	message string
}

func (e *httpProtocolError) Error() string {
	return e.message
}

func newHTTPProtocolError(format string, args ...any) error {
	return &httpProtocolError{message: fmt.Sprintf(format, args...)}
}

type byteContentRange struct {
	start       int64
	end         int64
	total       int64
	totalKnown  bool
	unsatisfied bool
}

func parseContentRangeTotal(s string) int64 {
	cr, ok := parseByteContentRange(s)
	if !ok || !cr.totalKnown {
		return -1
	}
	return cr.total
}

func parseByteContentRange(s string) (byteContentRange, bool) {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) != 2 || !strings.EqualFold(fields[0], "bytes") {
		return byteContentRange{}, false
	}
	rangePart, totalPart, ok := strings.Cut(fields[1], "/")
	if !ok || totalPart == "" {
		return byteContentRange{}, false
	}
	totalKnown := totalPart != "*"
	total := int64(-1)
	if totalKnown {
		var ok bool
		total, ok = parseHTTPNonNegativeDecimal(totalPart)
		if !ok {
			return byteContentRange{}, false
		}
	}
	if rangePart == "*" {
		if !totalKnown {
			return byteContentRange{}, false
		}
		return byteContentRange{total: total, totalKnown: true, unsatisfied: true}, true
	}
	startPart, endPart, ok := strings.Cut(rangePart, "-")
	if !ok {
		return byteContentRange{}, false
	}
	start, ok := parseHTTPNonNegativeDecimal(startPart)
	if !ok {
		return byteContentRange{}, false
	}
	end, ok := parseHTTPNonNegativeDecimal(endPart)
	if !ok || end < start {
		return byteContentRange{}, false
	}
	if totalKnown && end >= total {
		return byteContentRange{}, false
	}
	return byteContentRange{start: start, end: end, total: total, totalKnown: totalKnown}, true
}

func parseHTTPNonNegativeDecimal(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
}

func hasByteRangeSupport(h http.Header) bool {
	for _, value := range h.Values("Accept-Ranges") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "bytes") {
				return true
			}
		}
	}
	return false
}

func hasNonIdentityContentEncoding(h http.Header) bool {
	for _, value := range h.Values("Content-Encoding") {
		for _, token := range strings.Split(value, ",") {
			token = strings.TrimSpace(token)
			if token != "" && !strings.EqualFold(token, "identity") {
				return true
			}
		}
	}
	return false
}

func validateProbeRangeResponse(resp *http.Response, requestedStart, requestedEnd int64) (byteContentRange, error) {
	cr, err := validatePartialContentRange(resp)
	if err != nil {
		return byteContentRange{}, err
	}
	if cr.start != requestedStart || cr.end < requestedStart || cr.end > requestedEnd {
		return byteContentRange{}, newHTTPProtocolError("partial response Content-Range %q does not satisfy requested range bytes=%d-%d", resp.Header.Get("Content-Range"), requestedStart, requestedEnd)
	}
	if err := validateContentRangeLength(resp, cr); err != nil {
		return byteContentRange{}, err
	}
	return cr, nil
}

func validateExactRangeResponse(resp *http.Response, expectedStart, expectedEnd, expectedTotal int64) (byteContentRange, error) {
	cr, err := validatePartialContentRange(resp)
	if err != nil {
		return byteContentRange{}, err
	}
	if cr.start != expectedStart || cr.end != expectedEnd {
		return byteContentRange{}, newHTTPProtocolError("partial response Content-Range %q does not match requested range bytes=%d-%d", resp.Header.Get("Content-Range"), expectedStart, expectedEnd)
	}
	if expectedTotal >= 0 && cr.totalKnown && cr.total != expectedTotal {
		return byteContentRange{}, newHTTPProtocolError("partial response Content-Range total %d does not match expected total %d", cr.total, expectedTotal)
	}
	if err := validateContentRangeLength(resp, cr); err != nil {
		return byteContentRange{}, err
	}
	return cr, nil
}

func validateOpenRangeResponse(resp *http.Response, expectedStart, expectedTotal int64) (byteContentRange, error) {
	cr, err := validatePartialContentRange(resp)
	if err != nil {
		return byteContentRange{}, err
	}
	if cr.start != expectedStart {
		return byteContentRange{}, newHTTPProtocolError("partial response Content-Range %q does not start at requested offset %d", resp.Header.Get("Content-Range"), expectedStart)
	}
	if expectedTotal >= 0 && cr.totalKnown && cr.total != expectedTotal {
		return byteContentRange{}, newHTTPProtocolError("partial response Content-Range total %d does not match expected total %d", cr.total, expectedTotal)
	}
	if cr.totalKnown && cr.end != cr.total-1 {
		return byteContentRange{}, newHTTPProtocolError("partial response Content-Range %q does not complete the open-ended range", resp.Header.Get("Content-Range"))
	}
	if err := validateContentRangeLength(resp, cr); err != nil {
		return byteContentRange{}, err
	}
	return cr, nil
}

func validatePartialContentRange(resp *http.Response) (byteContentRange, error) {
	if hasNonIdentityContentEncoding(resp.Header) {
		return byteContentRange{}, newHTTPProtocolError("partial response uses unsupported Content-Encoding %q", resp.Header.Get("Content-Encoding"))
	}
	cr, ok := parseByteContentRange(resp.Header.Get("Content-Range"))
	if !ok || cr.unsatisfied {
		return byteContentRange{}, newHTTPProtocolError("partial response has invalid Content-Range %q", resp.Header.Get("Content-Range"))
	}
	return cr, nil
}

func validateContentRangeLength(resp *http.Response, cr byteContentRange) error {
	want := cr.end - cr.start + 1
	if resp.ContentLength >= 0 && resp.ContentLength != want {
		return newHTTPProtocolError("partial response Content-Length %d does not match Content-Range length %d", resp.ContentLength, want)
	}
	return nil
}

func completeLengthFromUnsatisfiedRange(resp *http.Response) (int64, bool) {
	cr, ok := parseByteContentRange(resp.Header.Get("Content-Range"))
	if !ok || !cr.unsatisfied || !cr.totalKnown {
		return 0, false
	}
	return cr.total, true
}

func isHTTPTransferSuccess(status int) bool {
	return status >= 200 && status < 300
}

func isHTTPBodySuccess(status int) bool {
	return isHTTPTransferSuccess(status) && status != http.StatusNoContent && status != http.StatusResetContent
}

func nonNegativeLength(n int64) int64 {
	if n < 0 {
		return 0
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
	if isSimpleRelativeName(out) && dir != "" && dir != "." {
		return dir + string(os.PathSeparator) + out
	}
	return filepath.Join(dir, filepath.Clean(out))
}

func resolveExistingOutputPath(path string, opts Options, allowResume bool) (string, error) {
	st, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return path, nil
	}
	if err != nil {
		return "", err
	}
	if st.IsDir() {
		return "", fmt.Errorf("output path is a directory: %s", path)
	}
	if allowResume || optionBool(opts, "allow-overwrite") {
		return path, nil
	}
	if optionBoolDefault(opts, "auto-file-renaming", true) {
		return nextAvailableOutputPath(path)
	}
	return "", fmt.Errorf("file already exists: %s", path)
}

func canResumeExistingOutput(path string, opts Options, meta remoteMeta, segmented bool) bool {
	if segmented || !optionBool(opts, "continue") || !meta.AcceptRange || meta.Length <= 0 {
		return false
	}
	st, err := os.Stat(path)
	return err == nil && !st.IsDir() && st.Size() > 0 && st.Size() < meta.Length
}

func nextAvailableOutputPath(path string) (string, error) {
	for i := 1; i < 10000; i++ {
		candidate := fmt.Sprintf("%s.%d", path, i)
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("could not find available auto-renamed path for %s", path)
}

func isSimpleRelativeName(name string) bool {
	return name != "" &&
		name != "." &&
		name != ".." &&
		!strings.ContainsAny(name, `/\`)
}

func (e *Engine) ensureDownloadDir(dir string) error {
	dir = filepath.Clean(dir)
	e.dirMu.Lock()
	if _, ok := e.createdDir[dir]; ok {
		e.dirMu.Unlock()
		info, err := os.Stat(dir)
		if err == nil && info.IsDir() {
			return nil
		}
		if err == nil {
			return fmt.Errorf("%s exists and is not a directory", dir)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		e.dirMu.Unlock()
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	e.dirMu.Lock()
	e.createdDir[dir] = struct{}{}
	e.dirMu.Unlock()
	return nil
}

func makeChunks(total, chunkSize int64) []chunkRange {
	if total <= 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = defaultHTTPSegmentSize
	}
	chunks := make([]chunkRange, 0, int(ceilDivInt64(total, chunkSize)))
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
	pieces := int(ceilDivInt64(total, pieceLength))
	if pieces <= 0 {
		return ""
	}
	bytesLen := (pieces + 7) / 8
	bits := make([]byte, bytesLen)
	completePieces := int(completedPieces(total, completed, pieceLength))
	for i := 0; i < completePieces; i++ {
		byteIndex := i / 8
		bit := uint(7 - (i % 8))
		bits[byteIndex] |= 1 << bit
	}
	return hex.EncodeToString(bits)
}

func completedPieces(total, completed, pieceLength int64) int64 {
	if total <= 0 || completed <= 0 || pieceLength <= 0 {
		return 0
	}
	pieces := ceilDivInt64(total, pieceLength)
	done := completed / pieceLength
	if completed >= total || done > pieces {
		return pieces
	}
	return done
}

func ceilDivInt64(n, d int64) int64 {
	if n <= 0 || d <= 0 {
		return 0
	}
	return 1 + (n-1)/d
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
	var protocolErr *httpProtocolError
	if errors.As(err, &protocolErr) {
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
