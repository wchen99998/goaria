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
	"net"
	"net/http"
	"net/url"
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
	if expectedTotal >= 0 && cr.end != expectedTotal-1 {
		return byteContentRange{}, newHTTPProtocolError("partial response Content-Range %q does not complete expected total %d", resp.Header.Get("Content-Range"), expectedTotal)
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
	return os.MkdirAll(dir, 0o755)
}

// reserveOutputPath prevents two downloads in the same Engine from writing the
// same destination concurrently. The filesystem existence checks still decide
// overwrite/resume behavior; reservations cover the otherwise unavoidable
// window between choosing a path and creating it.
func (e *Engine) reserveOutputPath(path, gid string, autoRename bool) (string, error) {
	key, err := outputPathReservationKey(path)
	if err != nil {
		return "", err
	}

	e.pathMu.Lock()
	defer e.pathMu.Unlock()
	if owner, ok := e.pathReservations[key]; !ok || owner == gid {
		e.recordPathReservationLocked(key, gid)
		return path, nil
	}
	if !autoRename {
		return "", fmt.Errorf("output path is already in use: %s", path)
	}
	for i := 1; i < 10000; i++ {
		candidate := fmt.Sprintf("%s.%d", path, i)
		candidateKey, err := outputPathReservationKey(candidate)
		if err != nil {
			return "", err
		}
		if _, reserved := e.pathReservations[candidateKey]; reserved {
			continue
		}
		if _, err := os.Stat(candidate); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		e.recordPathReservationLocked(candidateKey, gid)
		return candidate, nil
	}
	return "", fmt.Errorf("could not find available auto-renamed path for %s", path)
}

func outputPathReservationKey(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	absolute = filepath.Clean(absolute)
	current := absolute
	var suffix []string
	symlinkHops := 0
	for {
		if info, lstatErr := os.Lstat(current); lstatErr == nil && info.Mode()&os.ModeSymlink != 0 {
			if symlinkHops >= 255 {
				return "", fmt.Errorf("too many symlinks while resolving %s", path)
			}
			target, err := os.Readlink(current)
			if err != nil {
				return "", err
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(current), target)
			}
			current = filepath.Clean(target)
			symlinkHops++
			continue
		} else if lstatErr != nil && !errors.Is(lstatErr, os.ErrNotExist) {
			return "", lstatErr
		}
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return absolute, nil
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

// reserveOutputPaths reserves a set as one operation. It is intentionally
// separate from reserveOutputPath: torrent storage paths are fixed before the
// client starts and therefore cannot be auto-renamed after a conflict.
func (e *Engine) reserveOutputPaths(paths []string, gid string) error {
	return e.updateOutputPathReservations(paths, gid, false)
}

func (e *Engine) replaceOutputPaths(paths []string, gid string) error {
	return e.updateOutputPathReservations(paths, gid, true)
}

func (e *Engine) updateOutputPathReservations(paths []string, gid string, replace bool) error {
	type reservation struct {
		key  string
		path string
	}
	reservations := make([]reservation, 0, len(paths))
	seen := make(map[string]string, len(paths))
	for _, path := range paths {
		key, err := outputPathReservationKey(path)
		if err != nil {
			return err
		}
		if previous, duplicate := seen[key]; duplicate {
			return fmt.Errorf("multiple files use the same output path: %s and %s", previous, path)
		}
		seen[key] = path
		reservations = append(reservations, reservation{key: key, path: path})
	}

	e.pathMu.Lock()
	defer e.pathMu.Unlock()
	for _, candidate := range reservations {
		if owner, reserved := e.pathReservations[candidate.key]; reserved && owner != gid {
			return fmt.Errorf("output path is already in use: %s", candidate.path)
		}
	}
	if replace {
		for existing := range e.reservedPathsByGID[gid] {
			if _, keep := seen[existing]; !keep {
				delete(e.pathReservations, existing)
				delete(e.reservedPathsByGID[gid], existing)
			}
		}
	}
	for _, candidate := range reservations {
		e.recordPathReservationLocked(candidate.key, gid)
	}
	return nil
}

func (e *Engine) recordPathReservationLocked(path, gid string) {
	if e.pathReservations == nil {
		e.pathReservations = make(map[string]string)
	}
	if e.reservedPathsByGID == nil {
		e.reservedPathsByGID = make(map[string]map[string]struct{})
	}
	e.pathReservations[path] = gid
	paths := e.reservedPathsByGID[gid]
	if paths == nil {
		paths = make(map[string]struct{})
		e.reservedPathsByGID[gid] = paths
	}
	paths[path] = struct{}{}
}

func (e *Engine) releaseOutputPaths(gid string) {
	e.pathMu.Lock()
	defer e.pathMu.Unlock()
	for path := range e.reservedPathsByGID[gid] {
		if e.pathReservations[path] == gid {
			delete(e.pathReservations, path)
		}
	}
	delete(e.reservedPathsByGID, gid)
}

func makeChunks(total, chunkSize int64) []chunkRange {
	if total <= 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = defaultHTTPSegmentSize
	}
	if minChunkSize := ceilDivInt64(total, maxHTTPSegmentCount); chunkSize < minChunkSize {
		chunkSize = minChunkSize
	}
	chunks := make([]chunkRange, 0, int(ceilDivInt64(total, chunkSize)))
	for start := int64(0); start < total; {
		size := chunkSize
		if remaining := total - start; size > remaining {
			size = remaining
		}
		end := start + size - 1
		chunks = append(chunks, chunkRange{start: start, end: end})
		start = end + 1
	}
	return chunks
}

func bitfieldFor(total, completed, pieceLength int64) string {
	if total <= 0 || pieceLength <= 0 {
		return ""
	}
	pieceCount := ceilDivInt64(total, pieceLength)
	if pieceCount <= 0 || pieceCount > maxTorrentPieceCount || pieceCount > int64(int(^uint(0)>>1)) {
		return ""
	}
	pieces := int(pieceCount)
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
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return false
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
