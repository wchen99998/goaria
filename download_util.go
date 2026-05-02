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
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

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
