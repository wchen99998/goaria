package goaria

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSegmentedHTTPDownloadCompletes(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 64*1024)
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "payload.bin", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/payload.bin"}, Options{
		"out":                       "payload.bin",
		"split":                     "4",
		"max-connection-per-server": "4",
		"min-split-size":            "1",
		"goaria-http-segment-size":  "64K",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	waitForStatus(t, engine, gid, StatusComplete)
	got, err := os.ReadFile(filepath.Join(dir, "payload.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded data mismatch: got %d bytes want %d", len(got), len(data))
	}
	status, err := engine.TellStatus(gid, []string{"status", "totalLength", "completedLength", "files"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(StatusComplete) {
		t.Fatalf("status = %v", status["status"])
	}
	if status["totalLength"] != status["completedLength"] {
		t.Fatalf("length mismatch: %#v", status)
	}
}

func TestSegmentedHTTPBitfieldTracksOutOfOrderChunks(t *testing.T) {
	const chunkSize = 1024
	data := bytes.Repeat([]byte("b"), 3*chunkSize)
	releaseMiddle := make(chan struct{})
	thirdServed := make(chan struct{})
	var releaseOnce sync.Once
	var thirdOnce sync.Once
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		start, end, _ := parseTestRange(t, r.Header.Get("Range"), int64(len(data)))
		if start == chunkSize {
			<-releaseMiddle
		}
		writeRange(w, data, start, end)
		if start == 2*chunkSize {
			thirdOnce.Do(func() { close(thirdServed) })
		}
	}))
	defer src.Close()
	defer releaseOnce.Do(func() { close(releaseMiddle) })

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/out-of-order.bin"}, Options{
		"out":                       "out-of-order.bin",
		"split":                     "2",
		"max-connection-per-server": "2",
		"min-split-size":            "1",
		"goaria-http-segment-size":  strconv.Itoa(chunkSize),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-thirdServed:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the out-of-order chunk")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, err := engine.TellStatus(gid, []string{"status", "completedLength", "bitfield"})
		if err != nil {
			t.Fatal(err)
		}
		if status["bitfield"] == "a0" {
			if status["completedLength"] != strconv.Itoa(2*chunkSize) {
				t.Fatalf("completedLength = %v, want %d", status["completedLength"], 2*chunkSize)
			}
			break
		}
		if status["status"] == string(StatusError) {
			t.Fatalf("download errored: %#v", status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("bitfield = %v, want a0 for completed chunks 0 and 2", status["bitfield"])
		}
		time.Sleep(10 * time.Millisecond)
	}

	releaseOnce.Do(func() { close(releaseMiddle) })
	waitForStatus(t, engine, gid, StatusComplete)
	status, err := engine.TellStatus(gid, []string{"bitfield"})
	if err != nil {
		t.Fatal(err)
	}
	if status["bitfield"] != "e0" {
		t.Fatalf("final bitfield = %v, want e0", status["bitfield"])
	}
}

func TestSegmentedHTTPDownloadDowngradesWhenServerIgnoresRange(t *testing.T) {
	data := bytes.Repeat([]byte("range-downgrade-"), 256)
	var ignoredRanges atomic.Int32
	var fullRequests atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		if r.Header.Get("Range") != "" {
			ignoredRanges.Add(1)
		} else {
			fullRequests.Add(1)
		}
		// Deliberately ignore Range and return the complete representation.
		_, _ = w.Write(data)
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/downgrade.bin"}, Options{
		"out":                       "downgrade.bin",
		"split":                     "2",
		"max-connection-per-server": "2",
		"min-split-size":            "1",
		"goaria-http-segment-size":  "1K",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "downgrade.bin"), data)
	if ignoredRanges.Load() == 0 {
		t.Fatal("server did not receive the attempted Range request")
	}
	if got := fullRequests.Load(); got != 1 {
		t.Fatalf("full request count = %d, want 1", got)
	}
}

func TestSegmentedHTTPDownloadUsesFixedChunks(t *testing.T) {
	data := bytes.Repeat([]byte("fixed-chunk-"), 3500)
	var mu sync.Mutex
	var gotRanges []chunkRange
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		start, end, ok := parseTestRange(t, r.Header.Get("Range"), int64(len(data)))
		if !ok {
			http.Error(w, "expected range request", http.StatusBadRequest)
			return
		}
		mu.Lock()
		gotRanges = append(gotRanges, chunkRange{start: start, end: end})
		mu.Unlock()
		writeRange(w, data, start, end)
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/fixed.bin"}, Options{
		"out":                       "fixed.bin",
		"split":                     "4",
		"max-connection-per-server": "4",
		"min-split-size":            "1",
		"goaria-http-segment-size":  "10K",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "fixed.bin"), data)

	mu.Lock()
	ranges := append([]chunkRange(nil), gotRanges...)
	mu.Unlock()
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
	if got, want := len(ranges), int(ceilDivInt64(int64(len(data)), 10*1024)); got != want {
		t.Fatalf("range count = %d, want %d; ranges=%#v", got, want, ranges)
	}
	for i, r := range ranges {
		wantStart := int64(i * 10 * 1024)
		wantEnd := wantStart + 10*1024 - 1
		if wantEnd >= int64(len(data)) {
			wantEnd = int64(len(data)) - 1
		}
		if r.start != wantStart || r.end != wantEnd {
			t.Fatalf("range[%d] = %d-%d, want %d-%d", i, r.start, r.end, wantStart, wantEnd)
		}
	}
}

func TestSegmentedHTTPDownloadAdaptsRangeThrottle(t *testing.T) {
	for _, statusCode := range []int{http.StatusForbidden, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(statusCode), func(t *testing.T) {
			data := bytes.Repeat([]byte("adaptive-range-"), 32*1024)
			var activeRanges atomic.Int32
			var throttled atomic.Int32
			src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				setDownloadHeaders(w, data)
				if r.Method == http.MethodHead {
					return
				}
				rng := r.Header.Get("Range")
				if rng == "" {
					http.Error(w, "expected range request", http.StatusBadRequest)
					return
				}
				current := activeRanges.Add(1)
				defer activeRanges.Add(-1)
				if current > 2 {
					throttled.Add(1)
					http.Error(w, http.StatusText(statusCode), statusCode)
					return
				}
				time.Sleep(10 * time.Millisecond)
				start, end, _ := parseTestRange(t, rng, int64(len(data)))
				writeRange(w, data, start, end)
			}))
			defer src.Close()

			dir := t.TempDir()
			engine, err := NewEngine(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close(context.Background())
			gid, err := engine.AddURI([]string{src.URL + "/adaptive.bin"}, Options{
				"out":                       "adaptive.bin",
				"split":                     "5",
				"max-connection-per-server": "5",
				"min-split-size":            "1",
				"goaria-http-segment-size":  "16K",
				"max-tries":                 "20",
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitForStatus(t, engine, gid, StatusComplete)
			assertFileEquals(t, filepath.Join(dir, "adaptive.bin"), data)
			if throttled.Load() == 0 {
				t.Fatal("server did not throttle any range requests")
			}
		})
	}
}

func TestSegmentedHTTPDownloadStabilizesRangeThrottle(t *testing.T) {
	data := bytes.Repeat([]byte("stable-adaptive-"), 128*1024)
	var activeRanges atomic.Int32
	var throttled atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		rng := r.Header.Get("Range")
		if rng == "" {
			http.Error(w, "expected range request", http.StatusBadRequest)
			return
		}
		current := activeRanges.Add(1)
		defer activeRanges.Add(-1)
		if current > 2 {
			throttled.Add(1)
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		time.Sleep(2 * time.Millisecond)
		start, end, _ := parseTestRange(t, rng, int64(len(data)))
		writeRange(w, data, start, end)
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/stable.bin"}, Options{
		"out":                       "stable.bin",
		"split":                     "5",
		"max-connection-per-server": "5",
		"min-split-size":            "1",
		"goaria-http-segment-size":  "16K",
		"max-tries":                 "30",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "stable.bin"), data)
	if got := throttled.Load(); got == 0 {
		t.Fatal("server did not throttle any range requests")
	} else if got > 8 {
		t.Fatalf("throttled range requests = %d, want stable probing with at most 8", got)
	}
}

func TestSegmentedHTTPDownloadDoesNotAdaptUnauthorized(t *testing.T) {
	data := bytes.Repeat([]byte("unauthorized-"), 32*1024)
	var rangeRequests atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		if r.Header.Get("Range") != "" {
			rangeRequests.Add(1)
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer src.Close()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/unauthorized.bin"}, Options{
		"out":                       "unauthorized.bin",
		"split":                     "4",
		"max-connection-per-server": "4",
		"min-split-size":            "1",
		"goaria-http-segment-size":  "16K",
		"max-tries":                 "20",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	status, err := engine.TellStatus(gid, []string{"errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status["errorMessage"].(string), "401") {
		t.Fatalf("errorMessage = %#v, want 401", status["errorMessage"])
	}
	if got := rangeRequests.Load(); got > 4 {
		t.Fatalf("range requests = %d, want no retries beyond initial concurrency", got)
	}
}

func TestDownloadRecreatesCachedDirectory(t *testing.T) {
	data := []byte("payload")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(data)
	}))
	defer src.Close()

	root := t.TempDir()
	dir := filepath.Join(root, "channel")
	engine, err := NewEngine(Config{Dir: root})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	first, err := engine.AddURI([]string{src.URL + "/first.bin"}, Options{
		"dir": dir,
		"out": "first.bin",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, first, StatusComplete)
	if err := os.Remove(filepath.Join(dir, "first.bin")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}

	second, err := engine.AddURI([]string{src.URL + "/second.bin"}, Options{
		"dir": dir,
		"out": "second.bin",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, second, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "second.bin"), data)
}

func TestChangeOptionScalesActiveSegmentedDownload(t *testing.T) {
	runActiveSegmentedConnectionScaleTest(t, func(engine *Engine, gid string) error {
		_, err := engine.ChangeOption(gid, Options{"max-connection-per-server": "4"})
		return err
	})
}

func TestGlobalMaxConnectionOptionScalesActiveSegmentedDownload(t *testing.T) {
	runActiveSegmentedConnectionScaleTest(t, func(engine *Engine, gid string) error {
		_, err := engine.ChangeGlobalOption(Options{"max-connection-per-server": "4"})
		return err
	})
}

func TestChangeGlobalOptionDoesNotRebaseExistingDownloadDerivedState(t *testing.T) {
	root := t.TempDir()
	oldDir := filepath.Join(root, "old")
	newDir := filepath.Join(root, "new")
	sessionPath := filepath.Join(root, "session.txt")
	engine, err := NewEngine(Config{Dir: oldDir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{"http://example.invalid/file.bin"}, Options{
		"pause": "true",
		"out":   "file.bin",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ChangeGlobalOption(Options{"dir": newDir}); err != nil {
		t.Fatal(err)
	}

	global := engine.GetGlobalOption()
	if global["dir"] != newDir {
		t.Fatalf("global dir = %v, want %s", global["dir"], newDir)
	}
	opts, err := engine.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if opts["dir"] != oldDir {
		t.Fatalf("download option dir = %v, want original dir %s", opts["dir"], oldDir)
	}
	status, err := engine.TellStatus(gid, []string{"dir", "files"})
	if err != nil {
		t.Fatal(err)
	}
	if status["dir"] != oldDir {
		t.Fatalf("status dir = %v, want original dir %s", status["dir"], oldDir)
	}
	files := status["files"].([]FileInfo)
	if got, want := files[0].Path, filepath.Join(oldDir, "file.bin"); got != want {
		t.Fatalf("file path = %s, want %s", got, want)
	}
	if _, err := engine.SaveSession(); err != nil {
		t.Fatal(err)
	}
	session, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(session), newDir) {
		t.Fatalf("saved session unexpectedly contains rebased dir %s:\n%s", newDir, string(session))
	}
	if !strings.Contains(string(session), oldDir) {
		t.Fatalf("saved session missing original dir %s:\n%s", oldDir, string(session))
	}
}

func runActiveSegmentedConnectionScaleTest(t *testing.T, adjust func(*Engine, string) error) {
	t.Helper()
	data := bytes.Repeat([]byte("active-scale-"), 64*1024)
	var activeRanges atomic.Int64
	var activeFull atomic.Int64
	var maxActiveRanges atomic.Int64

	rememberMax := func(n int64) {
		for {
			old := maxActiveRanges.Load()
			if n <= old || maxActiveRanges.CompareAndSwap(old, n) {
				return
			}
		}
	}
	slowWrite := func(w http.ResponseWriter, payload []byte) {
		flusher, _ := w.(http.Flusher)
		for len(payload) > 0 {
			n := 1024
			if len(payload) < n {
				n = len(payload)
			}
			if _, err := w.Write(payload[:n]); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			payload = payload[n:]
			time.Sleep(time.Millisecond)
		}
	}

	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		rng := r.Header.Get("Range")
		if rng == "" {
			activeFull.Add(1)
			defer activeFull.Add(-1)
			w.WriteHeader(http.StatusOK)
			slowWrite(w, data)
			return
		}
		start, end, _ := parseTestRange(t, rng, int64(len(data)))
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		current := activeRanges.Add(1)
		rememberMax(current)
		defer activeRanges.Add(-1)
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		slowWrite(w, data[start:end+1])
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/active-scale.bin"}, Options{
		"out":                      "active-scale.bin",
		"min-split-size":           "1",
		"goaria-http-segment-size": "16K",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && activeRanges.Load()+activeFull.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if activeRanges.Load()+activeFull.Load() == 0 {
		t.Fatal("timed out waiting for initial HTTP transfer")
	}

	if err := adjust(engine, gid); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && maxActiveRanges.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := maxActiveRanges.Load(); got < 2 {
		status, err := engine.TellStatus(gid, []string{"connections", "status"})
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("active range connections peaked at %d, want at least 2 after max-connection-per-server change; status=%#v", got, status)
	}

	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "active-scale.bin"), data)
}

func TestDownloadSpeedTrackerAggregatesConcurrentWorkers(t *testing.T) {
	d := &Download{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracker := startDownloadSpeedTrackerWithInterval(ctx, d, 100*time.Millisecond)
	defer tracker.stopAndWait()

	for i := 0; i < 4; i++ {
		tracker.add(256 * 1024)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		d.mu.RLock()
		speed := d.downloadBPS
		d.mu.RUnlock()
		if speed > 0 {
			if speed < 5*1024*1024 {
				t.Fatalf("downloadBPS = %d, expected aggregate worker speed", speed)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for aggregate download speed")
}

func TestRateLimiterWaitHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- newRateLimiter(1).wait(ctx, 2)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("rate limiter did not stop after context cancellation")
	}
}

func TestCanceledRateLimiterWaitDoesNotPoisonSharedLimiter(t *testing.T) {
	limiter := newRateLimiter(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- limiter.wait(ctx, 2) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context.Canceled", err)
	}
	limiter.mu.Lock()
	remaining := time.Until(limiter.next)
	limiter.mu.Unlock()
	if remaining > time.Second {
		t.Fatalf("canceled reservation retained %v of shared bandwidth debt, want at most one byte", remaining)
	}
}

func TestDynamicAndOverallRateLimitersRefreshLimits(t *testing.T) {
	var limit atomic.Int64
	local := newDynamicRateLimiter(limit.Load)
	overall := newRateLimiter(1 << 30)
	local.parent = overall

	if err := local.wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	overall.mu.Lock()
	overallNext := overall.next
	overall.mu.Unlock()
	if overallNext.IsZero() {
		t.Fatal("overall limiter did not reserve bandwidth")
	}

	limit.Store(1 << 30)
	if err := local.wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	local.mu.Lock()
	gotLimit := local.limit
	local.mu.Unlock()
	if gotLimit != 1<<30 {
		t.Fatalf("dynamic limit = %d, want %d", gotLimit, int64(1<<30))
	}
}

func TestRateLimiterDoesNotBankIdleCreditOrDoubleDelayParents(t *testing.T) {
	now := time.Now()
	limiter := newRateLimiter(100)
	if got := limiter.reservationDeadline(now, 100).Sub(now); got != time.Second {
		t.Fatalf("first reservation delay = %v, want 1s", got)
	}
	idleNow := now.Add(10 * time.Second)
	if got := limiter.reservationDeadline(idleNow, 100).Sub(idleNow); got != time.Second {
		t.Fatalf("reservation after idle = %v, want 1s without banked credit", got)
	}

	child := newRateLimiter(100)
	child.parent = newRateLimiter(100)
	if got := child.reservationDeadline(now, 100).Sub(now); got != time.Second {
		t.Fatalf("composed limiter delay = %v, want max(parent, child)=1s", got)
	}
}

func TestOverflowSafeTransferArithmetic(t *testing.T) {
	if meetsSegmentedMinimum(math.MaxInt64, math.MaxInt64, true) {
		t.Fatal("overflowing twice-min-split comparison unexpectedly enabled segmentation")
	}
	if !meetsSegmentedMinimum(8<<20, 1, false) {
		t.Fatal("default segmented minimum rejected the 8 MiB floor")
	}
	if got := bytesPerSecond(math.MaxInt64, time.Nanosecond); got != math.MaxInt64 {
		t.Fatalf("overflow-safe byte rate = %d, want MaxInt64", got)
	}
}

func TestParseSizePreservesExactInt64Boundary(t *testing.T) {
	maxText := strconv.FormatInt(math.MaxInt64, 10)
	if got, err := parseSizeValue(maxText); err != nil || got != math.MaxInt64 {
		t.Fatalf("parseSizeValue(MaxInt64) = %d, %v", got, err)
	}
	if _, err := parseSizeValue("9223372036854775808"); err == nil {
		t.Fatal("parseSizeValue accepted MaxInt64+1")
	}
	if got, err := parseSizeValue("1.5K"); err != nil || got != 1536 {
		t.Fatalf("parseSizeValue(1.5K) = %d, %v; want 1536", got, err)
	}
}

func TestOptionValidationRejectsUnsafePersistenceAndDurationValues(t *testing.T) {
	tooMany := make([]string, maxDownloadOptionListItems+1)
	for name, opts := range map[string]Options{
		"duration overflow": {
			"timeout": strconv.FormatInt(int64(math.MaxInt64)/int64(time.Second)+1, 10),
		},
		"torrent size above absolute bound": {
			"goaria-max-torrent-size": strconv.FormatInt(maxSupportedTorrentSize+1, 10),
		},
		"line break":           {"out": "unsafe\nname"},
		"edge whitespace":      {"out": " trailing "},
		"oversized value list": {"header": tooMany},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateOptionValues(opts); err == nil {
				t.Fatalf("validateOptionValues(%#v) unexpectedly succeeded", opts)
			}
		})
	}
}

func TestMakeChunksCapsTaskCountAndHandlesMaxLength(t *testing.T) {
	total := int64(^uint64(0) >> 1)
	chunks := makeChunks(total, 1)
	if got := len(chunks); got > maxHTTPSegmentCount {
		t.Fatalf("chunk count = %d, want at most %d", got, maxHTTPSegmentCount)
	}
	if len(chunks) == 0 || chunks[0].start != 0 || chunks[len(chunks)-1].end != total-1 {
		t.Fatalf("chunks do not cover [0,%d]: first=%#v last=%#v", total-1, chunks[0], chunks[len(chunks)-1])
	}
}

func TestOpenRangeMustReachKnownRepresentationEnd(t *testing.T) {
	resp := &http.Response{
		Header:        http.Header{"Content-Range": []string{"bytes 5-7/*"}},
		ContentLength: 3,
	}
	if _, err := validateOpenRangeResponse(resp, 5, 10); err == nil {
		t.Fatal("accepted an open range that stopped before the known representation end")
	}
	if cr, err := validateOpenRangeResponse(resp, 5, -1); err != nil || cr.end != 7 {
		t.Fatalf("unknown-total open range = %#v, %v; want end 7, nil", cr, err)
	}
}

func TestOutputPathReservationsPreventConcurrentWriters(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	path := filepath.Join(t.TempDir(), "shared.bin")
	first, err := engine.reserveOutputPath(path, "first", true)
	if err != nil {
		t.Fatal(err)
	}
	if first != path {
		t.Fatalf("first reservation = %q, want %q", first, path)
	}
	second, err := engine.reserveOutputPath(path, "second", true)
	if err != nil {
		t.Fatal(err)
	}
	if want := path + ".1"; second != want {
		t.Fatalf("second reservation = %q, want %q", second, want)
	}
	if _, err := engine.reserveOutputPath(path, "third", false); err == nil {
		t.Fatal("concurrent reservation without auto-renaming unexpectedly succeeded")
	}

	engine.releaseOutputPaths("first")
	if got, err := engine.reserveOutputPath(path, "third", false); err != nil || got != path {
		t.Fatalf("reservation after release = %q, %v; want %q, nil", got, err, path)
	}
}

func TestOutputPathReservationCanonicalizesSymlinkedDirectories(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if err := os.Mkdir(realDir, 0o755); err != nil {
		t.Fatal(err)
	}
	linkDir := filepath.Join(root, "link")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	engine := &Engine{}
	realPath := filepath.Join(realDir, "shared.bin")
	if _, err := engine.reserveOutputPath(realPath, "real-owner", false); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.reserveOutputPath(filepath.Join(linkDir, "shared.bin"), "link-owner", false); err == nil {
		t.Fatal("symlink alias bypassed output-path reservation")
	}
}

func TestOutputPathReservationCanonicalizesBrokenFinalSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(t.TempDir(), "not-created.bin")
	link := filepath.Join(root, "output.bin")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	got, err := outputPathReservationKey(link)
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.Abs(target)
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Clean(want) {
		t.Fatalf("broken symlink reservation key = %q, want %q", got, filepath.Clean(want))
	}
}

func TestQueuePauseUnpauseAndChangePosition(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir(), MaxConcurrentDownloads: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(Options{"max-concurrent-downloads": "0"}); err != nil {
		t.Fatal(err)
	}

	pos0 := 0
	_, err = engine.AddURI([]string{"http://example.invalid/a"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	gid2, err := engine.AddURI([]string{"http://example.invalid/b"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	newPos, err := engine.ChangePosition(gid2, pos0, "POS_SET")
	if err != nil {
		t.Fatal(err)
	}
	if newPos != 0 {
		t.Fatalf("new position = %d, want 0", newPos)
	}
	waiting := engine.TellWaiting(0, 2, []string{"gid"})
	if len(waiting) == 0 || waiting[0]["gid"] != gid2 {
		t.Fatalf("waiting order = %#v, want gid2 first", waiting)
	}
}

func TestControlMethodsReturnGIDAndStoppedRemoveRequiresResultRemoval(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir(), MaxConcurrentDownloads: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(Options{"max-concurrent-downloads": "0"}); err != nil {
		t.Fatal(err)
	}

	gid, err := engine.AddURI([]string{"http://example.invalid/a"}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := engine.Unpause(gid); err != nil || got != gid {
		t.Fatalf("Unpause = %q, %v; want %q, nil", got, err, gid)
	}
	if got, err := engine.Pause(gid); err != nil || got != gid {
		t.Fatalf("Pause = %q, %v; want %q, nil", got, err, gid)
	}
	if got, err := engine.Remove(gid); err != nil || got != gid {
		t.Fatalf("Remove = %q, %v; want %q, nil", got, err, gid)
	}
	if _, err := engine.Remove(gid); err == nil {
		t.Fatal("Remove on stopped download succeeded; want error")
	}
	if got, err := engine.RemoveDownloadResult(gid); err != nil || got != "OK" {
		t.Fatalf("RemoveDownloadResult = %q, %v; want OK, nil", got, err)
	}

	forceGID, err := engine.AddURI([]string{"http://example.invalid/b"}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := engine.ForceRemove(forceGID); err != nil || got != forceGID {
		t.Fatalf("ForceRemove = %q, %v; want %q, nil", got, err, forceGID)
	}
}

func TestDefaultOptionsPreferAria2Compatibility(t *testing.T) {
	opts := defaultOptions(t.TempDir(), 0, 0)
	for key, want := range map[string]string{
		"split":                     "5",
		"max-connection-per-server": "1",
		"min-split-size":            "20M",
		"goaria-http-segment-size":  "50M",
		"allow-overwrite":           "false",
		"auto-file-renaming":        "true",
		"continue":                  "false",
	} {
		if got := optionString(opts, key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestExistingOutputFileAutoRenamedByDefault(t *testing.T) {
	data := []byte("replacement")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.txt", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	dir := t.TempDir()
	original := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(original, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/file.txt"}, Options{"out": "file.txt", "split": "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, original, []byte("existing"))
	assertFileEquals(t, filepath.Join(dir, "file.txt.1"), data)
}

func TestExistingOutputFileFailsWhenRenamingAndOverwriteDisabled(t *testing.T) {
	data := []byte("replacement")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.txt", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	dir := t.TempDir()
	original := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(original, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/file.txt"}, Options{
		"out":                "file.txt",
		"split":              "1",
		"auto-file-renaming": "false",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	status, err := engine.TellStatus(gid, []string{"errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status["errorMessage"].(string), "file already exists") {
		t.Fatalf("errorMessage = %#v, want file already exists", status["errorMessage"])
	}
	assertFileEquals(t, original, []byte("existing"))
}

func waitForStatus(t testing.TB, engine *Engine, gid string, want Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.TellStatus(gid, []string{"status", "errorMessage"})
		if err != nil {
			t.Fatal(err)
		}
		if status["status"] == string(want) {
			return
		}
		if status["status"] == string(StatusError) {
			t.Fatalf("download errored: %v", status["errorMessage"])
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", want)
}
