package goaria

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"testing"
)

func TestTransferSpeedQueries(t *testing.T) {
	d := &Download{gid: "active", status: StatusActive, downloadBPS: 4096, uploadBPS: 1024}
	engine := &Engine{downloads: map[string]*Download{d.gid: d}}
	for _, keys := range [][]string{nil, {"downloadSpeed", "uploadSpeed"}} {
		status, err := engine.TellStatus(d.gid, keys)
		if err != nil {
			t.Fatal(err)
		}
		if status["downloadSpeed"] != "4096" || status["uploadSpeed"] != "1024" {
			t.Fatalf("TellStatus(%v) rates = %v/%v, want 4096/1024", keys, status["downloadSpeed"], status["uploadSpeed"])
		}
		active := engine.TellActive(keys)
		if len(active) != 1 || active[0]["uploadSpeed"] != "1024" {
			t.Fatalf("TellActive(%v) = %#v", keys, active)
		}
	}
	encoded, err := json.Marshal(engine.GetGlobalStat())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["downloadSpeed"] != "4096" || fields["uploadSpeed"] != "1024" {
		t.Fatalf("serialized global rates = %s, want string values 4096/1024", encoded)
	}
}

func TestGlobalTransferSpeedsOnlyIncludeActiveDownloads(t *testing.T) {
	engine := &Engine{downloads: map[string]*Download{
		"active1":  {status: StatusActive, downloadBPS: 4096, uploadBPS: 1024},
		"active2":  {status: StatusActive, downloadBPS: 8192, uploadBPS: 2048},
		"negative": {status: StatusActive, downloadBPS: -1, uploadBPS: -1},
		"paused":   {status: StatusPaused, downloadBPS: math.MaxInt64, uploadBPS: math.MaxInt64},
		"waiting":  {status: StatusWaiting, downloadBPS: math.MaxInt64, uploadBPS: math.MaxInt64},
		"complete": {status: StatusComplete, downloadBPS: math.MaxInt64, uploadBPS: math.MaxInt64},
	}}
	stat := engine.GetGlobalStat()
	if stat.DownloadSpeed != "12288" || stat.UploadSpeed != "3072" {
		t.Fatalf("global rates = %s/%s, want 12288/3072", stat.DownloadSpeed, stat.UploadSpeed)
	}
	if stat.NumActive != "3" || stat.NumWaiting != "2" {
		t.Fatalf("global counts = %#v", stat)
	}
}

func TestGlobalTransferSpeedsSaturateIndependently(t *testing.T) {
	for _, uploadOverflows := range []bool{false, true} {
		t.Run(strconv.FormatBool(uploadOverflows), func(t *testing.T) {
			first := &Download{status: StatusActive, downloadBPS: math.MaxInt64 - 1, uploadBPS: 7}
			second := &Download{status: StatusActive, downloadBPS: 2, uploadBPS: 11}
			wantDownload, wantUpload := strconv.FormatInt(math.MaxInt64, 10), "18"
			if uploadOverflows {
				first.downloadBPS, first.uploadBPS = first.uploadBPS, first.downloadBPS
				second.downloadBPS, second.uploadBPS = second.uploadBPS, second.downloadBPS
				wantDownload, wantUpload = wantUpload, wantDownload
			}
			engine := &Engine{downloads: map[string]*Download{"first": first, "second": second}}
			stat := engine.GetGlobalStat()
			if stat.DownloadSpeed != wantDownload || stat.UploadSpeed != wantUpload {
				t.Fatalf("global rates = %s/%s, want %s/%s", stat.DownloadSpeed, stat.UploadSpeed, wantDownload, wantUpload)
			}
		})
	}
}

func TestInactiveDownloadClearsTransferSpeeds(t *testing.T) {
	for _, action := range []string{"pause", "remove", "complete", "error", "restart"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			d := &Download{gid: "transfer", status: StatusActive, ctx: ctx, downloadBPS: 4096, uploadBPS: 1024}
			engine := &Engine{
				ctx:        ctx,
				cfg:        Config{MaxDownloadResult: 10},
				downloads:  map[string]*Download{d.gid: d},
				active:     map[string]struct{}{d.gid: {}},
				stoppedSet: make(map[string]struct{}),
			}
			switch action {
			case "pause":
				if _, err := engine.Pause(d.gid); err != nil {
					t.Fatal(err)
				}
			case "remove":
				if _, err := engine.Remove(d.gid); err != nil {
					t.Fatal(err)
				}
			case "complete":
				engine.finishDownload(d, ctx, nil)
			case "error":
				engine.finishDownload(d, ctx, errors.New("test transfer failure"))
			case "restart":
				engine.mu.Lock()
				d.mu.Lock()
				engine.restartActiveDownloadLocked(d)
				d.mu.Unlock()
				engine.mu.Unlock()
			}
			status, err := engine.TellStatus(d.gid, []string{"downloadSpeed", "uploadSpeed"})
			if err != nil {
				t.Fatal(err)
			}
			if status["downloadSpeed"] != "0" || status["uploadSpeed"] != "0" {
				t.Fatalf("rates after %s = %#v, want zero", action, status)
			}
			d.setTransferBPS(4096, 1024)
			d.setDownloadBPS(8192)
			status = d.snapshot([]string{"downloadSpeed", "uploadSpeed"})
			if status["downloadSpeed"] != "0" || status["uploadSpeed"] != "0" {
				t.Fatalf("late sample restored rates after %s: %#v", action, status)
			}
		})
	}
}
