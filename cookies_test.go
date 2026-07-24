package goaria

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadedCookiesAreBounded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cookies.txt")
	var data strings.Builder
	for i := 0; i < maxCookiesPerRequest+10; i++ {
		fmt.Fprintf(&data, "example.test\tFALSE\t/\tFALSE\t0\tcookie%d\tvalue\n", i)
	}
	if err := os.WriteFile(path, []byte(data.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse("https://example.test/file")
	if err != nil {
		t.Fatal(err)
	}
	if got := len(loadedCookiesForRequest(u, path)); got != maxCookiesPerRequest {
		t.Fatalf("loaded %d cookies, want capped count %d", got, maxCookiesPerRequest)
	}

	oversized := filepath.Join(dir, "oversized.txt")
	f, err := os.Create(oversized)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(maxCookieFileSize + 1); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := loadedCookiesForRequest(u, oversized); got != nil {
		t.Fatalf("oversized cookie file produced %d cookies", len(got))
	}
}
