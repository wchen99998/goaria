package goaria

import (
	"compress/gzip"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

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
