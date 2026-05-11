package goaria

import (
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type loadedCookieJar struct {
	base http.CookieJar
	path string
}

func clientWithLoadedCookies(client *http.Client, path string) *http.Client {
	if client == nil || path == "" {
		return client
	}
	clone := *client
	clone.Jar = loadedCookieJar{base: client.Jar, path: path}
	return &clone
}

func (j loadedCookieJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	if j.base != nil {
		j.base.SetCookies(u, cookies)
	}
}

func (j loadedCookieJar) Cookies(u *url.URL) []*http.Cookie {
	out := append([]*http.Cookie(nil), loadedCookiesForRequest(u, j.path)...)
	if j.base != nil {
		out = append(out, j.base.Cookies(u)...)
	}
	return out
}

func loadedCookiesForRequest(u *url.URL, path string) []*http.Cookie {
	if u == nil || path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return nil
	}
	reqPath := u.EscapedPath()
	if reqPath == "" {
		reqPath = "/"
	}
	secureReq := u.Scheme == "https"
	now := time.Now().Unix()
	var out []*http.Cookie
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#HttpOnly_") {
			line = strings.TrimPrefix(line, "#HttpOnly_")
		} else if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 7 {
			continue
		}
		domain := strings.ToLower(strings.TrimSpace(fields[0]))
		includeSubdomains := strings.EqualFold(fields[1], "TRUE")
		cookiePath := fields[2]
		secureCookie := strings.EqualFold(fields[3], "TRUE")
		expires, _ := strconv.ParseInt(fields[4], 10, 64)
		name := fields[5]
		value := strings.Join(fields[6:], " ")
		if name == "" || secureCookie && !secureReq || expires > 0 && expires < now {
			continue
		}
		if !cookieDomainMatches(host, domain, includeSubdomains) || !cookiePathMatches(reqPath, cookiePath) {
			continue
		}
		out = append(out, &http.Cookie{Name: name, Value: value})
	}
	return out
}

func cookieDomainMatches(host, domain string, includeSubdomains bool) bool {
	domain = strings.TrimPrefix(domain, ".")
	if domain == "" {
		return false
	}
	if host == domain {
		return true
	}
	return includeSubdomains && strings.HasSuffix(host, "."+domain)
}

func cookiePathMatches(reqPath, cookiePath string) bool {
	if cookiePath == "" {
		cookiePath = "/"
	}
	if !strings.HasPrefix(cookiePath, "/") {
		cookiePath = "/" + cookiePath
	}
	return reqPath == cookiePath || strings.HasPrefix(reqPath, strings.TrimRight(cookiePath, "/")+"/")
}
