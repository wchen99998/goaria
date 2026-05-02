package goaria

import (
	"net/http"
	"time"

	"go.uber.org/zap"
)

const (
	// Version is the goaria package version.
	Version = "0.1.0"

	// Aria2CompatVersion is reported through aria2.getVersion.
	Aria2CompatVersion = "1.37.0-goaria"
)

type Status string

const (
	StatusActive   Status = "active"
	StatusWaiting  Status = "waiting"
	StatusPaused   Status = "paused"
	StatusError    Status = "error"
	StatusComplete Status = "complete"
	StatusRemoved  Status = "removed"
)

// Config controls an Engine. Zero values are replaced with aria2-compatible
// defaults where possible and pragmatic HTTP/S defaults elsewhere.
type Config struct {
	Dir                    string
	MaxConcurrentDownloads int
	MaxDownloadResult      int
	MaxRequestSize         int64
	RPCSecret              string
	UserAgent              string
	HTTPClient             *http.Client
	Logger                 *zap.Logger
}

type ServerConfig struct {
	Addr           string
	MaxRequestSize int64
	RPCSecret      string
	Logger         *zap.Logger
}

type URIStatus string

const (
	URIStatusUsed    URIStatus = "used"
	URIStatusWaiting URIStatus = "waiting"
)

type URIInfo struct {
	URI    string    `json:"uri"`
	Status URIStatus `json:"status"`
}

type FileInfo struct {
	Index           string    `json:"index"`
	Path            string    `json:"path"`
	Length          string    `json:"length"`
	CompletedLength string    `json:"completedLength"`
	Selected        string    `json:"selected"`
	URIs            []URIInfo `json:"uris"`
}

type ServerInfo struct {
	URI           string `json:"uri"`
	CurrentURI    string `json:"currentUri"`
	DownloadSpeed string `json:"downloadSpeed"`
}

type GlobalStat struct {
	DownloadSpeed   string `json:"downloadSpeed"`
	UploadSpeed     string `json:"uploadSpeed"`
	NumActive       string `json:"numActive"`
	NumWaiting      string `json:"numWaiting"`
	NumStopped      string `json:"numStopped"`
	NumStoppedTotal string `json:"numStoppedTotal,omitempty"`
}

type VersionInfo struct {
	Version         string   `json:"version"`
	EnabledFeatures []string `json:"enabledFeatures"`
}

type SessionInfo struct {
	SessionID string `json:"sessionId"`
}

type Notification struct {
	Method string
	GID    string
	Time   time.Time
}
