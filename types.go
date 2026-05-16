package goaria

import (
	"context"
	"net/http"
	"time"

	"github.com/wchen99998/torrent/stream"
	"go.uber.org/zap"
)

const (
	// Version is the goaria package version.
	Version = "0.3.0"

	// Aria2CompatVersion is reported through aria2.getVersion.
	Aria2CompatVersion = "1.37.0"
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
	UserAgent              string
	InputFile              string
	SaveSession            string
	HTTPClient             *http.Client
	Logger                 *zap.Logger
	TorrentFileHandler     TorrentFileHandler
}

// TorrentFileHandler is called for each selected torrent file as soon as that
// file is complete. Handlers for different files can run concurrently.
//
// The handler owns the received lease. If it returns nil, it must eventually
// call Release or Discard, and goaria keeps the torrent download active until
// the lease is finalized. If the handler returns an error before finalizing the
// lease, goaria releases the file storage and reports the handler error as the
// download error.
type TorrentFileHandler func(context.Context, TorrentFile) error

// TorrentFile represents one completed torrent file handed to downstream
// processing. Lease owns the file reader and storage lifecycle. Call
// Lease.Release or Lease.Discard when downstream processing has finished.
type TorrentFile struct {
	GID   string
	Lease *stream.FileLease
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

type BittorrentInfo struct {
	AnnounceList [][]string             `json:"announceList,omitempty"`
	Comment      string                 `json:"comment,omitempty"`
	CreationDate int64                  `json:"creationDate,omitempty"`
	Mode         string                 `json:"mode"`
	Info         map[string]interface{} `json:"info"`
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
