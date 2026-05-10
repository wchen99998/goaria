package goaria

import (
	"context"
	"io"
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
type TorrentFileHandler func(context.Context, TorrentFileLease) error

// TorrentFileLease represents one completed torrent file handed to downstream
// processing. Release or Discard closes Reader, finalizes the torrent file
// storage, and removes Path. They are safe to call once; repeated calls return
// the first finalization result.
type TorrentFileLease struct {
	GID    string
	Index  int
	Path   string
	Length int64

	// Reader is limited to this file's Length. Downstream code can ignore it and
	// use Path directly, but the lease must still be finalized.
	Reader io.ReadCloser

	// Release marks the completed file storage no longer needed.
	Release func(context.Context) error

	// Discard invalidates the file storage instead of preserving completion.
	Discard func(context.Context) error
}

// TorrentFile is kept as a compatibility alias for the completed-file lease.
type TorrentFile = TorrentFileLease

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
