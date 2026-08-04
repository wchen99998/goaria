package goaria

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	g "github.com/anacrolix/generics"
	"github.com/wchen99998/torrent"
	"github.com/wchen99998/torrent/bencode"
	"github.com/wchen99998/torrent/merkle"
	"github.com/wchen99998/torrent/metainfo"
	"github.com/wchen99998/torrent/storage"
	"github.com/wchen99998/torrent/stream"
	infohash_v2 "github.com/wchen99998/torrent/types/infohash-v2"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type torrentRuntime struct {
	client             *torrent.Client
	torrent            *torrent.Torrent
	selection          *stream.FileSelectionController
	selectionMu        sync.Mutex
	selectionSelected  map[int]struct{}
	selectionCompleted map[int]struct{}
	selectionClosed    bool
}

type torrentFileState struct {
	Index     int
	Path      string
	Length    int64
	Completed int64
	Selected  bool
}

const (
	torrentPeerProfileQBitTorrent = "qbittorrent"
	torrentPeerProfileNative      = "native"
	// The fork represents peer-protocol piece offsets with uint32 and stores
	// each runtime piece index in int32. Validate those implementation widths
	// before handing untrusted magnet metadata to the fork.
	maxTorrentProtocolPieceLength = int64(^uint32(0))
	maxTorrentPieceCount          = int64(1 << 20)
	maxSupportedTorrentSize       = 64 << 20
	defaultTorrentHandlerWorkers  = 4
	maxTorrentWebSeeds            = maxDownloadURIs

	torrentSelectFileExtOption  = "goaria-select-file-ext"
	torrentExcludeFileExtOption = "goaria-exclude-file-ext"

	qBittorrentVersion            = "5.2.0"
	qBittorrentBep20Prefix        = "-qB5200-"
	qBittorrentPeerVisibleVersion = "qBittorrent/" + qBittorrentVersion
)

func isMagnetURI(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && strings.EqualFold(u.Scheme, "magnet")
}

func isHTTPTorrentURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Host != "" && (u.Scheme == "http" || u.Scheme == "https")
}

func (e *Engine) addTorrent(torrentData string, uris []string, opts Options, position *int) (string, error) {
	for _, raw := range uris {
		if err := validateHTTPURI(raw); err != nil {
			return "", err
		}
	}
	if opts == nil {
		opts = Options{}
	}

	e.mu.RLock()
	if err := e.ctx.Err(); err != nil {
		e.mu.RUnlock()
		return "", ErrShutdown
	}
	merged := layerOptions(cloneOptions(e.global), opts)
	e.mu.RUnlock()

	data, mi, info, sourceURL, err := e.resolveTorrentSource(torrentData, merged)
	if err != nil {
		return "", err
	}
	if sourceURL != "" {
		merged["goaria-torrent-source-url"] = sourceURL
		if err := validateOptionLayerSize(merged); err != nil {
			return "", err
		}
	}
	if _, _, err := torrentSelectionAndIndexOut(info, merged); err != nil {
		return "", err
	}

	e.mu.Lock()
	if err := e.ctx.Err(); err != nil {
		e.mu.Unlock()
		return "", ErrShutdown
	}
	gid := optionString(merged, "gid")
	if gid == "" {
		gid = randomHex(8)
	} else if !validGID(gid) {
		e.mu.Unlock()
		return "", ErrInvalidGID
	}
	if _, exists := e.downloads[gid]; exists {
		e.mu.Unlock()
		return "", fmt.Errorf("gid already exists")
	}
	d, err := newTorrentDownload(gid, data, "", uris, merged, mi, info)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	if optionBool(merged, "pause") {
		d.status = StatusPaused
	} else {
		e.insertWaitingLocked(gid, position)
	}
	e.downloads[gid] = d
	e.mu.Unlock()
	e.signal()
	e.saveSessionBestEffort()
	return gid, nil
}

func (e *Engine) resolveTorrentSource(raw string, opts Options) ([]byte, *metainfo.MetaInfo, metainfo.Info, string, error) {
	maxSize := torrentSizeLimit(opts)
	data, decodeErr := decodeTorrentParamUpTo(raw, maxSize)
	if decodeErr == nil {
		mi, info, metaErr := torrentMetaInfo(data)
		if metaErr == nil {
			return data, mi, info, "", nil
		}
		if !isHTTPTorrentURL(raw) {
			return nil, nil, metainfo.Info{}, "", metaErr
		}
	} else {
		var sizeErr *torrentSizeError
		if errors.As(decodeErr, &sizeErr) || !isHTTPTorrentURL(raw) {
			return nil, nil, metainfo.Info{}, "", decodeErr
		}
	}

	data, err := e.fetchTorrentURL(e.ctx, raw, opts)
	if err != nil {
		return nil, nil, metainfo.Info{}, "", err
	}
	mi, info, err := torrentMetaInfo(data)
	if err != nil {
		return nil, nil, metainfo.Info{}, "", err
	}
	return data, mi, info, raw, nil
}

const defaultMaxTorrentSize = 16 << 20

type torrentSizeError struct {
	max int64
}

func (e *torrentSizeError) Error() string {
	return fmt.Sprintf("torrent data exceeds goaria-max-torrent-size %d", e.max)
}

func torrentSizeLimit(opts Options) int64 {
	maxSize := optionBytes(opts, "goaria-max-torrent-size", defaultMaxTorrentSize)
	if maxSize <= 0 {
		return defaultMaxTorrentSize
	}
	if maxSize > maxSupportedTorrentSize {
		return maxSupportedTorrentSize
	}
	return maxSize
}

func (e *Engine) fetchTorrentURL(ctx context.Context, raw string, opts Options) ([]byte, error) {
	if err := validateHTTPURI(raw); err != nil {
		return nil, err
	}
	maxSize := torrentSizeLimit(opts)
	maxTries := optionInt(opts, "max-tries", 5)
	if maxTries < 0 {
		maxTries = 1
	}
	retryWait := time.Duration(optionInt(opts, "retry-wait", 0)) * time.Second
	var lastErr error
	for attempt := 0; maxTries == 0 || attempt < maxTries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := e.fetchTorrentURLOnce(ctx, raw, opts, maxSize)
		if err == nil {
			return data, nil
		}
		lastErr = err
		var sizeErr *torrentSizeError
		if errors.As(err, &sizeErr) {
			break
		}
		if !isRetryableDownloadError(err) {
			break
		}
		if retryWait > 0 && (maxTries == 0 || attempt+1 < maxTries) {
			if err := sleepContext(ctx, retryWait); err != nil {
				return nil, err
			}
		}
	}
	return nil, lastErr
}

func (e *Engine) fetchTorrentURLOnce(ctx context.Context, raw string, opts Options, maxSize int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, err
	}
	applyRequestOptions(req, opts)
	resp, err := e.do(req, opts)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if !isHTTPBodySuccess(resp.StatusCode) || resp.StatusCode == http.StatusPartialContent {
		return nil, &httpStatusError{Method: http.MethodGet, URL: raw, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	if resp.ContentLength > maxSize {
		return nil, &torrentSizeError{max: maxSize}
	}
	body, closeBody, err := responseReader(resp, opts)
	if err != nil {
		return nil, err
	}
	defer closeBody()
	data, err := io.ReadAll(io.LimitReader(body, maxSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxSize {
		return nil, &torrentSizeError{max: maxSize}
	}
	return data, nil
}

func (e *Engine) addMagnet(raw string, opts Options, position *int) (string, error) {
	if _, err := torrent.TorrentSpecFromMagnetUri(raw); err != nil {
		return "", err
	}
	if opts == nil {
		opts = Options{}
	}
	e.mu.Lock()
	if err := e.ctx.Err(); err != nil {
		e.mu.Unlock()
		return "", ErrShutdown
	}
	merged := layerOptions(e.global, opts)
	if err := validateTorrentExtensionOptions(merged); err != nil {
		e.mu.Unlock()
		return "", err
	}
	gid := optionString(merged, "gid")
	if gid == "" {
		gid = randomHex(8)
	} else if !validGID(gid) {
		e.mu.Unlock()
		return "", ErrInvalidGID
	}
	if _, exists := e.downloads[gid]; exists {
		e.mu.Unlock()
		return "", fmt.Errorf("gid already exists")
	}
	d, err := newMagnetDownload(gid, raw, nil, merged)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	if optionBool(merged, "pause") {
		d.status = StatusPaused
	} else {
		e.insertWaitingLocked(gid, position)
	}
	e.downloads[gid] = d
	e.mu.Unlock()
	e.signal()
	e.saveSessionBestEffort()
	return gid, nil
}

func torrentMetaInfo(data []byte) (*metainfo.MetaInfo, metainfo.Info, error) {
	mi, info, _, err := torrent.ParseMetaInfoBytes(data)
	if err != nil {
		return nil, metainfo.Info{}, fmt.Errorf("invalid torrent metainfo: %w", err)
	}
	if err := validateTorrentInfo(info); err != nil {
		return nil, metainfo.Info{}, fmt.Errorf("invalid torrent metainfo: %w", err)
	}
	if info.HasV2() {
		if err := validateTorrentPieceLayers(mi.PieceLayers, info); err != nil {
			return nil, metainfo.Info{}, fmt.Errorf("invalid torrent metainfo: bad v2 piece layers: %w", err)
		}
	}
	return mi, *info, nil
}

func validateTorrentInfo(info *metainfo.Info) error {
	if len(info.Pieces)%metainfo.HashSize != 0 {
		return fmt.Errorf("pieces has invalid length")
	}
	v1Total, err := v1TorrentLength(info)
	if err != nil {
		return err
	}
	total := v1Total
	if info.HasV2() {
		if err := info.FileTree.Validate(); err != nil {
			return fmt.Errorf("bad v2 file tree: %w", err)
		}
		total, err = v2TorrentLength(&info.FileTree)
		if err != nil {
			return err
		}
	}
	if info.PieceLength <= 0 {
		if info.PieceLength < 0 || total != 0 || len(info.Pieces) != 0 || info.HasV2() {
			return fmt.Errorf("invalid piece length %d", info.PieceLength)
		}
		return nil
	}
	if info.PieceLength > maxTorrentProtocolPieceLength {
		return fmt.Errorf("piece length %d exceeds peer protocol limit", info.PieceLength)
	}
	if info.HasV2() && (info.PieceLength < 16<<10 || info.PieceLength&(info.PieceLength-1) != 0) {
		return fmt.Errorf("invalid v2 piece length %d", info.PieceLength)
	}
	v1Pieces := int64(len(info.Pieces) / metainfo.HashSize)
	if info.HasV1() {
		if ceilDivInt64(v1Total, info.PieceLength) != v1Pieces {
			return fmt.Errorf("piece count and file lengths are at odds")
		}
		if v1Pieces > maxTorrentPieceCount {
			return fmt.Errorf("torrent has %d pieces, limit is %d", v1Pieces, maxTorrentPieceCount)
		}
	}
	if info.HasV2() {
		v2Pieces, err := torrentInfoPieceCount(info)
		if err != nil {
			return err
		}
		if info.HasV1() {
			if v1Pieces != v2Pieces {
				return fmt.Errorf("hybrid piece counts differ: v1 has %d, v2 has %d", v1Pieces, v2Pieces)
			}
			if err := validateHybridTorrentLayout(info); err != nil {
				return err
			}
		}
	}
	return nil
}

func torrentInfoPieceCount(info *metainfo.Info) (int64, error) {
	if info.HasV2() {
		return validateV2TorrentGeometry(&info.FileTree, info.PieceLength)
	}
	return int64(len(info.Pieces) / metainfo.HashSize), nil
}

func validateV2TorrentGeometry(tree *metainfo.FileTree, pieceLength int64) (int64, error) {
	if pieceLength <= 0 {
		return 0, fmt.Errorf("invalid v2 piece length %d", pieceLength)
	}
	var pieces int64
	var paddedLength int64
	var geometryErr error
	tree.Walk(nil, func(path []string, node *metainfo.FileTree) {
		if geometryErr != nil || node.IsDir() {
			return
		}
		if node.File.Length < 0 {
			geometryErr = fmt.Errorf("file %q has negative length %d", strings.Join(path, "/"), node.File.Length)
			return
		}
		filePieces := ceilDivInt64(node.File.Length, pieceLength)
		if filePieces > maxTorrentPieceCount-pieces {
			geometryErr = fmt.Errorf("torrent has more than %d pieces at file %q", maxTorrentPieceCount, strings.Join(path, "/"))
			return
		}
		if filePieces > int64(^uint64(0)>>1)/pieceLength {
			geometryErr = fmt.Errorf("padded v2 file length overflows int64 at file %q", strings.Join(path, "/"))
			return
		}
		fileSpan := filePieces * pieceLength
		if fileSpan > int64(^uint64(0)>>1)-paddedLength {
			geometryErr = fmt.Errorf("padded v2 torrent length overflows int64 at file %q", strings.Join(path, "/"))
			return
		}
		pieces += filePieces
		paddedLength += fileSpan
	})
	return pieces, geometryErr
}

func validateHybridTorrentLayout(info *metainfo.Info) error {
	v1Files := make([]metainfo.FileInfo, 0, len(info.Files))
	for file := range info.UpvertedV1Files() {
		if strings.Contains(file.Attr, "p") {
			continue
		}
		v1Files = append(v1Files, file)
	}
	v2Files := info.UpvertedFiles()
	if len(v1Files) != len(v2Files) {
		return fmt.Errorf("hybrid file counts differ: v1 has %d, v2 has %d", len(v1Files), len(v2Files))
	}
	for i := range v1Files {
		v1Path := v1Files[i].BestPath()
		if len(info.Files) == 0 && len(v1Path) == 0 {
			v1Path = []string{info.BestName()}
		}
		v2Path := v2Files[i].BestPath()
		if !equalTorrentPath(v1Path, v2Path) {
			return fmt.Errorf("hybrid file %d paths differ: v1 %q, v2 %q", i+1, strings.Join(v1Path, "/"), strings.Join(v2Path, "/"))
		}
		if v1Files[i].Length != v2Files[i].Length {
			return fmt.Errorf("hybrid file %q lengths differ: v1 %d, v2 %d", strings.Join(v2Path, "/"), v1Files[i].Length, v2Files[i].Length)
		}
		if v1Files[i].TorrentOffset != v2Files[i].TorrentOffset {
			return fmt.Errorf("hybrid file %q offsets differ: v1 %d, v2 %d", strings.Join(v2Path, "/"), v1Files[i].TorrentOffset, v2Files[i].TorrentOffset)
		}
	}
	return nil
}

func equalTorrentPath(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func validateTorrentPieceLayers(pieceLayers map[string]string, info *metainfo.Info) error {
	var shapeErr error
	padHash := torrentPiecePadHash(info.PieceLength)
	validated := make(map[string]struct{}, len(pieceLayers))
	info.FileTree.Walk(nil, func(path []string, node *metainfo.FileTree) {
		if shapeErr != nil || node.IsDir() {
			return
		}
		piecesRoot := node.PiecesRootAsByteArray()
		if !piecesRoot.Ok {
			return
		}
		layer, ok := pieceLayers[string(piecesRoot.Value[:])]
		expectedHashes := ceilDivInt64(node.File.Length, info.PieceLength)
		if expectedHashes <= 1 {
			if ok {
				shapeErr = fmt.Errorf("unexpected piece layer for file %q", strings.Join(path, "/"))
			}
			return
		}
		if !ok {
			shapeErr = fmt.Errorf("no piece layers for file %q", strings.Join(path, "/"))
			return
		}
		expectedBytes := expectedHashes * 32
		if int64(len(layer)) != expectedBytes {
			shapeErr = fmt.Errorf("file %q piece layer has %d bytes, expected %d", strings.Join(path, "/"), len(layer), expectedBytes)
			return
		}
		rootKey := string(piecesRoot.Value[:])
		if _, ok := validated[rootKey]; ok {
			return
		}
		hashes, err := merkle.CompactLayerToSliceHashes(layer)
		if err != nil {
			shapeErr = fmt.Errorf("file %q: %w", strings.Join(path, "/"), err)
			return
		}
		if root := merkle.RootWithPadHash(hashes, padHash); root != piecesRoot.Value {
			shapeErr = fmt.Errorf("file %q: expected hash %x got %x", strings.Join(path, "/"), piecesRoot.Value, root)
			return
		}
		validated[rootKey] = struct{}{}
	})
	if shapeErr == nil {
		for root := range pieceLayers {
			if _, ok := validated[root]; !ok {
				return fmt.Errorf("piece layer root %x is not referenced by the file tree", []byte(root))
			}
		}
	}
	return shapeErr
}

func validateRefreshedTorrentMetadata(mi metainfo.MetaInfo, info *metainfo.Info, magnet bool) error {
	if err := validateTorrentInfo(info); err != nil {
		return fmt.Errorf("invalid torrent metadata: %w", err)
	}
	// BEP 52 piece layers are transferred separately from the BEP 9 info
	// dictionary. A magnet download can therefore have usable metadata before
	// its piece layers have arrived; the torrent client validates those layers
	// when it receives them. Direct metainfo must be complete immediately.
	if info.HasV2() && !magnet {
		if err := validateTorrentPieceLayers(mi.PieceLayers, info); err != nil {
			return fmt.Errorf("invalid torrent metadata: bad v2 piece layers: %w", err)
		}
	}
	return nil
}

func torrentPiecePadHash(pieceLength int64) (hash [sha256.Size]byte) {
	var pair [2 * sha256.Size]byte
	for blocks := pieceLength / merkle.BlockSize; blocks > 1; blocks >>= 1 {
		copy(pair[:sha256.Size], hash[:])
		copy(pair[sha256.Size:], hash[:])
		hash = sha256.Sum256(pair[:])
	}
	return hash
}

func v1TorrentLength(info *metainfo.Info) (int64, error) {
	if info.Length < 0 {
		return 0, fmt.Errorf("negative torrent length %d", info.Length)
	}
	if len(info.Files) == 0 {
		return info.Length, nil
	}
	var total int64
	for _, file := range info.Files {
		if file.Length < 0 {
			return 0, fmt.Errorf("file %q has negative length %d", strings.Join(file.BestPath(), "/"), file.Length)
		}
		if file.Length > int64(^uint64(0)>>1)-total {
			return 0, fmt.Errorf("torrent file lengths overflow int64")
		}
		total += file.Length
	}
	return total, nil
}

func v2TorrentLength(tree *metainfo.FileTree) (int64, error) {
	var total int64
	var lengthErr error
	tree.Walk(nil, func(path []string, node *metainfo.FileTree) {
		if lengthErr != nil || node.IsDir() {
			return
		}
		if node.File.Length < 0 {
			lengthErr = fmt.Errorf("file %q has negative length %d", strings.Join(path, "/"), node.File.Length)
			return
		}
		if node.File.Length > int64(^uint64(0)>>1)-total {
			lengthErr = fmt.Errorf("torrent file lengths overflow int64")
			return
		}
		total += node.File.Length
	})
	return total, lengthErr
}

func decodeTorrentParamUpTo(s string, maxSize int64) ([]byte, error) {
	decode := func(encoding *base64.Encoding) ([]byte, error) {
		limit := maxSize
		if limit < int64(^uint64(0)>>1) {
			limit++
		}
		data, err := io.ReadAll(io.LimitReader(base64.NewDecoder(encoding, strings.NewReader(s)), limit))
		if int64(len(data)) > maxSize {
			return nil, &torrentSizeError{max: maxSize}
		}
		return data, err
	}
	data, err := decode(base64.StdEncoding)
	if err == nil {
		return data, nil
	}
	var sizeErr *torrentSizeError
	if errors.As(err, &sizeErr) {
		return nil, err
	}
	data, rawErr := decode(base64.RawStdEncoding)
	if rawErr == nil {
		return data, nil
	}
	if errors.As(rawErr, &sizeErr) {
		return nil, rawErr
	}
	return nil, fmt.Errorf("invalid base64 torrent: %w", err)
}

func newMagnetDownload(gid, raw string, uris []string, opts Options) (*Download, error) {
	spec, err := torrent.TorrentSpecFromMagnetUri(raw)
	if err != nil {
		return nil, err
	}
	external := make([]URIInfo, len(uris))
	for i, uri := range uris {
		external[i].URI = uri
	}
	if _, err := mergeTorrentWebSeedURLs(spec.Webseeds, external); err != nil {
		return nil, err
	}
	d, err := newTorrentDownload(gid, nil, raw, uris, opts, nil, metainfo.Info{
		Name: spec.DisplayName,
	})
	if err != nil {
		return nil, err
	}
	d.infoHash = canonicalTorrentSpecInfoHash(spec).HexString()
	return d, nil
}

func canonicalTorrentSpecInfoHash(spec *torrent.TorrentSpec) metainfo.Hash {
	if spec == nil {
		return metainfo.Hash{}
	}
	if spec.InfoHash != (metainfo.Hash{}) {
		return spec.InfoHash
	}
	if spec.InfoHashV2.Ok {
		return *spec.InfoHashV2.Value.ToShort()
	}
	return metainfo.Hash{}
}

func ensureTorrentSpecShortInfoHash(spec *torrent.TorrentSpec) {
	if spec != nil && spec.InfoHash == (metainfo.Hash{}) && spec.InfoHashV2.Ok {
		// v1.54.8's AddTorrentOpt indexes all new torrents by a non-zero
		// 20-byte hash. For a v2-only torrent that key is the BEP 52 short
		// infohash. Passing both that key and the same full v2 hash makes the
		// fork reject the duplicate identity, so let its metadata verifier
		// recognize the short key and install the full v2 hash once available.
		spec.InfoHash = *spec.InfoHashV2.Value.ToShort()
		spec.InfoHashV2.SetNone()
	}
}

func canonicalTorrentMetaInfoHash(mi *metainfo.MetaInfo, info metainfo.Info) metainfo.Hash {
	if mi == nil {
		return metainfo.Hash{}
	}
	if info.HasV1() {
		return mi.HashInfoBytes()
	}
	v2 := infohash_v2.HashBytes(mi.InfoBytes)
	return *v2.ToShort()
}

func newTorrentDownload(gid string, data []byte, magnet string, uris []string, opts Options, mi *metainfo.MetaInfo, info metainfo.Info) (*Download, error) {
	if err := validateTorrentInfo(&info); err != nil {
		return nil, fmt.Errorf("invalid torrent metadata: %w", err)
	}
	numPieces, err := torrentInfoPieceCount(&info)
	if err != nil {
		return nil, fmt.Errorf("invalid torrent metadata: %w", err)
	}
	infos := make([]URIInfo, 0, len(uris))
	for i, raw := range uris {
		status := URIStatusWaiting
		if i == 0 {
			status = URIStatusUsed
		}
		infos = append(infos, URIInfo{URI: raw, Status: status})
	}
	if mi != nil {
		if _, err := mergeTorrentWebSeedURLs(mi.UrlList, infos); err != nil {
			return nil, err
		}
	}
	dir := optionString(opts, "dir")
	if dir == "" {
		dir = "."
	}
	infoHash := canonicalTorrentMetaInfoHash(mi, info)
	layoutOpts := opts
	if magnet != "" && mi == nil {
		layoutOpts = nil
	}
	files, err := torrentFileStates(info, infoHash, layoutOpts, dir)
	if err != nil {
		return nil, err
	}
	total := torrentSelectedLength(files)
	d := &Download{
		kind:          downloadKindTorrent,
		gid:           gid,
		uris:          infos,
		options:       opts,
		status:        StatusWaiting,
		dir:           dir,
		totalLength:   total,
		pieceLength:   info.PieceLength,
		numPieces:     numPieces,
		torrentFiles:  files,
		torrentData:   append([]byte(nil), data...),
		torrentMagnet: magnet,
		bittorrent:    bittorrentInfo(mi, info),
		createdAt:     time.Now(),
	}
	if mi != nil {
		d.infoHash = infoHash.HexString()
	}
	d.bitfield = unknownTorrentBitfield(d.numPieces)
	return d, nil
}

func unknownTorrentBitfield(numPieces int64) string {
	bytesLen := ceilDivInt64(numPieces, 8)
	if bytesLen <= 0 || bytesLen > int64(int(^uint(0)>>1))/2 {
		return ""
	}
	return strings.Repeat("00", int(bytesLen))
}

func bittorrentInfo(mi *metainfo.MetaInfo, info metainfo.Info) *BittorrentInfo {
	mode := "single"
	if len(info.Files) > 0 || info.HasV2() && info.IsDir() {
		mode = "multi"
	}
	bt := &BittorrentInfo{
		Mode: mode,
		Info: map[string]interface{}{
			"name": info.BestName(),
		},
	}
	if mi != nil {
		bt.AnnounceList = mi.UpvertedAnnounceList()
		bt.Comment = mi.Comment
		bt.CreationDate = mi.CreationDate
	}
	return bt
}

func torrentFileStates(info metainfo.Info, infoHash metainfo.Hash, opts Options, dir string) ([]torrentFileState, error) {
	selected, _, err := torrentSelectionAndIndexOut(info, opts)
	if err != nil {
		return nil, err
	}
	plan, err := storage.PlanFiles(torrentFileClientOpts(dir, opts), &info, infoHash)
	if err != nil {
		return nil, err
	}
	canonicalDir, err := outputPathReservationKey(plan.TorrentDir)
	if err != nil {
		return nil, err
	}
	out := make([]torrentFileState, 0, len(plan.Files))
	for _, file := range plan.Files {
		canonicalPath, err := outputPathReservationKey(file.StoragePath)
		if err != nil {
			return nil, err
		}
		if !pathWithinDirectory(canonicalDir, canonicalPath) {
			return nil, fmt.Errorf("torrent output path escapes download directory: %s", file.StoragePath)
		}
		index := file.Index + 1
		out = append(out, torrentFileState{
			Index:    index,
			Path:     file.StoragePath,
			Length:   file.Length,
			Selected: selected[index],
		})
	}
	return out, nil
}

func pathWithinDirectory(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func safeTorrentRelPath(path string) string {
	parts := strings.FieldsFunc(filepath.ToSlash(path), func(r rune) bool {
		return r == '/'
	})
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			continue
		}
		clean = append(clean, part)
	}
	if len(clean) == 0 {
		return "torrent"
	}
	return filepath.Join(clean...)
}

func torrentSelectedLength(files []torrentFileState) int64 {
	var total int64
	for _, f := range files {
		if f.Selected {
			total += f.Length
		}
	}
	return total
}

func validateTorrentFinalizedIndexes(files []torrentFileState, finalized map[int]struct{}) error {
	if len(finalized) == 0 {
		return nil
	}
	valid := make(map[int]struct{}, len(files))
	for _, file := range files {
		valid[file.Index] = struct{}{}
	}
	for index := range finalized {
		if _, ok := valid[index]; !ok {
			return fmt.Errorf("finalized torrent file index %d out of range", index)
		}
	}
	return nil
}

func torrentFileStateProgress(files []torrentFileState, finalized map[int]struct{}) (total, completed int64) {
	for i := range files {
		if _, ok := finalized[files[i].Index]; ok {
			files[i].Completed = files[i].Length
		} else {
			if files[i].Completed < 0 {
				files[i].Completed = 0
			}
			if files[i].Completed > files[i].Length {
				files[i].Completed = files[i].Length
			}
		}
		if files[i].Selected {
			total += files[i].Length
			completed += files[i].Completed
		}
	}
	return total, completed
}

func (d *Download) restoreTorrentFinalized(indexes map[int]struct{}) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.torrentMagnet == "" {
		if err := validateTorrentFinalizedIndexes(d.torrentFiles, indexes); err != nil {
			return err
		}
	}
	d.torrentFinalized = maps.Clone(indexes)
	d.totalLength, d.completedLen = torrentFileStateProgress(d.torrentFiles, d.torrentFinalized)
	// Without a live torrent there is no verified per-piece state. Preserve
	// completed byte/file accounting without fabricating a contiguous prefix.
	d.donePieces = 0
	d.bitfield = unknownTorrentBitfield(d.numPieces)
	return nil
}

func torrentSelectionAndIndexOut(info metainfo.Info, opts Options) (map[int]bool, map[int]string, error) {
	files := info.UpvertedFiles()
	selected, err := selectedFileSet(len(files), optionString(opts, "select-file"))
	if err != nil {
		return nil, nil, err
	}
	indexOut, err := parseIndexOut(opts)
	if err != nil {
		return nil, nil, err
	}
	for index := range indexOut {
		if index < 1 || index > len(files) {
			return nil, nil, fmt.Errorf("index-out file index %d out of range", index)
		}
	}
	for index := range selected {
		if index < 1 || index > len(files) {
			return nil, nil, fmt.Errorf("select-file index %d out of range", index)
		}
	}
	selected, err = applyTorrentExtensionSelection(info, selected, indexOut, opts)
	if err != nil {
		return nil, nil, err
	}
	return selected, indexOut, nil
}

func selectedFileSet(numFiles int, spec string) (map[int]bool, error) {
	selected := make(map[int]bool, numFiles)
	if strings.TrimSpace(spec) == "" {
		for i := 1; i <= numFiles; i++ {
			selected[i] = true
		}
		return selected, nil
	}
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, fmt.Errorf("invalid empty select-file item")
		}
		first, last, ok := strings.Cut(item, "-")
		if !ok {
			n, err := strconv.Atoi(strings.TrimSpace(item))
			if err != nil || n < 1 || n > numFiles {
				return nil, fmt.Errorf("select-file index %q out of range", item)
			}
			selected[n] = true
			continue
		}
		start, err1 := strconv.Atoi(strings.TrimSpace(first))
		end, err2 := strconv.Atoi(strings.TrimSpace(last))
		if err1 != nil || err2 != nil || start < 1 || end < start || end > numFiles {
			return nil, fmt.Errorf("invalid select-file range %q", item)
		}
		for i := start; i <= end; i++ {
			selected[i] = true
		}
	}
	return selected, nil
}

func parseIndexOut(opts Options) (map[int]string, error) {
	out := make(map[int]string)
	for _, raw := range optionStringList(opts, "index-out") {
		left, right, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("invalid index-out %q", raw)
		}
		index, err := strconv.Atoi(strings.TrimSpace(left))
		if err != nil || index <= 0 {
			return nil, fmt.Errorf("invalid index-out index %q", left)
		}
		path := strings.TrimSpace(right)
		if path == "" {
			return nil, fmt.Errorf("invalid empty index-out path")
		}
		out[index] = path
	}
	return out, nil
}

type torrentExtensionFilters struct {
	include map[string]struct{}
	exclude map[string]struct{}
}

func validateTorrentExtensionOptions(opts Options) error {
	_, err := parseTorrentExtensionFilters(opts)
	return err
}

func parseTorrentExtensionFilters(opts Options) (torrentExtensionFilters, error) {
	include, err := parseTorrentExtensionFilter(opts, torrentSelectFileExtOption)
	if err != nil {
		return torrentExtensionFilters{}, err
	}
	exclude, err := parseTorrentExtensionFilter(opts, torrentExcludeFileExtOption)
	if err != nil {
		return torrentExtensionFilters{}, err
	}
	return torrentExtensionFilters{include: include, exclude: exclude}, nil
}

func parseTorrentExtensionFilter(opts Options, key string) (map[string]struct{}, error) {
	exts := make(map[string]struct{})
	for _, raw := range splitCSVOptions(opts, key) {
		ext := strings.ToLower(strings.TrimSpace(raw))
		if ext == "" {
			continue
		}
		if ext == "." || strings.ContainsAny(ext, `/\`) || strings.ContainsRune(ext, '\x00') {
			return nil, fmt.Errorf("invalid %s extension %q", key, raw)
		}
		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		exts[ext] = struct{}{}
	}
	return exts, nil
}

func applyTorrentExtensionSelection(info metainfo.Info, selected map[int]bool, indexOut map[int]string, opts Options) (map[int]bool, error) {
	filters, err := parseTorrentExtensionFilters(opts)
	if err != nil {
		return nil, err
	}
	if len(filters.include) == 0 && len(filters.exclude) == 0 {
		return selected, nil
	}
	files := info.UpvertedFiles()
	out := make(map[int]bool, len(selected))
	for i, fi := range files {
		index := i + 1
		if !selected[index] {
			continue
		}
		path := fi.DisplayPath(&info)
		if override := indexOut[index]; override != "" {
			path = override
		}
		if filters.matches(path) {
			out[index] = true
		}
	}
	return out, nil
}

func (f torrentExtensionFilters) matches(path string) bool {
	lowerPath := strings.ToLower(filepath.ToSlash(path))
	if len(f.include) > 0 && !matchesTorrentExtension(lowerPath, f.include) {
		return false
	}
	if len(f.exclude) > 0 && matchesTorrentExtension(lowerPath, f.exclude) {
		return false
	}
	return true
}

func matchesTorrentExtension(path string, exts map[string]struct{}) bool {
	for ext := range exts {
		if strings.HasSuffix(path, ext) {
			return true
		}
	}
	return false
}

func applyTorrentClientIdentityOptions(cfg *torrent.ClientConfig, opts Options) error {
	profile := strings.ToLower(strings.TrimSpace(optionString(opts, "goaria-torrent-peer-profile")))
	if profile == "" {
		profile = torrentPeerProfileQBitTorrent
	}
	switch profile {
	case torrentPeerProfileQBitTorrent, "qbt", "qbit":
		cfg.Bep20 = qBittorrentBep20Prefix
		cfg.HTTPUserAgent = qBittorrentPeerVisibleVersion
		cfg.ExtendedHandshakeClientVersion = qBittorrentPeerVisibleVersion
	case torrentPeerProfileNative, "anacrolix", "goaria":
		if userAgent := optionString(opts, "user-agent"); userAgent != "" {
			cfg.HTTPUserAgent = userAgent
		}
	default:
		return fmt.Errorf("invalid goaria-torrent-peer-profile %q", profile)
	}
	if prefix := optionString(opts, "goaria-torrent-bep20-prefix"); prefix != "" {
		if len(prefix) > 20 {
			return fmt.Errorf("goaria-torrent-bep20-prefix too long: %d bytes", len(prefix))
		}
		cfg.Bep20 = prefix
	}
	if userAgent := optionString(opts, "goaria-torrent-user-agent"); userAgent != "" {
		cfg.HTTPUserAgent = userAgent
	}
	if version := optionString(opts, "goaria-torrent-client-version"); version != "" {
		cfg.ExtendedHandshakeClientVersion = version
	}
	return nil
}

func torrentInfoPrivate(info metainfo.Info) bool {
	return info.Private != nil && *info.Private
}

func applyTorrentPeerDiscoveryOptions(cfg *torrent.ClientConfig, opts Options, privateTorrent bool) {
	cfg.NoDHT = optionBool(opts, "goaria-disable-dht")
	cfg.DisableTrackers = optionBool(opts, "goaria-disable-trackers")
	cfg.DisablePEX = optionBool(opts, "goaria-disable-pex")
	if !privateTorrent {
		return
	}
	if !optionExplicit(opts, "goaria-disable-dht") {
		cfg.NoDHT = true
	}
	if !optionExplicit(opts, "goaria-disable-pex") {
		cfg.DisablePEX = true
	}
}

func (r *torrentRuntime) markSelectionCompleted(index int) {
	r.selectionMu.Lock()
	defer r.selectionMu.Unlock()
	if r.selection == nil || r.selectionClosed {
		return
	}
	if r.selectionCompleted == nil {
		r.selectionCompleted = make(map[int]struct{})
	}
	r.selectionCompleted[index] = struct{}{}
	if r.selectionAllCompletedLocked() {
		r.closeSelectionLocked()
	}
}

func (r *torrentRuntime) selectionAllCompletedLocked() bool {
	if len(r.selectionSelected) == 0 {
		return false
	}
	for index := range r.selectionSelected {
		if _, ok := r.selectionCompleted[index]; !ok {
			return false
		}
	}
	return true
}

func (r *torrentRuntime) closeSelectionLocked() {
	if r.selectionClosed {
		return
	}
	r.selectionClosed = true
	r.selection.Close()
}

func torrentFileIndexSet(indexes []int) map[int]struct{} {
	out := make(map[int]struct{}, len(indexes))
	for _, index := range indexes {
		out[index] = struct{}{}
	}
	return out
}

func torrentFileClientOpts(dir string, opts Options) storage.NewFileClientOpts {
	return storage.NewFileClientOpts{
		ClientBaseDir:      dir,
		PieceCompletion:    storage.NewMapPieceCompletion(),
		UsePartFiles:       g.Some(false),
		ForceClassicFileIO: true,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		FilePathMaker:      torrentFilePathMaker(opts),
	}
}

type validatingTorrentStorage struct {
	delegate storage.ClientImpl
}

func (s validatingTorrentStorage) OpenTorrent(ctx context.Context, info *metainfo.Info, infoHash metainfo.Hash) (storage.TorrentImpl, error) {
	if err := validateTorrentInfo(info); err != nil {
		return storage.TorrentImpl{}, fmt.Errorf("invalid torrent metadata: %w", err)
	}
	return s.delegate.OpenTorrent(ctx, info, infoHash)
}

func (e *Engine) runTorrentDownload(d *Download) error {
	d.mu.RLock()
	ctx := d.ctx
	opts := d.options
	data := append([]byte(nil), d.torrentData...)
	magnet := d.torrentMagnet
	webseeds := slices.Clone(d.uris)
	dir := d.dir
	d.mu.RUnlock()
	if ctx == nil {
		return context.Canceled
	}
	ctx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	var mi *metainfo.MetaInfo
	privateTorrent := false
	if magnet == "" {
		var info metainfo.Info
		var err error
		mi, info, err = torrentMetaInfo(data)
		if err != nil {
			return err
		}
		privateTorrent = torrentInfoPrivate(info)
	}
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = dir
	cfg.NoDefaultPortForwarding = true
	if err := applyTorrentClientIdentityOptions(cfg, opts); err != nil {
		return err
	}
	fileStorage := storage.NewFileOpts(torrentFileClientOpts(dir, opts))
	defer fileStorage.Close()
	cfg.DefaultStorage = validatingTorrentStorage{delegate: fileStorage}
	if optionBool(opts, "goaria-torrent-no-upload") {
		cfg.NoUpload = true
	}
	if optionBool(opts, "goaria-torrent-disable-smart-ban") {
		cfg.DisableSmartBanning = true
	}
	cfg.Slogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	applyTorrentPeerDiscoveryOptions(cfg, opts, privateTorrent)
	cfg.DisableUTP = optionBool(opts, "disable-utp") || optionBool(opts, "goaria-disable-utp")
	cfg.DisableIPv6 = optionBool(opts, "disable-ipv6")
	if maxPeers := optionInt(opts, "bt-max-peers", 0); maxPeers > 0 {
		cfg.EstablishedConnsPerTorrent = maxPeers
		cfg.TorrentPeersHighWater = maxPeers
		cfg.TorrentPeersLowWater = maxPeers / 2
	}
	applyTorrentRateLimitOptions(cfg, opts)
	cfg.ListenPort = optionInt(opts, "listen-port", 0)
	if host := optionString(opts, "goaria-listen-host"); host != "" {
		cfg.ListenHost = func(string) string { return host }
	}
	cl, err := torrent.NewClient(cfg)
	if err != nil {
		return err
	}
	defer cl.Close()

	var spec *torrent.TorrentSpec
	if magnet != "" {
		spec, err = torrent.TorrentSpecFromMagnetUri(magnet)
		if err != nil {
			return err
		}
	} else {
		spec, err = torrent.TorrentSpecFromMetaInfoErr(mi)
		if err != nil {
			return err
		}
	}
	applyTorrentTrackerOptions(spec, opts)
	webSeedURLs, err := extractTorrentWebSeedURLs(spec, webseeds)
	if err != nil {
		return err
	}
	ensureTorrentSpecShortInfoHash(spec)
	tor, _, err := cl.AddTorrentSpec(spec)
	if err != nil {
		return err
	}
	e.installTorrentWriteChunkErrorHandler(d, tor, cancelRun)
	if len(webSeedURLs) > 0 {
		tor.AddWebSeeds(webSeedURLs, e.torrentWebSeedOptions(opts)...)
	}
	if peers := optionStringList(opts, "goaria-peer-addrs"); len(peers) > 0 {
		spec := torrent.TorrentSpec{PeerAddrs: peers}
		if err := tor.MergeSpec(&spec); err != nil {
			return err
		}
	}
	d.mu.Lock()
	d.torrent = &torrentRuntime{client: cl, torrent: tor}
	d.mu.Unlock()

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-tor.GotInfo():
	}
	if err := e.refreshTorrentMetadata(d, tor); err != nil {
		return err
	}
	if e.cfg.TorrentFileHandler != nil {
		e.reconcileReleasedTorrentFiles(d, tor)
	}
	opts = d.optionSnapshot()
	if optionBool(opts, "bt-metadata-only") {
		e.updateTorrentProgress(d, tor)
		e.notify("aria2.onBtDownloadComplete", d.gid)
		return nil
	}
	if err := e.reserveTorrentOutputPaths(d); err != nil {
		return err
	}

	stopProgress := e.startTorrentProgress(ctx, d, tor)
	defer stopProgress()

	files := tor.Files()
	selected := selectedTorrentFiles(d, files)
	if e.cfg.TorrentFileHandler != nil {
		if err := e.processCompletedTorrentFiles(ctx, d, tor); err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			return err
		}
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		e.notify("aria2.onBtDownloadComplete", d.gid)
		return nil
	}
	for _, item := range selected {
		item.Download()
	}
	if len(selected) == 0 {
		e.notify("aria2.onBtDownloadComplete", d.gid)
		return nil
	}
	for _, item := range selected {
		if err := item.WaitVerifiedComplete(ctx); err != nil {
			item.SetPriority(torrent.PiecePriorityNone)
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			return err
		}
		e.updateTorrentProgress(d, tor)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	e.notify("aria2.onBtDownloadComplete", d.gid)
	return nil
}

func applyTorrentRateLimitOptions(cfg *torrent.ClientConfig, opts Options) {
	if uploadLimit := optionBytes(opts, "max-upload-limit", 0); uploadLimit > 0 {
		// The client selects a chunk-safe burst when zero is supplied. This also
		// avoids converting an arbitrary byte rate to int or granting a burst as
		// large as the configured per-second limit.
		cfg.UploadRateLimiter = rate.NewLimiter(rate.Limit(uploadLimit), 0)
	}
	downloadLimit := optionBytes(opts, "max-download-limit", 0)
	if overallLimit := optionBytes(opts, "max-overall-download-limit", 0); overallLimit > 0 && (downloadLimit <= 0 || overallLimit < downloadLimit) {
		downloadLimit = overallLimit
	}
	if downloadLimit > 0 {
		// The torrent client replaces a zero burst with a bounded default suited
		// to peer and webseed reads. Avoid converting an arbitrary byte limit to
		// int or making the burst scale without bound with the configured rate.
		cfg.DownloadRateLimiter = rate.NewLimiter(rate.Limit(downloadLimit), 0)
	}
}

func (e *Engine) installTorrentWriteChunkErrorHandler(d *Download, tor *torrent.Torrent, cancel context.CancelCauseFunc) {
	tor.SetOnWriteChunkError(func(err error) {
		if errors.Is(err, context.Canceled) {
			e.log.Debug("ignoring canceled torrent write",
				zap.String("gid", d.gid),
				zap.Error(err),
			)
			return
		}
		e.log.Error("torrent write chunk error; disabling data download",
			zap.String("gid", d.gid),
			zap.Error(err),
		)
		tor.DisallowDataDownload()
		cancel(fmt.Errorf("torrent write chunk error: %w", err))
	})
}

func applyTorrentTrackerOptions(spec *torrent.TorrentSpec, opts Options) {
	if exclude := splitCSVOptions(opts, "bt-exclude-tracker"); len(exclude) > 0 {
		excluded := make(map[string]struct{}, len(exclude))
		removeAll := false
		for _, tracker := range exclude {
			if tracker == "*" {
				removeAll = true
				break
			}
			excluded[tracker] = struct{}{}
		}
		filtered := spec.Trackers[:0]
		if !removeAll {
			for _, tier := range spec.Trackers {
				outTier := tier[:0]
				for _, tracker := range tier {
					if _, ok := excluded[tracker]; !ok {
						outTier = append(outTier, tracker)
					}
				}
				if len(outTier) > 0 {
					filtered = append(filtered, outTier)
				}
			}
		}
		spec.Trackers = filtered
	}
	if trackers := splitCSVOptions(opts, "bt-tracker"); len(trackers) > 0 {
		spec.Trackers = append(spec.Trackers, trackers)
	}
}

func extractTorrentWebSeedURLs(spec *torrent.TorrentSpec, uris []URIInfo) ([]string, error) {
	urls, err := mergeTorrentWebSeedURLs(spec.Webseeds, uris)
	// TorrentSpec webseeds are otherwise installed during MergeSpec without
	// the per-download HTTP client and request director configured by Goaria.
	spec.Webseeds = nil
	return urls, err
}

func mergeTorrentWebSeedURLs(embedded []string, uris []URIInfo) ([]string, error) {
	seen := make(map[string]struct{}, min(len(embedded)+len(uris), maxTorrentWebSeeds))
	urls := make([]string, 0, min(len(embedded)+len(uris), maxTorrentWebSeeds))
	appendUnique := func(raw string) error {
		if _, ok := seen[raw]; ok {
			return nil
		}
		if len(urls) >= maxTorrentWebSeeds {
			return fmt.Errorf("too many torrent webseeds: maximum is %d", maxTorrentWebSeeds)
		}
		seen[raw] = struct{}{}
		urls = append(urls, raw)
		return nil
	}
	for _, raw := range embedded {
		if err := appendUnique(raw); err != nil {
			return nil, err
		}
	}
	for _, uri := range uris {
		if err := appendUnique(uri.URI); err != nil {
			return nil, err
		}
	}
	return urls, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func (e *Engine) torrentWebSeedOptions(opts Options) []torrent.AddWebSeedsOpt {
	opts = cloneOptions(opts)
	return []torrent.AddWebSeedsOpt{
		torrent.WebSeedHttpClient(&http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return e.do(req, opts)
			}),
		}),
		torrent.WebSeedHttpRequestDirector(func(req *http.Request) error {
			applyWebSeedRequestOptions(req, opts)
			return nil
		}),
	}
}

func applyWebSeedRequestOptions(req *http.Request, opts Options) {
	rangeHeader := req.Header.Get("Range")
	applyRequestOptions(req, opts)
	req.Header.Del("Range")
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	req.Header.Set("Accept-Encoding", "identity")
}

func splitCSVOptions(opts Options, key string) []string {
	var out []string
	for _, raw := range optionStringList(opts, key) {
		for _, item := range strings.Split(raw, ",") {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
	}
	return out
}

func torrentFilePathMaker(opts Options) storage.FilePathMaker {
	indexOut, _ := parseIndexOut(opts)
	if len(indexOut) == 0 {
		return nil
	}
	return func(pathOpts storage.FilePathMakerOpts) (string, error) {
		if override := indexOut[pathOpts.FileIndex+1]; override != "" {
			return safeTorrentRelPath(override), nil
		}
		if pathOpts.DefaultPath != "" {
			return pathOpts.DefaultPath, nil
		}
		var parts []string
		if pathOpts.Info.BestName() != metainfo.NoName {
			parts = append(parts, pathOpts.Info.BestName())
		}
		return filepath.Join(append(parts, pathOpts.File.BestPath()...)...), nil
	}
}

func (e *Engine) refreshTorrentMetadata(d *Download, tor *torrent.Torrent) error {
	mi := tor.Metainfo()
	info, err := mi.UnmarshalInfo()
	if err != nil {
		return err
	}
	if err := validateRefreshedTorrentMetadata(mi, &info, d.torrentMagnet != ""); err != nil {
		return err
	}
	numPieces, err := torrentInfoPieceCount(&info)
	if err != nil {
		return fmt.Errorf("invalid torrent metadata: %w", err)
	}
	donePieces, bitfield := torrentPieceProgress(tor.PieceStateRuns())
	d.mu.Lock()
	defer d.mu.Unlock()
	opts := d.options
	if _, _, err := torrentSelectionAndIndexOut(info, opts); err != nil {
		return err
	}
	files, err := torrentFileStates(info, tor.InfoHash(), opts, d.dir)
	if err != nil {
		return err
	}
	if err := validateTorrentFinalizedIndexes(files, d.torrentFinalized); err != nil {
		return err
	}
	d.infoHash = tor.InfoHash().HexString()
	d.pieceLength = info.PieceLength
	d.numPieces = numPieces
	d.bittorrent = bittorrentInfo(&mi, info)
	d.torrentFiles = files
	d.totalLength, d.completedLen = torrentFileStateProgress(d.torrentFiles, d.torrentFinalized)
	d.donePieces = donePieces
	d.bitfield = bitfield
	if len(d.torrentData) == 0 {
		// Keep the metainfo for magnet downloads so metadata queries keep working
		// after the torrent runtime is released.
		var buf bytes.Buffer
		if err := bencode.NewEncoder(&buf).Encode(mi); err == nil {
			d.torrentData = buf.Bytes()
		}
	}
	return nil
}

func selectedTorrentFiles(d *Download, files []*torrent.File) []*torrent.File {
	d.mu.RLock()
	states := append([]torrentFileState(nil), d.torrentFiles...)
	finalized := maps.Clone(d.torrentFinalized)
	d.mu.RUnlock()
	out := make([]*torrent.File, 0, len(files))
	for i, f := range files {
		if i >= len(states) || !states[i].Selected {
			continue
		}
		if _, ok := finalized[states[i].Index]; ok {
			continue
		}
		out = append(out, f)
	}
	return out
}

func (e *Engine) reconcileReleasedTorrentFiles(d *Download, tor *torrent.Torrent) {
	snapshots := tor.FileSnapshots()
	reconciled := 0
	d.mu.Lock()
	for i := range d.torrentFiles {
		if i >= len(snapshots) || !snapshots[i].Released {
			continue
		}
		state := &d.torrentFiles[i]
		if _, ok := d.torrentFinalized[state.Index]; ok {
			continue
		}
		if d.torrentFinalized == nil {
			d.torrentFinalized = make(map[int]struct{})
		}
		d.torrentFinalized[state.Index] = struct{}{}
		state.Completed = state.Length
		reconciled++
	}
	if reconciled != 0 {
		d.totalLength, d.completedLen = torrentFileStateProgress(d.torrentFiles, d.torrentFinalized)
	}
	d.mu.Unlock()
	if reconciled == 0 {
		return
	}
	e.log.Info("reconciled released torrent files",
		zap.String("gid", d.gid),
		zap.Int("files", reconciled),
	)
	e.queueSessionSaveBestEffort()
}

func (e *Engine) reserveTorrentOutputPaths(d *Download) error {
	d.mu.RLock()
	paths := selectedTorrentOutputPaths(d.torrentFiles, d.torrentFinalized)
	gid := d.gid
	d.mu.RUnlock()
	return e.reserveOutputPaths(paths, gid)
}

func selectedTorrentOutputPaths(states []torrentFileState, finalized map[int]struct{}) []string {
	paths := make([]string, 0, len(states))
	for _, state := range states {
		if !state.Selected {
			continue
		}
		if _, done := finalized[state.Index]; done {
			continue
		}
		paths = append(paths, state.Path)
	}
	return paths
}

func selectedTorrentFileIndexesFromStates(states []torrentFileState, finalized map[int]struct{}) []int {
	indexes := make([]int, 0, len(states))
	for i, state := range states {
		if !state.Selected {
			continue
		}
		if _, ok := finalized[state.Index]; ok {
			continue
		}
		indexes = append(indexes, i)
	}
	return indexes
}

func (e *Engine) processCompletedTorrentFiles(ctx context.Context, d *Download, tor *torrent.Torrent) error {
	d.mu.Lock()
	indexes := selectedTorrentFileIndexesFromStates(d.torrentFiles, d.torrentFinalized)
	if len(indexes) == 0 {
		d.mu.Unlock()
		return nil
	}
	opts := cloneOptions(d.options)
	selection := stream.NewFileSelectionController(indexes)
	runtime := &torrentRuntime{selection: selection}
	if d.torrent != nil {
		runtime = d.torrent
		runtime.selectionMu.Lock()
		runtime.selection = selection
		runtime.selectionSelected = torrentFileIndexSet(indexes)
		runtime.selectionCompleted = make(map[int]struct{}, len(indexes))
		runtime.selectionClosed = false
		runtime.selectionMu.Unlock()
	}
	d.mu.Unlock()
	defer func() {
		runtime.selectionMu.Lock()
		if runtime.selection == selection {
			runtime.closeSelectionLocked()
			runtime.selection = nil
			runtime.selectionSelected = nil
			runtime.selectionCompleted = nil
		}
		runtime.selectionMu.Unlock()
	}()
	return stream.Files(ctx, tor, stream.FilesOptions{
		Selection:              selection,
		MaxActive:              optionInt(opts, "goaria-torrent-max-active-files", min(len(indexes), defaultTorrentHandlerWorkers)),
		Readahead:              optionBytes(opts, "goaria-torrent-readahead", 4<<20),
		RequireExplicitRelease: true,
	}, func(ctx context.Context, lease *stream.FileLease) error {
		e.updateTorrentProgress(d, tor)
		handlerErr := e.handleCompletedTorrentFile(ctx, d, lease)
		if handlerErr != nil {
			return handlerErr
		}
		if err := waitTorrentFileLeaseFinalized(ctx, lease); err != nil {
			return err
		}
		if err := e.markTorrentFileFinalized(d, runtime, lease.Index); err != nil {
			return err
		}
		e.updateTorrentProgress(d, tor)
		return nil
	})
}

func waitTorrentFileLeaseFinalized(ctx context.Context, lease *stream.FileLease) error {
	if lease.Finalized() {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Prefer a successful finalization that raced with cancellation.
			if lease.Finalized() {
				return nil
			}
			return ctx.Err()
		case <-ticker.C:
			if lease.Finalized() {
				return nil
			}
		}
	}
}

func (e *Engine) markTorrentFileFinalized(d *Download, runtime *torrentRuntime, zeroBasedIndex int) error {
	ariaIndex := zeroBasedIndex + 1
	var runtimeDonePieces int64
	var runtimeBitfield string
	hasRuntimePieceState := runtime != nil && runtime.torrent != nil
	if hasRuntimePieceState {
		runtimeDonePieces, runtimeBitfield = torrentPieceProgress(runtime.torrent.PieceStateRuns())
	}
	d.mu.Lock()
	var state *torrentFileState
	for i := range d.torrentFiles {
		if d.torrentFiles[i].Index == ariaIndex {
			state = &d.torrentFiles[i]
			break
		}
	}
	if state == nil {
		d.mu.Unlock()
		return fmt.Errorf("finalized torrent file index %d out of range", ariaIndex)
	}
	_, alreadyFinalized := d.torrentFinalized[ariaIndex]
	if d.torrentFinalized == nil {
		d.torrentFinalized = make(map[int]struct{})
	}
	d.torrentFinalized[ariaIndex] = struct{}{}
	state.Completed = state.Length
	d.totalLength, d.completedLen = torrentFileStateProgress(d.torrentFiles, d.torrentFinalized)
	if hasRuntimePieceState {
		d.donePieces = runtimeDonePieces
		d.bitfield = runtimeBitfield
	} else {
		d.donePieces = 0
		d.bitfield = unknownTorrentBitfield(d.numPieces)
	}
	d.mu.Unlock()

	runtime.markSelectionCompleted(zeroBasedIndex)
	if !alreadyFinalized {
		e.queueSessionSaveBestEffort()
	}
	return nil
}

func (e *Engine) handleCompletedTorrentFile(ctx context.Context, d *Download, lease *stream.FileLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tf := TorrentFile{GID: d.gid, Lease: lease}
	handlerErr := e.cfg.TorrentFileHandler(ctx, tf)
	if handlerErr != nil {
		releaseErr := lease.Release(context.Background())
		e.updateTorrentProgress(d, lease.File.Torrent())
		return errors.Join(handlerErr, releaseErr)
	}
	return nil
}

func (e *Engine) startTorrentProgress(ctx context.Context, d *Download, tor *torrent.Torrent) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		lastCompleted := e.updateTorrentProgress(d, tor)
		var lastAt = time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case now := <-ticker.C:
				completed := e.updateTorrentProgress(d, tor)
				elapsed := now.Sub(lastAt)
				if elapsed > 0 {
					delta := completed - lastCompleted
					if delta < 0 {
						delta = 0
					}
					d.setDownloadBPS(bytesPerSecond(delta, elapsed))
				}
				lastCompleted = completed
				lastAt = now
			}
		}
	}()
	return func() {
		close(stop)
		<-done
		e.updateTorrentProgress(d, tor)
	}
}

func (e *Engine) updateTorrentProgress(d *Download, tor *torrent.Torrent) int64 {
	stats := tor.Stats()
	snapshots := tor.FileSnapshots()
	donePieces, bitfield := torrentPieceProgress(tor.PieceStateRuns())
	d.mu.Lock()
	defer d.mu.Unlock()
	var completed int64
	for i := range d.torrentFiles {
		var n int64
		_, finalized := d.torrentFinalized[d.torrentFiles[i].Index]
		if finalized {
			n = d.torrentFiles[i].Length
		} else if i >= len(snapshots) {
			n = d.torrentFiles[i].Completed
		} else if snapshots[i].Released {
			n = d.torrentFiles[i].Length
		} else {
			n = verifiedTorrentFileProgress(snapshots[i], d.torrentFiles[i].Length)
		}
		d.torrentFiles[i].Completed = n
		if d.torrentFiles[i].Selected {
			completed += n
		}
	}
	d.completedLen = completed
	d.connections = stats.ActivePeers + stats.HalfOpenPeers
	d.numSeeders = stats.ConnectedSeeders
	d.seeder = tor.Seeding()
	d.donePieces = donePieces
	d.bitfield = bitfield
	return completed
}

func verifiedTorrentFileProgress(snapshot torrent.FileSnapshot, length int64) int64 {
	n := snapshot.VerifiedCompleted
	if n < 0 {
		return 0
	}
	if n > length {
		return length
	}
	return n
}

func torrentPieceProgress(runs torrent.PieceStateRuns) (done int64, bitfield string) {
	pieces := 0
	for _, run := range runs {
		if run.Length <= 0 {
			continue
		}
		if run.Length > int(maxTorrentPieceCount)-pieces {
			return 0, ""
		}
		pieces += run.Length
	}
	if pieces == 0 {
		return 0, ""
	}
	bits := make([]byte, (pieces+7)/8)
	index := 0
	for _, run := range runs {
		if run.Length <= 0 {
			continue
		}
		end := index + run.Length
		if end < index || end > pieces {
			end = pieces
		}
		if run.Ok && run.Complete {
			for ; index < end; index++ {
				bits[index/8] |= 1 << uint(7-index%8)
				done++
			}
		} else {
			index = end
		}
		if index == pieces {
			break
		}
	}
	return done, hex.EncodeToString(bits)
}

func (d *Download) torrentPeersLocked() []map[string]string {
	if d.torrent == nil || d.torrent.torrent == nil {
		return []map[string]string{}
	}
	tor := d.torrent.torrent
	snapshots := tor.PeerSnapshots()
	out := make([]map[string]string, 0, len(snapshots))
	for _, peer := range snapshots {
		host, port, err := net.SplitHostPort(peer.RemoteAddr)
		if err != nil {
			host = peer.RemoteAddr
			port = "0"
		}
		stats := peer.Stats
		bitfield := ""
		if len(peer.RemoteBitfield) > 0 {
			bitfield = hex.EncodeToString(peer.RemoteBitfield)
		}
		out = append(out, map[string]string{
			"peerId":        url.QueryEscape(peer.PeerID.String()),
			"ip":            host,
			"port":          port,
			"bitfield":      bitfield,
			"amChoking":     strconv.FormatBool(peer.LocalChoking),
			"peerChoking":   strconv.FormatBool(peer.RemoteChoking),
			"downloadSpeed": strconv.FormatInt(int64(stats.DownloadRate), 10),
			"uploadSpeed":   strconv.FormatInt(int64(stats.LastWriteUploadRate), 10),
			"seeder":        strconv.FormatBool(peer.Seeder),
		})
	}
	return out
}
