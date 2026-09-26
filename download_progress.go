package goaria

import "path/filepath"

func (d *Download) setURIUsed(raw string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.uris {
		if d.uris[i].URI == raw {
			d.uris[i].Status = URIStatusUsed
		} else {
			d.uris[i].Status = URIStatusWaiting
		}
	}
}

func (d *Download) setMetadata(meta remoteMeta, path string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	total := nonNegativeLength(meta.Length)
	d.path = path
	d.dir = filepath.Dir(path)
	d.out = filepath.Base(path)
	d.currentURI = meta.FinalURI
	d.totalLength = total
	if total > 0 && d.completedLen > total {
		d.completedLen = 0
	}
	d.pieceLength = 0
	d.numPieces = 0
	d.donePieces = 0
	d.bitfield = ""
}

func (d *Download) outputPath() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.path
}

func (d *Download) resetProgress(total int64, chunks []chunkRange) {
	d.mu.Lock()
	defer d.mu.Unlock()
	total = nonNegativeLength(total)
	d.totalLength = total
	d.completedLen = 0
	if len(chunks) > 0 {
		d.pieceLength = chunks[0].end - chunks[0].start + 1
		d.numPieces = int64(len(chunks))
	} else {
		d.pieceLength = total
		d.numPieces = 1
	}
	d.donePieces = completedPieces(d.totalLength, d.completedLen, d.pieceLength)
	d.bitfield = bitfieldFor(d.totalLength, d.completedLen, d.pieceLength)
}

func (d *Download) resetSingleProgress(total, completed int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	total = nonNegativeLength(total)
	completed = nonNegativeLength(completed)
	d.totalLength = total
	d.completedLen = completed
	if total > 0 {
		d.pieceLength = total
		d.numPieces = 1
		d.donePieces = completedPieces(total, completed, total)
		d.bitfield = bitfieldFor(total, completed, total)
	}
}

func (d *Download) addCompleted(n int64) {
	if n <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.completedLen += n
	if d.totalLength > 0 && d.completedLen > d.totalLength {
		d.completedLen = d.totalLength
	}
	// Segmented downloads mark bits only after their corresponding range has
	// completed. Aggregate byte counts cannot identify which out-of-order
	// range those bytes belong to.
	if d.pieceLength > 0 && d.numPieces <= 1 {
		done := completedPieces(d.totalLength, d.completedLen, d.pieceLength)
		if done != d.donePieces {
			d.donePieces = done
			d.bitfield = bitfieldFor(d.totalLength, d.completedLen, d.pieceLength)
		}
	}
}

func (d *Download) rollbackCompleted(n int64) {
	if n <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.completedLen -= n
	if d.completedLen < 0 {
		d.completedLen = 0
	}
}

func (d *Download) markPieceComplete(index int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if index < 0 || int64(index) >= d.numPieces || d.bitfield == "" {
		return
	}
	hexIndex := index / 4
	if hexIndex >= len(d.bitfield) {
		return
	}
	nibble, ok := decodeHexNibble(d.bitfield[hexIndex])
	if !ok {
		return
	}
	mask := byte(1 << uint(3-index%4))
	if nibble&mask != 0 {
		return
	}
	nibble |= mask
	bits := []byte(d.bitfield)
	bits[hexIndex] = "0123456789abcdef"[nibble]
	d.bitfield = string(bits)
	d.donePieces++
}

func decodeHexNibble(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, true
	default:
		return 0, false
	}
}

func (d *Download) setConnections(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.connections = n
}

func (d *Download) setDownloadBPS(n int64) {
	d.setTransferBPS(n, 0)
}

func (d *Download) setTransferBPS(download, upload int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.status != StatusActive {
		return
	}
	d.downloadBPS = download
	d.uploadBPS = upload
}
