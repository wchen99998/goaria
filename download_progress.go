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
	if d.pieceLength > 0 {
		done := completedPieces(d.totalLength, d.completedLen, d.pieceLength)
		if done != d.donePieces {
			d.donePieces = done
			d.bitfield = bitfieldFor(d.totalLength, d.completedLen, d.pieceLength)
		}
	}
}

func (d *Download) setConnections(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.connections = n
}

func (d *Download) setDownloadBPS(n int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.downloadBPS = n
}
