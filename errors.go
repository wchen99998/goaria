package goaria

import "errors"

var (
	ErrNotFound            = errors.New("download not found")
	ErrInvalidGID          = errors.New("invalid gid")
	ErrInvalidParams       = errors.New("invalid params")
	ErrUnsupportedProtocol = errors.New("only http and https downloads are supported")
	ErrUnsupportedMethod   = errors.New("method is unsupported for http/https-only mode")
	ErrShutdown            = errors.New("engine is shutting down")
)
