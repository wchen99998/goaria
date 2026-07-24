package main

import (
	"context"
	"errors"
	"testing"

	"github.com/wchen99998/goaria/jsonrpc"
)

func TestDaemonFlagsConfigureMaxRequestSize(t *testing.T) {
	var opts daemonOptions
	fs := newDaemonFlagSet(&opts)
	if err := fs.Parse([]string{
		"--listen", "localhost:1234",
		"--rpc-max-request-size", "8388608",
	}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if opts.addr != "localhost:1234" {
		t.Fatalf("addr = %q, want localhost:1234", opts.addr)
	}
	if opts.maxRequestSize != 8<<20 {
		t.Fatalf("maxRequestSize = %d, want %d", opts.maxRequestSize, 8<<20)
	}
}

func TestDaemonFlagsDefaultMaxRequestSize(t *testing.T) {
	var opts daemonOptions
	fs := newDaemonFlagSet(&opts)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if opts.maxRequestSize != jsonrpc.DefaultMaxRequestSize {
		t.Fatalf("maxRequestSize = %d, want %d", opts.maxRequestSize, jsonrpc.DefaultMaxRequestSize)
	}
}

func TestMergeCloseError(t *testing.T) {
	primaryErr := errors.New("primary failure")
	closeErr := errors.New("close failure")
	result := primaryErr
	calls := 0

	mergeCloseError(&result, func(ctx context.Context) error {
		calls++
		if ctx == nil || ctx.Err() != nil {
			t.Fatalf("close context must be live")
		}
		return closeErr
	})

	if calls != 1 {
		t.Fatalf("close called %d times, want 1", calls)
	}
	if !errors.Is(result, primaryErr) {
		t.Fatalf("result %v does not preserve primary error", result)
	}
	if !errors.Is(result, closeErr) {
		t.Fatalf("result %v does not include close error", result)
	}
}

func TestMergeCloseErrorReturnsCloseFailure(t *testing.T) {
	closeErr := errors.New("close failure")
	var result error
	mergeCloseError(&result, func(context.Context) error { return closeErr })
	if !errors.Is(result, closeErr) {
		t.Fatalf("result = %v, want close error", result)
	}
}

func TestMergeCloseErrorPreservesPrimaryOnSuccessfulClose(t *testing.T) {
	primaryErr := errors.New("primary failure")
	result := primaryErr
	mergeCloseError(&result, func(context.Context) error { return nil })
	if result != primaryErr {
		t.Fatalf("result = %v, want original primary error", result)
	}
}
