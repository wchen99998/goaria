package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/wchen99998/goaria"
	"github.com/wchen99998/goaria/jsonrpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) > 1 && os.Args[1] == "daemon" {
		return runDaemon(os.Args[2:])
	}
	return runDownload(os.Args[1:])
}

type daemonOptions struct {
	addr           string
	dir            string
	inputFile      string
	saveSession    string
	secret         string
	logLevel       string
	maxRequestSize int64
}

func newDaemonFlagSet(opts *daemonOptions) *flag.FlagSet {
	fs := flag.NewFlagSet("goaria daemon", flag.ExitOnError)
	fs.StringVar(&opts.addr, "listen", jsonrpc.DefaultAddr, "RPC listen address")
	fs.StringVar(&opts.dir, "dir", ".", "download directory")
	fs.StringVar(&opts.inputFile, "input-file", "", "aria2-style session/input file to load at startup")
	fs.StringVar(&opts.saveSession, "save-session", "", "aria2-style session file to save automatically")
	fs.StringVar(&opts.secret, "rpc-secret", "", "aria2 RPC secret token")
	fs.Int64Var(&opts.maxRequestSize, "rpc-max-request-size", jsonrpc.DefaultMaxRequestSize, "maximum JSON-RPC request size in bytes")
	fs.StringVar(&opts.logLevel, "log-level", "info", "debug, info, warn, or error")
	return fs
}

func runDaemon(args []string) (retErr error) {
	var opts daemonOptions
	fs := newDaemonFlagSet(&opts)
	if err := fs.Parse(args); err != nil {
		return err
	}
	log, err := newLogger(opts.logLevel)
	if err != nil {
		return err
	}
	defer log.Sync()

	engine, err := goaria.NewEngine(goaria.Config{
		Dir:         opts.dir,
		InputFile:   opts.inputFile,
		SaveSession: opts.saveSession,
		Logger:      log,
	})
	if err != nil {
		return err
	}
	defer mergeCloseError(&retErr, engine.Close)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := jsonrpc.NewServer(engine, jsonrpc.Config{
		Addr:           opts.addr,
		MaxRequestSize: opts.maxRequestSize,
		Secret:         opts.secret,
		Logger:         log,
	})
	return server.ListenAndServe(ctx)
}

func runDownload(args []string) (retErr error) {
	fs := flag.NewFlagSet("goaria", flag.ExitOnError)
	dir := fs.String("dir", ".", "download directory")
	out := fs.String("out", "", "output filename for a single URL")
	split := fs.Int("split", 4, "number of pieces per download")
	connections := fs.Int("max-connection-per-server", 4, "maximum HTTP connections per server")
	segmentSize := fs.String("http-segment-size", "50M", "HTTP range segment size")
	httpVersion := fs.String("http-version", "auto", "HTTP version: auto, 1.1, 2, or 3")
	userAgent := fs.String("user-agent", "", "HTTP User-Agent")
	logLevel := fs.String("log-level", "info", "debug, info, warn, or error")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		printUsage()
		return nil
	}
	log, err := newLogger(*logLevel)
	if err != nil {
		return err
	}
	defer log.Sync()
	engine, err := goaria.NewEngine(goaria.Config{Dir: *dir, UserAgent: *userAgent, Logger: log})
	if err != nil {
		return err
	}
	defer mergeCloseError(&retErr, engine.Close)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gids := make([]string, 0, fs.NArg())
	for i, raw := range fs.Args() {
		opts := goaria.Options{
			"split":                     fmt.Sprint(*split),
			"max-connection-per-server": fmt.Sprint(*connections),
			"goaria-http-segment-size":  *segmentSize,
			"http-version":              *httpVersion,
		}
		if *out != "" && fs.NArg() == 1 && i == 0 {
			opts["out"] = *out
		}
		gid, err := engine.AddURI([]string{raw}, opts, nil)
		if err != nil {
			return err
		}
		gids = append(gids, gid)
	}
	return waitForDownloads(ctx, engine, gids)
}

func mergeCloseError(result *error, closeFn func(context.Context) error) {
	if err := closeFn(context.Background()); err != nil {
		*result = errors.Join(*result, err)
	}
}

func waitForDownloads(ctx context.Context, engine *goaria.Engine, gids []string) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			allDone := true
			var failures []string
			for _, gid := range gids {
				status, err := engine.TellStatus(gid, []string{"gid", "status", "completedLength", "totalLength", "errorMessage", "files"})
				if err != nil {
					return err
				}
				switch status["status"] {
				case string(goaria.StatusComplete):
				case string(goaria.StatusError), string(goaria.StatusRemoved):
					failures = append(failures, fmt.Sprintf("%s: %v", gid, status["errorMessage"]))
				default:
					allDone = false
				}
			}
			if len(failures) > 0 {
				return errors.New(strings.Join(failures, "; "))
			}
			if allDone {
				return nil
			}
		}
	}
}

func newLogger(level string) (*zap.Logger, error) {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		return nil, err
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zapLevel)
	return cfg.Build()
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `goaria %s

Usage:
  goaria [flags] URL [URL...]
  goaria daemon [flags]

`, goaria.Version)
}
