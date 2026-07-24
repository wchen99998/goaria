package goaria

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

func dialSOCKS5(ctx context.Context, dialer *net.Dialer, proxyAddr, user, pass, network, target string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("unsupported socks network %q", network)
	}
	started := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, err
	}
	deadline := time.Time{}
	if dialer.Timeout > 0 {
		deadline = started.Add(dialer.Timeout)
	}
	if !dialer.Deadline.IsZero() && (deadline.IsZero() || dialer.Deadline.Before(deadline)) {
		deadline = dialer.Deadline
	}
	if contextDeadline, ok := ctx.Deadline(); ok && (deadline.IsZero() || contextDeadline.Before(deadline)) {
		deadline = contextDeadline
	}
	if !deadline.IsZero() {
		_ = conn.SetDeadline(deadline)
	}
	cancelDone := make(chan struct{})
	stopCancel := context.AfterFunc(ctx, func() {
		_ = conn.Close()
		close(cancelDone)
	})
	if err := socks5Handshake(conn, user, pass, target); err != nil {
		_ = conn.Close()
		if !stopCancel() {
			<-cancelDone
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	if !stopCancel() {
		<-cancelDone
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func socks5Handshake(conn net.Conn, user, pass, target string) error {
	methods := []byte{0x00}
	if user != "" {
		methods = append(methods, 0x02)
	}
	if err := writeAll(conn, append([]byte{0x05, byte(len(methods))}, methods...)); err != nil {
		return err
	}
	methodSelection := make([]byte, 2)
	if _, err := io.ReadFull(conn, methodSelection); err != nil {
		return err
	}
	if methodSelection[0] != 0x05 {
		return fmt.Errorf("invalid socks version %d", methodSelection[0])
	}
	switch methodSelection[1] {
	case 0x00:
	case 0x02:
		if user == "" {
			return fmt.Errorf("socks proxy selected unoffered username/password authentication")
		}
		if err := socks5UserPassAuth(conn, user, pass); err != nil {
			return err
		}
	default:
		return fmt.Errorf("socks authentication method rejected")
	}

	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return err
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid socks target port %q", portText)
	}
	req := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil && ip.To4() != nil {
		req = append(req, 0x01)
		req = append(req, ip.To4()...)
	} else if ip := net.ParseIP(host); ip != nil {
		req = append(req, 0x04)
		req = append(req, ip.To16()...)
	} else {
		if len(host) > 255 {
			return fmt.Errorf("socks target host too long")
		}
		req = append(req, 0x03, byte(len(host)))
		req = append(req, []byte(host)...)
	}
	var portBuf [2]byte
	binary.BigEndian.PutUint16(portBuf[:], uint16(port))
	req = append(req, portBuf[:]...)
	if err := writeAll(conn, req); err != nil {
		return err
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	if header[0] != 0x05 || header[1] != 0x00 {
		return fmt.Errorf("socks connect failed with status %d", header[1])
	}
	if header[2] != 0x00 {
		return fmt.Errorf("invalid socks reserved byte %d", header[2])
	}
	var discard int
	switch header[3] {
	case 0x01:
		discard = 4
	case 0x03:
		var length [1]byte
		if _, err := io.ReadFull(conn, length[:]); err != nil {
			return err
		}
		discard = int(length[0])
	case 0x04:
		discard = 16
	default:
		return fmt.Errorf("invalid socks address type %d", header[3])
	}
	buf := make([]byte, discard+2)
	_, err = io.ReadFull(conn, buf)
	return err
}

func socks5UserPassAuth(conn net.Conn, user, pass string) error {
	if len(user) > 255 || len(pass) > 255 {
		return fmt.Errorf("socks credentials too long")
	}
	req := []byte{0x01, byte(len(user))}
	req = append(req, []byte(user)...)
	req = append(req, byte(len(pass)))
	req = append(req, []byte(pass)...)
	if err := writeAll(conn, req); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != 0x01 || resp[1] != 0x00 {
		return fmt.Errorf("socks authentication failed")
	}
	return nil
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}
