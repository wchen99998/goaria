package goaria

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"time"
)

func dialSOCKS5(ctx context.Context, dialer *net.Dialer, proxyAddr, user, pass, network, target string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("unsupported socks network %q", network)
	}
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := socks5Handshake(conn, user, pass, target); err != nil {
		conn.Close()
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
	if _, err := conn.Write(append([]byte{0x05, byte(len(methods))}, methods...)); err != nil {
		return err
	}
	br := bufio.NewReader(conn)
	version, err := br.ReadByte()
	if err != nil {
		return err
	}
	method, err := br.ReadByte()
	if err != nil {
		return err
	}
	if version != 0x05 {
		return fmt.Errorf("invalid socks version %d", version)
	}
	switch method {
	case 0x00:
	case 0x02:
		if err := socks5UserPassAuth(conn, br, user, pass); err != nil {
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
	if _, err := conn.Write(req); err != nil {
		return err
	}
	header := make([]byte, 4)
	if _, err := ioReadFull(br, header); err != nil {
		return err
	}
	if header[0] != 0x05 || header[1] != 0x00 {
		return fmt.Errorf("socks connect failed with status %d", header[1])
	}
	var discard int
	switch header[3] {
	case 0x01:
		discard = 4
	case 0x03:
		n, err := br.ReadByte()
		if err != nil {
			return err
		}
		discard = int(n)
	case 0x04:
		discard = 16
	default:
		return fmt.Errorf("invalid socks address type %d", header[3])
	}
	buf := make([]byte, discard+2)
	_, err = ioReadFull(br, buf)
	return err
}

func socks5UserPassAuth(conn net.Conn, br *bufio.Reader, user, pass string) error {
	if len(user) > 255 || len(pass) > 255 {
		return fmt.Errorf("socks credentials too long")
	}
	req := []byte{0x01, byte(len(user))}
	req = append(req, []byte(user)...)
	req = append(req, byte(len(pass)))
	req = append(req, []byte(pass)...)
	if _, err := conn.Write(req); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := ioReadFull(br, resp); err != nil {
		return err
	}
	if resp[1] != 0x00 {
		return fmt.Errorf("socks authentication failed")
	}
	return nil
}

func ioReadFull(r *bufio.Reader, p []byte) (int, error) {
	for n := 0; n < len(p); {
		m, err := r.Read(p[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return len(p), nil
}
