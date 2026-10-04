// Package ws implements RFC 6455 WebSocket framing from scratch: the
// opening handshake (server side), frame encode/decode, client-to-server
// masking enforcement, fragmentation, and close/status handling.
package ws

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

const (
	OpContinuation = 0x0
	OpText         = 0x1
	OpBinary       = 0x2
	OpClose        = 0x8
	OpPing         = 0x9
	OpPong         = 0xA
)

var ErrMaskRequired = errors.New("ws: client frames must be masked")

// Handshake performs the RFC 6455 opening handshake on an upgraded
// connection. It returns after writing the 101 response.
func Handshake(c net.Conn, r *bufio.Reader) error {
	req := map[string]string{}
	var line string
	for {
		l, err := r.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(l, "\r\n")
		if line == "" {
			break
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			req[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
	if !strings.Contains(strings.ToLower(req["upgrade"]), "websocket") ||
		!strings.Contains(strings.ToLower(req["connection"]), "upgrade") {
		return errors.New("ws: not an upgrade request")
	}
	key := req["sec-websocket-key"]
	if key == "" {
		return errors.New("ws: missing Sec-WebSocket-Key")
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	_, err := c.Write([]byte(resp))
	return err
}

// ComputeAccept returns the Sec-WebSocket-Accept value for a key.
func ComputeAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// WriteAccept writes the 101 response for an already-parsed handshake.
func WriteAccept(c net.Conn, key string) error {
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + ComputeAccept(key) + "\r\n\r\n"
	_, err := c.Write([]byte(resp))
	return err
}

// Message is one complete (defragmented) message.
type Message struct {
	Opcode int
	Data   []byte
}

// ReadServerMessage is ReadMessage for the CLIENT side: server frames
// arrive unmasked and the client must not treat that as an error.
func ReadServerMessage(r *bufio.Reader, maxPayload int64, writeRaw func(frame []byte) error) (Message, error) {
	msg, err := readServerMessage(r, maxPayload, writeRaw)
	return msg, err
}

func readServerMessage(r *bufio.Reader, maxPayload int64, writeRaw func(frame []byte) error) (Message, error) {
	var msg Message
	started := false
	for {
		fin, opcode, _, payload, err := readFrame(r, maxPayload)
		if err != nil {
			return msg, err
		}
		switch opcode {
		case OpPing:
			if f, e := encodeFrame(true, OpPong, payload); e == nil && writeRaw != nil {
				_ = writeRaw(f)
			}
			continue
		case OpPong:
			continue
		case OpClose:
			if writeRaw != nil {
				if f, e := encodeFrame(true, OpClose, payload); e == nil {
					_ = writeRaw(f)
				}
			}
			return msg, io.EOF
		}
		switch opcode {
		case OpText, OpBinary:
			if started {
				return msg, fmt.Errorf("ws: new data frame mid-message")
			}
			started = true
			msg.Opcode = opcode
			msg.Data = payload
		case OpContinuation:
			if !started {
				return msg, fmt.Errorf("ws: continuation without start")
			}
			msg.Data = append(msg.Data, payload...)
		default:
			return msg, fmt.Errorf("ws: reserved opcode %d", opcode)
		}
		if fin {
			return msg, nil
		}
	}
}

// ReadMessage reads one message, defragmenting continuation frames and
// answering ping/pong transparently. writeRaw lets the caller inject
// control replies on the same connection.
func ReadMessage(r *bufio.Reader, maxPayload int64, writeRaw func(frame []byte) error) (Message, error) {
	var msg Message
	started := false
	for {
		fin, opcode, masked, payload, err := readFrame(r, maxPayload)
		if err != nil {
			return msg, err
		}
		if !masked {
			return msg, ErrMaskRequired
		}
		switch opcode {
		case OpPing:
			if f, err := encodeFrame(true, OpPong, payload); err == nil && writeRaw != nil {
				_ = writeRaw(f)
			}
			continue
		case OpPong:
			continue
		case OpClose:
			if writeRaw != nil {
				if f, err := encodeFrame(true, OpClose, payload); err == nil {
					_ = writeRaw(f)
				}
			}
			return msg, io.EOF
		}

		switch opcode {
		case OpText, OpBinary:
			if started {
				return msg, fmt.Errorf("ws: new data frame mid-message")
			}
			started = true
			msg.Opcode = opcode
			msg.Data = payload
		case OpContinuation:
			if !started {
				return msg, fmt.Errorf("ws: continuation without start")
			}
			msg.Data = append(msg.Data, payload...)
		default:
			return msg, fmt.Errorf("ws: reserved opcode %d", opcode)
		}
		if fin {
			if !started {
				return msg, fmt.Errorf("ws: fin without data frame")
			}
			return msg, nil
		}
	}
}

// readFrame reads one frame, unmasking in place.
func readFrame(r *bufio.Reader, maxPayload int64) (fin bool, opcode int, masked bool, payload []byte, err error) {
	var h [2]byte
	if _, err = io.ReadFull(r, h[:]); err != nil {
		return
	}
	fin = h[0]&0x80 != 0
	opcode = int(h[0] & 0x0f)
	masked = h[1]&0x80 != 0
	length := uint64(h[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(r, ext[:]); err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(r, ext[:]); err != nil {
			return
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if int64(length) > maxPayload {
		err = fmt.Errorf("ws: frame too large (%d)", length)
		return
	}
	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(r, mask[:]); err != nil {
			return
		}
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(r, payload); err != nil {
		return
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return
}

// WriteMessage frames and writes one complete message (server frames
// are never masked).
func WriteMessage(c net.Conn, opcode int, data []byte) error {
	f, err := encodeFrame(true, opcode, data)
	if err != nil {
		return err
	}
	_, err = c.Write(f)
	return err
}

// WriteClientMessage frames and writes one message with the client-side
// mandatory masking (RFC 6455 §5.3).
func WriteClientMessage(c net.Conn, opcode int, data []byte) error {
	out := []byte{0, 0}
	out[0] |= 0x80
	out[0] |= byte(opcode)
	n := len(data)
	switch {
	case n < 126:
		out[1] = byte(n) | 0x80
	case n < 65536:
		out[1] = 126 | 0x80
		out = append(out, byte(n>>8), byte(n))
	default:
		out[1] = 127 | 0x80
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		out = append(out, ext[:]...)
	}
	var mask [4]byte
	rand.Read(mask[:])
	out = append(out, mask[:]...)
	for i := range data {
		data[i] ^= mask[i%4]
	}
	out = append(out, data...)
	_, err := c.Write(out)
	return err
}

func encodeFrame(fin bool, opcode int, data []byte) ([]byte, error) {
	out := []byte{0, 0}
	if fin {
		out[0] |= 0x80
	}
	out[0] |= byte(opcode)
	n := len(data)
	switch {
	case n < 126:
		out[1] = byte(n)
	case n < 65536:
		out[1] = 126
		out = append(out, byte(n>>8), byte(n))
	default:
		out[1] = 127
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		out = append(out, ext[:]...)
	}
	return append(out, data...), nil
}
