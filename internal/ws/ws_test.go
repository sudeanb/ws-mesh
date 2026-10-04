package ws

import (
	"bufio"
	"bytes"
	"net"
	"strings"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	sizes := []int{0, 5, 125, 126, 1000, 65535, 65536, 70000}
	for _, n := range sizes {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i)
		}
		f, err := encodeFrame(true, OpBinary, data)
		if err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(bytes.NewReader(f))
		gotFin, gotOp, masked, payload, err := readFrame(r, 1<<20)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if !gotFin || gotOp != OpBinary || masked || !bytes.Equal(payload, data) {
			t.Fatalf("n=%d round-trip broken", n)
		}
	}
}

func TestUnmaskedClientFrameRejected(t *testing.T) {
	// a server MUST reject unmasked client frames: fin+text, len 5, no mask
	f := []byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'}
	r := bufio.NewReader(bytes.NewReader(f))
	msg, err := ReadMessage(r, 1<<20, nil)
	if err != ErrMaskRequired {
		t.Fatalf("want ErrMaskRequired, got %v (msg=%+v)", err, msg)
	}
}

func TestFragmentedMessageDefragments(t *testing.T) {
	// frame 1: fin=0, text "hel"; frame 2: continuation, fin=1 "lo"
	f1, _ := encodeFrame(false, OpText, []byte("hel"))
	f2, _ := encodeFrame(true, OpContinuation, []byte("lo"))
	// client side: mask them (server requires masking) — reuse unmasked
	// path by testing ReadMessage's defrag with a raw pair; masking is
	// applied by wrapping each frame with a mask manually
	masked := func(frame []byte, payloadOff int) []byte {
		// set mask bit, append 4-byte mask, xor payload in place
		frame[1] |= 0x80
		mask := []byte{1, 2, 3, 4}
		for i := payloadOff; i < len(frame); i++ {
			frame[i] ^= mask[(i-payloadOff)%4]
		}
		return append(append([]byte{}, frame[:payloadOff]...), append(mask, frame[payloadOff:]...)...)
	}
	// payload starts after the 2-byte header (small lengths)
	m1 := masked(f1, 2)
	m2 := masked(f2, 2)

	r := bufio.NewReader(bytes.NewReader(append(m1, m2...)))
	var pongFrames [][]byte
	msg, err := ReadMessage(r, 1<<20, func(frame []byte) error {
		pongFrames = append(pongFrames, frame)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Data) != "hello" || msg.Opcode != OpText {
		t.Fatalf("defrag: %q", msg.Data)
	}
	if len(pongFrames) != 0 {
		t.Fatal("no pings were sent")
	}
}

func TestPingAnsweredWithPong(t *testing.T) {
	ping, _ := encodeFrame(true, OpPing, []byte("hb"))
	// mask it
	ping[1] |= 0x80
	mask := []byte{9, 9, 9, 9}
	for i := 2; i < len(ping); i++ {
		ping[i] ^= mask[(i-2)%4]
	}
	frame := append([]byte{ping[0], ping[1]}, append(mask, ping[2:]...)...)

	r := bufio.NewReader(bytes.NewReader(frame))
	var out [][]byte
	// the connection ends after the ping: EOF is the expected terminator
	_, _ = ReadMessage(r, 1<<20, func(f []byte) error {
		out = append(out, f)
		return nil
	})
	if len(out) != 1 {
		t.Fatalf("want 1 pong frame, got %d", len(out))
	}
	if out[0][0] != 0x8a { // fin + pong
		t.Fatalf("not a pong: %x", out[0][0])
	}
	if string(out[0][2:]) != "hb" {
		t.Fatalf("pong payload: %q", out[0][2:])
	}
}

func TestHandshakeAcceptKey(t *testing.T) {
	// RFC 6455 §1.3 worked example
	if got := ComputeAccept("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("accept: %q", got)
	}
}

func TestEndToEndHandshake(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		if err := Handshake(c, bufio.NewReader(c)); err != nil {
			c.Close()
			return
		}
		WriteMessage(c, OpText, []byte("welcome"))
		c.Close()
	}()

	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"))
	r := bufio.NewReader(c)
	// client: read the 101 and its headers
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimRight(line, "\r\n") == "" {
			break
		}
		if strings.HasPrefix(line, "Sec-WebSocket-Accept:") {
			if !strings.Contains(line, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") {
				t.Fatalf("wrong accept: %s", line)
			}
		}
	}
	fin, op, _, payload, err := readFrame(r, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !fin || op != OpText || string(payload) != "welcome" {
		t.Fatalf("first message: fin=%v op=%d %q", fin, op, payload)
	}
}
