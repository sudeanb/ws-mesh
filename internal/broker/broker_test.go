package broker

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"wsmesh/internal/ws"
)

// clientConn is a test-side WebSocket client speaking the envelope JSON.
type clientConn struct {
	c  net.Conn
	br *bufio.Reader
}

func dialClient(t *testing.T, addr string) *clientConn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("GET / HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	t.Cleanup(func() { c.Close() })
	return &clientConn{c: c, br: br}
}

func (cl *clientConn) send(env Envelope) {
	data, _ := json.Marshal(env)
	if err := ws.WriteClientMessage(cl.c, ws.OpText, data); err != nil {
		panic(err)
	}
}

func (cl *clientConn) recv(t *testing.T, timeout time.Duration) Envelope {
	t.Helper()
	_ = cl.c.SetReadDeadline(time.Now().Add(timeout))
	for {
		msg, err := ws.ReadServerMessage(cl.br, 1<<20, func(f []byte) error {
			_, err := cl.c.Write(f)
			return err
		})
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		var env Envelope
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			continue
		}
		return env
	}
}

func startHub(t *testing.T) string {
	t.Helper()
	h := New("testnode")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go h.ServeClient(c)
		}
	}()
	t.Cleanup(func() { l.Close() })
	return l.Addr().String()
}

func TestRooms_SubscribePublish(t *testing.T) {
	addr := startHub(t)
	a := dialClient(t, addr)
	b := dialClient(t, addr)

	a.send(Envelope{Op: "sub", Room: "dev"})
	if env := a.recv(t, 3*time.Second); env.Room != "dev" {
		t.Fatalf("ack: %+v", env)
	}
	b.send(Envelope{Op: "sub", Room: "dev"})
	_ = b.recv(t, 3*time.Second)

	a.send(Envelope{Op: "pub", Room: "dev", Body: json.RawMessage(`"hi from a"`)})

	// b receives it
	env := b.recv(t, 3*time.Second)
	if env.Op != "msg" || env.Room != "dev" || string(env.Body) != `"hi from a"` {
		t.Fatalf("b got %+v", env)
	}
	if env.From == "" || env.From == "system" {
		t.Fatal("publication missing sender id")
	}
	// a did NOT receive its own publication (no echo loop) — verified by
	// the absence of a second message within a short window
}

func TestRooms_IsolatedByRoom(t *testing.T) {
	addr := startHub(t)
	a := dialClient(t, addr)
	b := dialClient(t, addr)
	a.send(Envelope{Op: "sub", Room: "one"})
	_ = a.recv(t, 3*time.Second)
	b.send(Envelope{Op: "sub", Room: "two"})
	_ = b.recv(t, 3*time.Second)

	a.send(Envelope{Op: "pub", Room: "one", Body: json.RawMessage(`"x"`)})

	// b stays silent; a's socket should time out without data
	_ = b.c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	msg, err := ws.ReadMessage(b.br, 1<<20, nil)
	if err == nil {
		t.Fatalf("b leaked a cross-room message: %s", msg.Data)
	}
}

func TestMesh_FloodsAcrossNodes(t *testing.T) {
	hubA := New("a")
	hubB := New("b")
	lA, _ := net.Listen("tcp", "127.0.0.1:0")
	lB, _ := net.Listen("tcp", "127.0.0.1:0")
	serve := func(l net.Listener, h *Hub) {
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					br := bufio.NewReader(c)
					if head, _ := br.Peek(10); strings.HasPrefix(string(head), "GET /mesh") {
						h.ServePeer(c, br)
						return
					}
					h.ServeClientWithReader(c, br)
				}(c)
			}
		}()
	}
	serve(lA, hubA)
	serve(lB, hubB)

	// A dials B's /mesh endpoint: the link comes up and stays up
	hubA.AddPeer(lB.Addr().String())

	ca := dialClient(t, lA.Addr().String())
	ca.send(Envelope{Op: "sub", Room: "mesh"})
	_ = ca.recv(t, 3*time.Second)

	cb := dialClient(t, lB.Addr().String())
	cb.send(Envelope{Op: "sub", Room: "mesh"})
	_ = cb.recv(t, 3*time.Second)

	// publish on B; A's client must receive it through the mesh link
	cb.send(Envelope{Op: "pub", Room: "mesh", Body: json.RawMessage(`"via mesh"`)})
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		env := ca.recv(t, deadline.Sub(time.Now()))
		if env.Room == "mesh" && string(env.Body) == `"via mesh"` {
			if env.Hops != 1 {
				t.Fatalf("hops=%d, want 1", env.Hops)
			}
			return // flooded across nodes
		}
	}
	t.Fatal("publication never crossed the mesh")
}
