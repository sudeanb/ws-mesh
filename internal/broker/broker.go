// Package broker implements rooms, persistent subscriptions and the
// mesh flood: clients speak tiny JSON control messages over one
// WebSocket; nodes relay publications to their peers exactly once
// (deduplicated by message id).
package broker

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"wsmesh/internal/ws"
)

// Envelope is the only wire format clients and peers speak.
type Envelope struct {
	Op     string          `json:"op"`               // sub | unsub | pub | msg | error
	Room   string          `json:"room,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"`
	From   string          `json:"from,omitempty"`
	ID     string          `json:"id,omitempty"` // publication id for mesh dedup
	Hops   int             `json:"hops,omitempty"`
}

type subscriber struct {
	conn net.Conn
	id   string
	rooms map[string]bool
}

type Hub struct {
	mu    sync.Mutex
	rooms map[string]map[*subscriber]bool

	muPeers sync.Mutex
	peers   map[*peerConn]bool

	name string
}

type peerConn struct {
	conn   net.Conn
	name   string
	dialed bool // true when we are the WebSocket client of this link
}

func New(name string) *Hub {
	return &Hub{rooms: map[string]map[*subscriber]bool{}, peers: map[*peerConn]bool{}, name: name}
}

// ServeClient upgrades and serves one dashboard/client connection.
func (h *Hub) ServeClient(c net.Conn) error {
	br := bufio.NewReader(c)
	if err := ws.Handshake(c, br); err != nil {
		return err
	}
	return h.serveAfterHandshake(c, br)
}

// ServeClientWithReader is the listener-side variant that already holds
// a buffered reader over the connection.
func (h *Hub) ServeClientWithReader(c net.Conn, br *bufio.Reader) error {
	if err := ws.Handshake(c, br); err != nil {
		return err
	}
	return h.serveAfterHandshake(c, br)
}

// ServeClientHijacked serves a connection net/http already parsed: the
// 101 is written from the header map, then the frame loop takes over.
func (h *Hub) ServeClientHijacked(c net.Conn, key string) {
	if err := ws.WriteAccept(c, key); err != nil {
		c.Close()
		return
	}
	_ = h.serveAfterHandshake(c, bufio.NewReader(c))
}

func (h *Hub) serveAfterHandshake(c net.Conn, br *bufio.Reader) error {
	sub := &subscriber{conn: c, id: randID(), rooms: map[string]bool{}}
	defer func() {
		h.mu.Lock()
		for room := range sub.rooms {
			delete(h.rooms[room], sub)
		}
		h.mu.Unlock()
		c.Close()
	}()

	for {
		msg, err := ws.ReadMessage(br, 1<<20, func(frame []byte) error {
			_, err := c.Write(frame)
			return err
		})
		if err != nil {
			return err
		}
		var env Envelope
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			h.send(sub, Envelope{Op: "error", Body: json.RawMessage(`"bad json"`)})
			continue
		}
		switch env.Op {
		case "sub":
			h.mu.Lock()
			if h.rooms[env.Room] == nil {
				h.rooms[env.Room] = map[*subscriber]bool{}
			}
			h.rooms[env.Room][sub] = true
			sub.rooms[env.Room] = true
			h.mu.Unlock()
			h.send(sub, Envelope{Op: "msg", Room: env.Room, From: "system",
				Body: json.RawMessage(`"subscribed"`), ID: randID()})
		case "unsub":
			h.mu.Lock()
			delete(h.rooms[env.Room], sub)
			delete(sub.rooms, env.Room)
			h.mu.Unlock()
		case "pub":
			env.From = sub.id
			env.ID = randID()
			env.Op = "msg"
			h.broadcast(env, nil)
			h.floodPeers(env)
		}
	}
}

// broadcast delivers to every subscriber of env.Room except except.
func (h *Hub) broadcast(env Envelope, except *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.rooms[env.Room] {
		if sub == except {
			continue
		}
		h.send(sub, env)
	}
}

func (h *Hub) send(sub *subscriber, env Envelope) {
	data, err := json.Marshal(env)
	if err != nil {
		return
	}
	_ = sub.conn.SetWriteDeadline(deadline())
	if err := ws.WriteMessage(sub.conn, ws.OpText, data); err != nil {
		h.mu.Lock()
		for room := range sub.rooms {
			delete(h.rooms[room], sub)
		}
		h.mu.Unlock()
	}
}

func deadline() time.Time { return time.Now().Add(5 * time.Second) }

// AddPeer dials a peer node and relays publications both ways. On this
// link we are the WebSocket CLIENT: masked writes, unmasked reads.
func (h *Hub) AddPeer(addr string) {
	go func() {
		for {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}
			if _, err := c.Write([]byte("GET /mesh HTTP/1.1\r\nHost: mesh\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + clientKey() + "\r\nSec-WebSocket-Version: 13\r\n\r\n")); err != nil {
				c.Close()
				continue
			}
			br := bufio.NewReader(c)
			// consume the 101 response headers
			for {
				line, err := br.ReadString('\n')
				if err != nil || strings.TrimRight(line, "\r\n") == "" {
					break
				}
			}
			p := &peerConn{conn: c, name: addr, dialed: true}
			h.muPeers.Lock()
			h.peers[p] = true
			h.muPeers.Unlock()
			log.Printf("mesh: peer %s up", addr)

			for {
				msg, err := ws.ReadServerMessage(br, 1<<20, func(frame []byte) error {
					_, err := c.Write(frame)
					return err
				})
				if err != nil {
					break
				}
				h.relayPeerMessage(msg)
			}
			h.muPeers.Lock()
			delete(h.peers, p)
			h.muPeers.Unlock()
			c.Close()
			log.Printf("mesh: peer %s down, redialing", addr)
		}
	}()
}

// relayPeerMessage delivers a mesh publication locally and re-floods it.
func (h *Hub) relayPeerMessage(msg ws.Message) {
	var env Envelope
	if err := json.Unmarshal(msg.Data, &env); err != nil || env.Op != "msg" {
		return
	}
	if h.seen(env.ID) {
		return
	}
	h.markSeen(env.ID)
	env.Hops++
	if env.Hops > 8 {
		return
	}
	h.broadcast(env, nil)
	h.floodPeers(env)
}

// floodPeers publishes env to all peers. Each link's masking follows
// its role: dialed links are WebSocket clients (masked writes), inbound
// links are servers (unmasked).
func (h *Hub) floodPeers(env Envelope) {
	data, err := json.Marshal(env)
	if err != nil {
		return
	}
	h.muPeers.Lock()
	defer h.muPeers.Unlock()
	for p := range h.peers {
		_ = p.conn.SetWriteDeadline(deadline())
		var werr error
		if p.dialed {
			werr = ws.WriteClientMessage(p.conn, ws.OpText, data)
		} else {
			werr = ws.WriteMessage(p.conn, ws.OpText, data)
		}
		if werr != nil {
			p.conn.Close()
		}
	}
}

// ServePeer upgrades an inbound node-to-node connection ("/mesh" path):
// publications received here are delivered locally and re-flooded with
// hop+1. We are the WebSocket server on this link.
func (h *Hub) ServePeer(c net.Conn, br *bufio.Reader) {
	if err := ws.Handshake(c, br); err != nil {
		c.Close()
		return
	}
	p := &peerConn{conn: c, name: c.RemoteAddr().String()}
	h.muPeers.Lock()
	h.peers[p] = true
	h.muPeers.Unlock()
	defer func() {
		h.muPeers.Lock()
		delete(h.peers, p)
		h.muPeers.Unlock()
		c.Close()
	}()
	for {
		msg, err := ws.ReadMessage(br, 1<<20, func(frame []byte) error {
			_, err := c.Write(frame)
			return err
		})
		if err != nil {
			return
		}
		h.relayPeerMessage(msg)
	}
}

var seenIDs = struct {
	sync.Mutex
	m map[string]bool
}{m: map[string]bool{}}

func (h *Hub) seen(id string) bool {
	seenIDs.Lock()
	defer seenIDs.Unlock()
	return seenIDs.m[id]
}

func (h *Hub) markSeen(id string) {
	seenIDs.Lock()
	defer seenIDs.Unlock()
	seenIDs.m[id] = true
}

func randID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func clientKey() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
