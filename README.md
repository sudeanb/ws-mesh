# WS Mesh — a WebSocket broker that links into a mesh

RFC 6455 implemented from the bytes up (handshake, frame codec, masking
rules, fragmentation, ping/pong, close), then a broker on top: rooms,
persistent subscriptions, JSON envelopes — and node-to-node links that
flood publications across a mesh exactly once per hop.

```
node B:  wsmesh -addr :8081
node A:  wsmesh -addr :8080 -peers localhost:8081
# subscribe on A, publish on B — it crosses the link with hops=1
```

The bundled dashboard (`/` on any node) subscribes, publishes and shows
every message with its sender and hop count.

## Layers

| package | contents |
|---|---|
| `internal/ws` | RFC 6455: Sec-WebSocket-Accept handshake, 7/16/64-bit length framing, mandatory client masking (server writes unmasked), defragmentation, ping→pong, close handshake. **Both roles are first-class**: servers read masked client frames, clients read unmasked server frames (`ReadServerMessage`). |
| `internal/broker` | rooms with `sub`/`unsub`/`pub` JSON envelopes; publications get a random id; mesh links relay with hop+1 and a seen-set cuts loops at 8 hops |
| mesh links | dialer speaks the WebSocket *client* role (masked writes, unmasked reads); inbound `/mesh` connections speak the *server* role — each link's framing follows its role exactly |

## What the tests prove

- frame round-trips at every length boundary (125/126/65535/65536+)
- unmasked client frames are rejected (`ErrMaskRequired`)
- fragmented messages defragment; pings are answered with pongs
- the RFC 6455 §1.3 worked example produces the exact accept key
- two clients in a room, one publishes: the other receives with a
  sender id; rooms are isolated
- **two nodes, one link**: a publish on B arrives on A with hops=1 and
  does not loop forever

## Limits

No TLS (put it behind a terminator), no per-room persistence across
restarts, flooding only (no spanning-tree suppression), seen-set is
process-local. None of that is hidden — the code reads the way it runs.

MIT licensed. Part of a 100-project systems-programming campaign.
