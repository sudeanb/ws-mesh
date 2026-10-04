// Command wsmesh serves a WebSocket broker with rooms and persistent
// subscriptions, and links identical nodes into a flooding mesh.
//
//	wsmesh -addr :8080 [-peers node1:8080,node2:8080] [-name a]
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"wsmesh/internal/broker"
	"wsmesh/internal/webui"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	peers := flag.String("peers", "", "comma-separated peer addresses to link")
	name := flag.String("name", "", "this node's name")
	flag.Parse()

	if *name == "" {
		*name = *addr
	}

	hub := broker.New(*name)
	for _, p := range strings.Split(*peers, ",") {
		if p = strings.TrimSpace(p); p != "" {
			hub.AddPeer(p)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			hj, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "no hijack", http.StatusInternalServerError)
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				return
			}
			go hub.ServeClientHijacked(conn, r.Header.Get("Sec-WebSocket-Key"))
			return
		}
		switch r.URL.Path {
		case "/":
			data, _ := webui.Files.ReadFile("index.html")
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write(data)
		case "/app.js":
			data, _ := webui.Files.ReadFile("app.js")
			w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
			w.Write(data)
		case "/style.css":
			data, _ := webui.Files.ReadFile("style.css")
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
			w.Write(data)
		default:
			http.NotFound(w, r)
		}
	})

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("wsmesh %s on %s (peers: %q)", *name, *addr, *peers)
	log.Fatal(srv.ListenAndServe())
}
