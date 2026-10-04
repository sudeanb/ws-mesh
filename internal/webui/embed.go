// Package webui embeds the dashboard's static assets.
package webui

import "embed"

//go:embed index.html app.js style.css
var Files embed.FS
