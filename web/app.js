"use strict";
const log = document.querySelector("#log");
let ws, myId = "";

function connect() {
  ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onopen = () => subscribe(document.querySelector("#room").value);
  ws.onmessage = (e) => {
    const env = JSON.parse(e.data);
    if (env.from === myId && env.op !== "msg") return;
    if (env.from === "system" && env.id) { /* ack */ }
    if (env.op === "msg") {
      const div = document.createElement("div");
      div.innerHTML = `<span class="room">#${env.room || "?"}</span> ` +
        `<span class="from">${env.from || "anon"}${env.hops ? " · " + env.hops + " hop" : ""}</span><br>` +
        (env.body || "");
      log.prepend(div);
    }
  };
  ws.onclose = () => setTimeout(connect, 1500);
}
function send(env) { ws.send(JSON.stringify(env)); }
function subscribe(room) { send({op: "sub", room}); }
document.querySelector("#sub").onclick = () => subscribe(document.querySelector("#room").value);
document.querySelector("#pub").onclick = () => {
  let body;
  try { body = JSON.parse(document.querySelector("#body").value); } catch { body = document.querySelector("#body").value; }
  send({op: "pub", room: document.querySelector("#room").value, body});
};
connect();
