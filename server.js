// Watch Together — signaling + relay server.
//
// A deliberately tiny WebSocket server. It knows nothing about WebRTC, chat, or
// video sync; it just groups sockets into rooms and forwards messages between the
// members of a room. Peers do all the real work themselves.
//
// Message protocol (all JSON, one object per frame):
//   client -> server:
//     { type: "join", room }          join (or create) a room
//     { type: "signal", to, data }    WebRTC SDP/ICE, relayed to peer `to`
//     { type: "chat", text, name }    chat message, broadcast to the room
//     { type: "sync", action, time }  play/pause/seek, broadcast to the room
//   server -> client:
//     { type: "joined", selfId, peers }     you're in; here are existing peers
//     { type: "peer-joined", id }           someone new arrived
//     { type: "peer-left", id }             someone disconnected
//     { type: "signal", from, data }        relayed WebRTC signal
//     { type: "chat",   from, name, text }  relayed chat
//     { type: "sync",   from, action, time }relayed sync event
//     { type: "error",  message }

import { WebSocketServer } from "ws";

const PORT = process.env.PORT || 8999;

/** roomId -> Map<peerId, ws> */
const rooms = new Map();

let nextId = 1;
const newId = () => `p${nextId++}`;

const wss = new WebSocketServer({ port: PORT });
console.log(`[watch-together] signaling server listening on ws://localhost:${PORT}`);

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function roomMembers(roomId) {
  return rooms.get(roomId) || new Map();
}

function broadcast(roomId, obj, exceptId) {
  for (const [id, ws] of roomMembers(roomId)) {
    if (id !== exceptId) send(ws, obj);
  }
}

function leaveRoom(ws) {
  const { roomId, peerId } = ws;
  if (!roomId) return;
  const members = rooms.get(roomId);
  if (members) {
    members.delete(peerId);
    if (members.size === 0) rooms.delete(roomId);
    else broadcast(roomId, { type: "peer-left", id: peerId });
  }
  ws.roomId = null;
}

wss.on("connection", (ws) => {
  ws.peerId = newId();
  ws.roomId = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: "error", message: "invalid JSON" });
    }

    switch (msg.type) {
      case "join": {
        const roomId = String(msg.room || "").trim();
        if (!roomId) return send(ws, { type: "error", message: "missing room" });

        leaveRoom(ws); // in case they were already in one
        ws.roomId = roomId;

        if (!rooms.has(roomId)) rooms.set(roomId, new Map());
        const members = rooms.get(roomId);

        const existing = [...members.keys()];
        members.set(ws.peerId, ws);

        // Tell the newcomer who's already here (they will initiate offers).
        send(ws, { type: "joined", selfId: ws.peerId, peers: existing });
        // Tell everyone else about the newcomer.
        broadcast(roomId, { type: "peer-joined", id: ws.peerId }, ws.peerId);
        console.log(`[room ${roomId}] ${ws.peerId} joined (${members.size} present)`);
        break;
      }

      case "signal": {
        const target = roomMembers(ws.roomId).get(msg.to);
        if (target) send(target, { type: "signal", from: ws.peerId, data: msg.data });
        break;
      }

      case "chat":
        broadcast(ws.roomId, { type: "chat", from: ws.peerId, name: msg.name, text: msg.text }, ws.peerId);
        break;

      case "sync":
        broadcast(ws.roomId, { type: "sync", from: ws.peerId, action: msg.action, time: msg.time }, ws.peerId);
        break;

      default:
        send(ws, { type: "error", message: `unknown type: ${msg.type}` });
    }
  });

  ws.on("close", () => leaveRoom(ws));
  ws.on("error", () => leaveRoom(ws));
});
