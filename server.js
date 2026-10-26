// VibeSync — signaling + relay server.
//
// A deliberately tiny WebSocket server. It knows nothing about WebRTC, chat, or
// video sync; it just groups sockets into rooms and forwards messages between the
// members of a room. Peers do all the real work themselves.
//
// It also keeps connections alive. Managed hosts (Render, Cloudflare, most
// reverse proxies) silently drop a WebSocket that sits idle for ~30-60s. During a
// long stretch of uninterrupted playback no sync/chat frames flow, so without a
// heartbeat the socket dies and pause/play stops propagating. We ping every 30s
// and reap sockets that stop answering; clients reconnect on their side.
//
// Message protocol (all JSON, one object per frame):
//   client -> server:
//     { type: "join", room, name }    join (or create) a room
//     { type: "signal", to, data }    WebRTC SDP/ICE, relayed to peer `to`
//     { type: "chat", text, name }    chat message, broadcast to the room
//     { type: "sync", action, time }  play/pause/seek, broadcast to the room
//     { type: "sync-request" }        "someone tell me the current playback state"
//     { type: "navigate", url, title, name }  "I opened this page" — offered to peers
//     { type: "kick", targetId }      king kicks a peer from the room
//     { type: "promote-king", targetId }  king promotes a peer to king
//     { type: "ping" }                keepalive; server replies { type: "pong" }
//   server -> client:
//     { type: "joined", selfId, peers, kings }  you're in; peers = [{ id, name }]
//     { type: "peer-joined", id, name }     someone new arrived
//     { type: "peer-left", id }             someone disconnected
//     { type: "signal", from, data }        relayed WebRTC signal
//     { type: "chat",   from, name, text }  relayed chat
//     { type: "sync",   from, action, time }relayed sync event
//     { type: "sync-request", from }        relayed resync request
//     { type: "navigate", from, name, url, title }  relayed shared link
//     { type: "kicked", reason }            you've been kicked
//     { type: "king-update", kings }        updated list of king peer ids
//     { type: "room-full" }                 room is at capacity
//     { type: "pong" }                      keepalive reply
//     { type: "error",  message }

import { WebSocketServer } from "ws";

const PORT = process.env.PORT || 8999;
const MAX_ROOM_SIZE = 4;

/** roomId -> Map<peerId, ws> */
const rooms = new Map();

/** roomId -> Set<peerId> (kings / room owners) */
const roomKings = new Map();

let nextId = 1;
const newId = () => `p${nextId++}`;

const wss = new WebSocketServer({ port: PORT });
console.log(`[vibesync] signaling server listening on ws://localhost:${PORT}`);

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

function getKings(roomId) {
  return roomKings.get(roomId) || new Set();
}

function isKing(roomId, peerId) {
  const kings = roomKings.get(roomId);
  return kings ? kings.has(peerId) : false;
}

function broadcastKingUpdate(roomId) {
  const kings = getKings(roomId);
  broadcast(roomId, { type: "king-update", kings: [...kings] });
}

function leaveRoom(ws) {
  const { roomId, peerId } = ws;
  if (!roomId) return;
  const members = rooms.get(roomId);
  if (members) {
    members.delete(peerId);

    // Remove from kings
    const kings = roomKings.get(roomId);
    if (kings) {
      kings.delete(peerId);

      if (members.size === 0) {
        rooms.delete(roomId);
        roomKings.delete(roomId);
      } else {
        // If no kings remain, promote the longest-standing member
        if (kings.size === 0) {
          const firstMember = members.keys().next().value;
          kings.add(firstMember);
          const firstWs = members.get(firstMember);
          console.log(`[room ${roomId}] No kings left, auto-promoting ${firstMember} (${firstWs.name})`);
        }
        broadcast(roomId, { type: "peer-left", id: peerId });
        broadcastKingUpdate(roomId);
      }
    } else {
      if (members.size === 0) rooms.delete(roomId);
      else broadcast(roomId, { type: "peer-left", id: peerId });
    }
  }
  ws.roomId = null;
}

wss.on("connection", (ws) => {
  ws.peerId = newId();
  ws.roomId = null;
  ws.name = "Guest";
  ws.isAlive = true;

  // Answer to protocol-level pings so the heartbeat below knows we're alive.
  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw) => {
    ws.isAlive = true; // any traffic counts as "still here"

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: "error", message: "invalid JSON" });
    }

    switch (msg.type) {
      case "join": {
        const roomId = String(msg.room || "").trim().toLowerCase();
        if (!roomId) return send(ws, { type: "error", message: "missing room" });

        leaveRoom(ws); // in case they were already in one
        ws.roomId = roomId;
        ws.name = String(msg.name || "Guest").slice(0, 40);

        if (!rooms.has(roomId)) rooms.set(roomId, new Map());
        if (!roomKings.has(roomId)) roomKings.set(roomId, new Set());
        const members = rooms.get(roomId);
        const kings = roomKings.get(roomId);

        // --- Deduplicate by name ---
        // If someone with the same name is already in the room, they are a
        // ghost from a dropped connection. Evict the old socket so the room
        // doesn't fill up with stale tiles.
        for (const [existingId, existingWs] of members) {
          if (existingWs.name === ws.name && existingId !== ws.peerId) {
            console.log(`[room ${roomId}] Evicting ghost peer ${existingId} (${existingWs.name}) — same name reconnected as ${ws.peerId}`);
            members.delete(existingId);
            kings.delete(existingId);
            broadcast(roomId, { type: "peer-left", id: existingId }, existingId);
            // Close the old socket so it doesn't linger.
            existingWs.roomId = null;
            try { existingWs.close(4001, "replaced"); } catch { /* already gone */ }
          }
        }

        // --- Check room capacity ---
        if (members.size >= MAX_ROOM_SIZE) {
          send(ws, { type: "room-full", max: MAX_ROOM_SIZE });
          ws.roomId = null;
          console.log(`[room ${roomId}] Rejected ${ws.peerId} (${ws.name}) — room full (${members.size}/${MAX_ROOM_SIZE})`);
          break;
        }

        // First member becomes king
        if (members.size === 0) {
          kings.add(ws.peerId);
          console.log(`[room ${roomId}] ${ws.peerId} (${ws.name}) is the first member — crowned king`);
        }

        const existing = [...members.entries()].map(([id, peer]) => ({ id, name: peer.name }));
        members.set(ws.peerId, ws);

        // Tell the newcomer who's already here (they will initiate offers).
        send(ws, { type: "joined", selfId: ws.peerId, peers: existing, kings: [...kings] });
        // Tell everyone else about the newcomer.
        broadcast(roomId, { type: "peer-joined", id: ws.peerId, name: ws.name }, ws.peerId);
        // Broadcast updated king list to everyone
        broadcastKingUpdate(roomId);
        console.log(`[room ${roomId}] ${ws.peerId} (${ws.name}) joined (${members.size} present)`);
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

      case "navigate":
        // A peer shared a page; relay the link so others can choose to follow.
        broadcast(ws.roomId, { type: "navigate", from: ws.peerId, name: msg.name, url: msg.url, title: msg.title }, ws.peerId);
        break;

      case "sync-request":
        // A peer just (re)connected and wants the room's current playback state.
        broadcast(ws.roomId, { type: "sync-request", from: ws.peerId }, ws.peerId);
        break;

      case "kick": {
        if (!ws.roomId) break;
        if (!isKing(ws.roomId, ws.peerId)) {
          send(ws, { type: "error", message: "Only kings can kick" });
          break;
        }
        const targetId = msg.targetId;
        const members = roomMembers(ws.roomId);
        const targetWs = members.get(targetId);
        if (!targetWs) break;

        // Can't kick another king
        if (isKing(ws.roomId, targetId)) {
          send(ws, { type: "error", message: "Cannot kick another king" });
          break;
        }

        console.log(`[room ${ws.roomId}] ${ws.peerId} (${ws.name}) kicked ${targetId} (${targetWs.name})`);
        send(targetWs, { type: "kicked", reason: `You were kicked by ${ws.name}` });
        // Remove them from the room
        targetWs.roomId = null;
        members.delete(targetId);
        broadcast(ws.roomId, { type: "peer-left", id: targetId });
        try { targetWs.close(4002, "kicked"); } catch { /* already gone */ }
        break;
      }

      case "promote-king": {
        if (!ws.roomId) break;
        if (!isKing(ws.roomId, ws.peerId)) {
          send(ws, { type: "error", message: "Only kings can promote" });
          break;
        }
        const targetId = msg.targetId;
        const members = roomMembers(ws.roomId);
        if (!members.has(targetId)) break;

        const kings = getKings(ws.roomId);
        kings.add(targetId);
        console.log(`[room ${ws.roomId}] ${ws.peerId} (${ws.name}) promoted ${targetId} to king`);
        broadcastKingUpdate(ws.roomId);
        break;
      }

      case "ping":
        send(ws, { type: "pong" });
        break;

      default:
        send(ws, { type: "error", message: `unknown type: ${msg.type}` });
    }
  });

  ws.on("close", () => leaveRoom(ws));
  ws.on("error", () => leaveRoom(ws));
});

// Heartbeat: every 30s ping everyone and drop anyone who didn't answer the last
// round. This keeps idle proxies from timing us out and cleans up half-open
// sockets (e.g. a laptop that slept) so their ghost tiles disappear for others.
const HEARTBEAT_MS = 30_000;
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      leaveRoom(ws);
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {
      /* socket already gone */
    }
  }
}, HEARTBEAT_MS);

wss.on("close", () => clearInterval(heartbeat));
