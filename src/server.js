import http from "node:http";
import process from "node:process";
import { WebSocketServer, WebSocket } from "ws";

const HOST = "0.0.0.0";
const PORT = positiveInteger(process.env.PORT, 8080);
const WS_PATH = normalizePath(process.env.RELAY_PATH || "/ws");
const MAX_PAYLOAD_BYTES = positiveInteger(process.env.MAX_PAYLOAD_BYTES, 4 * 1024 * 1024);
const MAX_ROOM_CLIENTS = positiveInteger(process.env.MAX_ROOM_CLIENTS, 8);
const MAX_MESSAGES_PER_SECOND = positiveInteger(process.env.MAX_MESSAGES_PER_SECOND, 120);
const ROOM_PATTERN = /^[A-Z0-9]{8,64}$/;
const ID_PATTERN = /^[A-Za-z0-9_-]{8,128}$/;

// The relay intentionally stores no clipboard contents. A room exists only while
// at least one live WebSocket is connected.
const rooms = new Map();
const stats = { connections: 0, messages: 0, rejected: 0 };

const httpServer = http.createServer((request, response) => {
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);

  if (request.method === "GET" && url.pathname === "/health") {
    const payload = JSON.stringify({
      ok: true,
      service: "clipsync-relay",
      rooms: rooms.size,
      connections: stats.connections,
      uptimeSeconds: Math.floor(process.uptime())
    });
    response.writeHead(200, {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "content-length": Buffer.byteLength(payload)
    });
    response.end(payload);
    return;
  }

  if (request.method === "GET" && url.pathname === "/") {
    response.writeHead(200, {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "no-store"
    });
    response.end("Clipboard Sync relay is running. WebSocket endpoint: " + WS_PATH + "\n");
    return;
  }

  response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  response.end("Not found\n");
});

const webSockets = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_PAYLOAD_BYTES,
  perMessageDeflate: false
});

httpServer.on("upgrade", (request, socket, head) => {
  let url;
  try {
    url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);
  } catch {
    rejectUpgrade(socket, 400, "Bad request");
    return;
  }

  if (url.pathname !== WS_PATH) {
    rejectUpgrade(socket, 404, "Not found");
    return;
  }

  webSockets.handleUpgrade(request, socket, head, (client) => {
    webSockets.emit("connection", client, request);
  });
});

webSockets.on("connection", (client, request) => {
  const state = {
    room: null,
    messageWindowStarted: Date.now(),
    messagesInWindow: 0,
    remoteAddress: request.socket.remoteAddress || "unknown"
  };
  stats.connections += 1;
  client.isAlive = true;

  client.on("pong", () => {
    client.isAlive = true;
  });

  client.on("message", (raw, isBinary) => {
    if (isBinary) {
      rejectMessage(client, "binary messages are not supported");
      return;
    }

    if (!allowMessage(state)) {
      rejectMessage(client, "message rate exceeded");
      return;
    }

    let message;
    try {
      message = JSON.parse(raw.toString("utf8"));
    } catch {
      rejectMessage(client, "message must be valid JSON");
      return;
    }

    const validation = validateMessage(message, state.room);
    if (!validation.ok) {
      rejectMessage(client, validation.error);
      return;
    }

    if (message.type === "hello") {
      joinRoom(client, state, message.room);
      return;
    }

    if (!state.room) {
      joinRoom(client, state, message.room);
      if (client.readyState !== WebSocket.OPEN) return;
    }

    stats.messages += 1;
    broadcast(state.room, client, JSON.stringify(message));
  });

  client.on("close", () => {
    stats.connections = Math.max(0, stats.connections - 1);
    leaveRoom(client, state);
  });

  client.on("error", () => {
    // The close event performs room cleanup. Do not log message contents or pairing codes.
  });
});

const heartbeat = setInterval(() => {
  for (const client of webSockets.clients) {
    if (client.isAlive === false) {
      client.terminate();
      continue;
    }
    client.isAlive = false;
    client.ping();
  }
}, 25_000);
heartbeat.unref();

httpServer.listen(PORT, HOST, () => {
  console.log(`Clipboard Sync relay listening on ${HOST}:${PORT}${WS_PATH}`);
});

function validateMessage(message, currentRoom) {
  if (!message || typeof message !== "object" || Array.isArray(message))
    return invalid("message must be an object");
  if (typeof message.room !== "string" || !ROOM_PATTERN.test(message.room))
    return invalid("room must be 8-64 uppercase letters or digits");
  if (typeof message.id !== "string" || !ID_PATTERN.test(message.id))
    return invalid("id is invalid");
  if (!["hello", "text", "clipboard", "chunk"].includes(message.type))
    return invalid("unsupported message type");
  if (!Number.isSafeInteger(message.ts) || message.ts < 0)
    return invalid("ts must be a non-negative integer");
  if (["text", "clipboard", "chunk"].includes(message.type) && typeof message.data !== "string")
    return invalid("message data must be a string");
  if (message.type === "hello" && message.data !== "")
    return invalid("hello data must be empty");
  if (message.type === "chunk") {
    if (typeof message.transferId !== "string" || !ID_PATTERN.test(message.transferId))
      return invalid("transferId is invalid");
    if (!Number.isSafeInteger(message.chunkIndex) || message.chunkIndex < 0)
      return invalid("chunkIndex is invalid");
    if (!Number.isSafeInteger(message.chunkCount) || message.chunkCount < 1 || message.chunkCount > 1400)
      return invalid("chunkCount is invalid");
    if (message.chunkIndex >= message.chunkCount)
      return invalid("chunkIndex is outside chunkCount");
  }
  if (currentRoom && message.room !== currentRoom)
    return invalid("a connection cannot change rooms");
  return { ok: true };
}

function joinRoom(client, state, room) {
  if (state.room === room) return true;
  const current = rooms.get(room) || new Set();
  if (current.size >= MAX_ROOM_CLIENTS) {
    rejectMessage(client, "room is full");
    return false;
  }
  current.add(client);
  rooms.set(room, current);
  state.room = room;
  return true;
}

function leaveRoom(client, state) {
  if (!state.room) return;
  const room = rooms.get(state.room);
  if (!room) return;
  room.delete(client);
  if (room.size === 0) rooms.delete(state.room);
  state.room = null;
}

function broadcast(roomName, sender, payload) {
  const room = rooms.get(roomName);
  if (!room) return;
  for (const peer of room) {
    if (peer === sender || peer.readyState !== WebSocket.OPEN) continue;
    peer.send(payload, { binary: false }, () => {
      // A send error will be followed by the WebSocket close/error cleanup.
    });
  }
}

function allowMessage(state) {
  const now = Date.now();
  if (now - state.messageWindowStarted >= 1000) {
    state.messageWindowStarted = now;
    state.messagesInWindow = 0;
  }
  state.messagesInWindow += 1;
  return state.messagesInWindow <= MAX_MESSAGES_PER_SECOND;
}

function rejectMessage(client, reason) {
  stats.rejected += 1;
  if (client.readyState === WebSocket.OPEN) {
    client.close(1008, reason.slice(0, 120));
  }
}

function invalid(error) {
  return { ok: false, error };
}

function rejectUpgrade(socket, status, message) {
  socket.write(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  socket.destroy();
}

function normalizePath(value) {
  const path = value.startsWith("/") ? value : `/${value}`;
  return path.length > 1 ? path.replace(/\/+$/, "") : path;
}

function positiveInteger(value, fallback) {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function shutdown(signal) {
  console.log(`${signal}: shutting down`);
  clearInterval(heartbeat);
  for (const client of webSockets.clients) {
    client.close(1012, "service restarting");
  }
  webSockets.close(() => {
    httpServer.close(() => process.exit(0));
  });
}

process.once("SIGTERM", () => shutdown("SIGTERM"));
process.once("SIGINT", () => shutdown("SIGINT"));
