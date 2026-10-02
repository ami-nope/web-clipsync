# Clipboard Sync Railway relay

This is the production relay for the Windows Clipboard Sync client. It is
deliberately small: it keeps only live WebSocket membership in memory, routes
JSON messages to other clients in the same room, and never stores clipboard
contents.

## Local verification

```powershell
npm install
npm run check
npm start
```

Health: `http://localhost:8080/health`

WebSocket: `ws://localhost:8080/ws`

## Deploy independently to Railway

From this directory:

```powershell
railway login
railway init
railway up
```

Generate a public domain for the service in Railway. Configure the Windows
client with the resulting URL converted from `https://` to `wss://`, for
example:

```text
wss://your-generated-domain.up.railway.app/ws
```

Railway injects `PORT`; the server binds to `0.0.0.0` and exposes `/health` for
deployment health checks. No database or persistent volume is needed. A
deployment restart disconnects clients, and the Windows/browser clients
reconnect automatically.

## Relay behavior

- `hello` joins a connection to an 8–64 character uppercase alphanumeric room.
- `text`, `clipboard`, and chunked `chunk` messages are forwarded to every
  other live connection in that room.
- Sender echoes are not returned by the server.
- Payloads are limited to 4 MiB by default.
- Rooms are limited to 8 live clients by default.
- Per-connection message rate is limited to 120 messages/second.
- WebSocket compression is disabled to reduce latency and CPU use.
- Ping/pong heartbeat removes dead connections.

Set `MAX_PAYLOAD_BYTES`, `MAX_ROOM_CLIENTS`, or
`MAX_MESSAGES_PER_SECOND` as Railway variables if needed.

The relay is not an authentication service. The pairing code is a bearer
secret; use a fresh, random code and `wss://` in production.
