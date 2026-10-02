const CHUNK_BYTES = 1024 * 1024;
const MAX_BYTES = 256 * 1024 * 1024;
const RELAY_URL = location.host
  ? `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`
  : "wss://web-clipsync-production.up.railway.app/ws";

const $ = selector => document.querySelector(selector);
const ui = {
  room: $("#roomInput"), name: $("#deviceNameInput"), connect: $("#connectButton"),
  pill: $("#connectionPill"), connection: $("#connectionText"), devices: $("#deviceList"),
  deviceCount: $("#deviceCount"), deviceSummary: $("#deviceSummary"), path: $("#activePath"),
  pathSummary: $("#pathSummary"), last: $("#lastTransfer"), lastSummary: $("#lastTransferSummary"),
  history: $("#historyList"), preview: $("#clipboardPreview"), title: $("#clipboardTitle"),
  description: $("#clipboardDescription"), note: $("#permissionNote"), toast: $("#toast")
};
const ownId = getId("clipsync-browser-device-id");
let socket = null;
let room = "";
let reconnect = true;
let reconnectTimer;
let reconnectAttempt = 0;
let newestTimestamp = 0;
let seen = new Set();
let transfers = new Map();
let devices = [];
let history = loadHistory();
let imageUrls = new Set();
let toastTimer;

ui.room.value = formatRoom(localStorage.getItem("clipsync-last-room") || "");
ui.name.value = localStorage.getItem("clipsync-browser-device-name") || "Web browser";
renderHistory();
renderDevices();

ui.room.addEventListener("input", () => { ui.room.value = formatRoom(ui.room.value); });
ui.room.addEventListener("keydown", event => { if (event.key === "Enter") connect(); });
ui.name.addEventListener("change", () => localStorage.setItem("clipsync-browser-device-name", cleanName()));
ui.connect.addEventListener("click", () => socket?.readyState === WebSocket.OPEN ? disconnect() : connect());
$("#copyCodeButton").addEventListener("click", copyRoom);
$("#readClipboardButton").addEventListener("click", readAndSend);
$("#clearHistoryButton").addEventListener("click", clearHistory);

function connect() {
  const normalized = normalizeRoom(ui.room.value);
  if (normalized.length < 8) { showToast("Use at least 8 letters or numbers for the room code."); ui.room.focus(); return; }
  if (socket) socket.close();
  room = normalized;
  ui.room.value = formatRoom(room);
  localStorage.setItem("clipsync-last-room", room);
  reconnect = true;
  reconnectAttempt = 0;
  newestTimestamp = 0;
  seen = new Set();
  transfers.clear();
  setConnection("connecting", "Connecting…");
  try { socket = new WebSocket(RELAY_URL); }
  catch (error) { setConnection("disconnected", "Connection failed"); showToast(error.message); return; }
  socket.onopen = () => {
    reconnectAttempt = 0;
    setConnection("connected", "Connected");
    socket.send(JSON.stringify({ room, id: randomId(), type: "hello", data: "", ts: Date.now(), deviceId: ownId, deviceName: cleanName(), medium: "browser" }));
    devices = [{ id: ownId, name: cleanName(), medium: "browser" }, ...devices.filter(device => device.id !== ownId)];
    renderDevices();
    showToast(`Connected to room ${formatRoom(room)}`);
  };
  socket.onmessage = event => Promise.resolve(typeof event.data === "string" ? event.data : event.data.text()).then(raw => handleMessage(JSON.parse(raw))).catch(() => showToast("A relay message could not be read."));
  socket.onerror = () => setConnection("disconnected", "Connection error");
  socket.onclose = () => {
    socket = null;
    if (!reconnect) { setConnection("disconnected", "Not connected"); return; }
    setConnection("connecting", "Reconnecting…");
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connect, Math.min(30000, 1000 * 2 ** reconnectAttempt++));
  };
}

function disconnect() {
  reconnect = false;
  clearTimeout(reconnectTimer);
  if (socket) socket.close();
  socket = null;
  devices = [];
  setConnection("disconnected", "Not connected");
  renderDevices();
}

function handleMessage(message) {
  if (!message || message.room !== room || !message.id || seen.has(message.id)) return;
  seen.add(message.id);
  if (seen.size > 8192) seen.delete(seen.values().next().value);
  if (message.type === "presence") {
    devices = Array.isArray(message.devices) ? message.devices : [];
    renderDevices();
    return;
  }
  let payload = null;
  if (message.type === "text") payload = { text: message.data };
  if (message.type === "clipboard") {
    try { payload = JSON.parse(new TextDecoder().decode(fromBase64(message.data))); } catch { return; }
  }
  if (message.type === "chunk") payload = acceptChunk(message);
  if (!payload || message.ts < newestTimestamp) return;
  newestTimestamp = message.ts;
  addHistory(payload, "Received");
  showPreview(payload, "Received from a paired device");
  recordTransfer(payload, "Received");
  if ($("#autoApplyInput").checked) {
    writeClipboard(payload).then(() => ui.note.textContent = "Live apply is on — incoming clipboard content is written automatically.")
      .catch(() => ui.note.textContent = "The browser blocked automatic clipboard writing. Use Apply from history.");
  }
}

function acceptChunk(message) {
  if (!message.transferId || !Number.isInteger(message.chunkIndex) || !Number.isInteger(message.chunkCount) || message.chunkCount < 1 || message.chunkIndex < 0 || message.chunkIndex >= message.chunkCount || message.chunkCount > 1400) return null;
  let transfer = transfers.get(message.transferId);
  if (!transfer) { transfer = { ts: message.ts, count: message.chunkCount, chunks: new Map(), bytes: 0, made: Date.now() }; transfers.set(message.transferId, transfer); }
  if (transfer.ts !== message.ts || transfer.count !== message.chunkCount || transfer.chunks.has(message.chunkIndex)) return null;
  let bytes;
  try { bytes = fromBase64(message.data); } catch { transfers.delete(message.transferId); return null; }
  transfer.bytes += bytes.length;
  if (transfer.bytes > MAX_BYTES) { transfers.delete(message.transferId); return null; }
  transfer.chunks.set(message.chunkIndex, bytes);
  for (const [id, value] of transfers) if (value.made < Date.now() - 120000) transfers.delete(id);
  if (transfer.chunks.size !== transfer.count) return null;
  const joined = new Uint8Array(transfer.bytes);
  let offset = 0;
  for (let index = 0; index < transfer.count; index++) { const chunk = transfer.chunks.get(index); if (!chunk) return null; joined.set(chunk, offset); offset += chunk.length; }
  transfers.delete(message.transferId);
  try { return JSON.parse(new TextDecoder().decode(joined)); } catch { return null; }
}

async function readAndSend() {
  if (!socket || socket.readyState !== WebSocket.OPEN) { showToast("Connect to a room first."); return; }
  try {
    const payload = await readClipboard();
    showPreview(payload, "Ready to send");
    await sendClipboard(payload);
    addHistory(payload, "Sent");
    recordTransfer(payload, "Sent");
    showToast(`${payloadLabel(payload)} sent to the room.`);
  } catch (error) { showToast(error.message || "Could not read the browser clipboard."); }
}

async function sendClipboard(payload) {
  const normalized = normalizePayload(payload);
  const bytes = new TextEncoder().encode(JSON.stringify(normalized));
  if (!bytes.length || bytes.length > MAX_BYTES) throw new Error("Clipboard data is larger than 256 MiB.");
  const timestamp = Math.max(Date.now(), newestTimestamp + 1);
  newestTimestamp = timestamp;
  const transferId = randomId();
  const count = Math.ceil(bytes.length / CHUNK_BYTES);
  for (let index = 0; index < count; index++) {
    if (!socket || socket.readyState !== WebSocket.OPEN) throw new Error("The room disconnected during transfer.");
    while (socket.bufferedAmount > 4 * 1024 * 1024) await wait(4);
    const chunk = bytes.subarray(index * CHUNK_BYTES, Math.min((index + 1) * CHUNK_BYTES, bytes.length));
    const message = { room, id: randomId(), type: "chunk", data: toBase64(chunk), ts: timestamp, transferId, chunkIndex: index, chunkCount: count };
    seen.add(message.id);
    socket.send(JSON.stringify(message));
  }
}

async function readClipboard() {
  if (navigator.clipboard?.read) {
    const payload = {};
    for (const item of await navigator.clipboard.read()) for (const type of item.types) {
      if (type === "text/plain") payload.text = await (await item.getType(type)).text();
      else if (type === "text/html") payload.html = await (await item.getType(type)).text();
      else if (type === "text/rtf") payload.rtf = await (await item.getType(type)).text();
      else if (type.startsWith("image/")) { const blob = await item.getType(type); payload.image = { mime: type, data: toBase64(new Uint8Array(await blob.arrayBuffer())) }; }
    }
    return normalizePayload(payload);
  }
  if (navigator.clipboard?.readText) return { text: await navigator.clipboard.readText() };
  throw new Error("This browser does not provide clipboard access.");
}

async function writeClipboard(payload) {
  const normalized = normalizePayload(payload);
  if (!navigator.clipboard?.write || !globalThis.ClipboardItem) throw new Error("Rich clipboard writing is unavailable.");
  const values = {};
  if (normalized.text !== undefined) values["text/plain"] = new Blob([normalized.text], { type: "text/plain" });
  if (normalized.html !== undefined) values["text/html"] = new Blob([normalized.html], { type: "text/html" });
  if (normalized.rtf !== undefined) values["text/rtf"] = new Blob([normalized.rtf], { type: "text/rtf" });
  if (normalized.image) values[normalized.image.mime] = new Blob([fromBase64(normalized.image.data)], { type: normalized.image.mime });
  await navigator.clipboard.write([new ClipboardItem(values)]);
}

function normalizePayload(payload) {
  const result = {};
  if (typeof payload?.text === "string") result.text = payload.text;
  if (typeof payload?.html === "string") result.html = payload.html;
  if (typeof payload?.rtf === "string") result.rtf = payload.rtf;
  if (payload?.image?.data && typeof payload.image.data === "string") result.image = { mime: payload.image.mime || "image/png", data: payload.image.data };
  if (!Object.keys(result).length) throw new Error("The clipboard is empty.");
  return result;
}

function showPreview(payload, title) {
  ui.title.textContent = title;
  ui.description.textContent = `${payloadLabel(payload)} · ${formatBytes(payloadSize(payload))}`;
  ui.preview.replaceChildren();
  ui.preview.hidden = false;
  if (payload.image) { const image = document.createElement("img"); image.src = makeImageUrl(payload.image); ui.preview.append(image); }
  else ui.preview.textContent = payload.text || "Rich formatted content";
}

function addHistory(payload, direction) {
  const item = { id: randomId(), direction, kind: payloadLabel(payload), preview: payload.text || (payload.image ? "Image content" : "Rich formatted content"), size: payloadSize(payload), time: Date.now(), payload };
  if (payload.image) item.imageUrl = makeImageUrl(payload.image);
  history.unshift(item);
  history = history.slice(0, 30);
  saveHistory();
  renderHistory();
}

function renderHistory() {
  ui.history.replaceChildren();
  if (!history.length) { ui.history.innerHTML = '<div class="empty-state compact"><span class="empty-icon">◷</span><p>History is empty</p><small>Transfers will appear here while this dashboard is open.</small></div>'; return; }
  for (const item of history) {
    const row = document.createElement("div"); row.className = "history-row";
    const thumb = document.createElement("div"); thumb.className = "history-thumb";
    if (item.imageUrl) { const image = document.createElement("img"); image.src = item.imageUrl; thumb.append(image); } else thumb.textContent = item.kind === "Text" ? "T" : "✦";
    const info = document.createElement("div"); info.className = "history-info";
    const title = document.createElement("strong"); title.textContent = `${item.direction} · ${item.kind}`;
    const description = document.createElement("small"); description.textContent = `${truncate(item.preview, 42)} · ${formatBytes(item.size)}`;
    info.append(title, description);
    const time = document.createElement("span"); time.className = "history-time"; time.textContent = relativeTime(item.time);
    row.append(thumb, info, time);
    if (item.payload && item.direction === "Received") { const apply = document.createElement("button"); apply.className = "text-button"; apply.textContent = "Apply"; apply.onclick = () => writeClipboard(item.payload).then(() => showToast("Applied to your clipboard.")).catch(error => showToast(error.message)); row.append(apply); }
    ui.history.append(row);
  }
}

function renderDevices() {
  const visible = devices.length ? devices : (socket?.readyState === WebSocket.OPEN ? [{ id: ownId, name: cleanName(), medium: "browser" }] : []);
  ui.deviceCount.textContent = visible.length;
  const others = visible.filter(device => device.id !== ownId).length;
  ui.deviceSummary.textContent = others ? `${others} other ${others === 1 ? "device" : "devices"} in this room` : "Waiting for a paired device";
  ui.devices.replaceChildren();
  if (!visible.length) { ui.devices.innerHTML = '<div class="empty-state"><span class="empty-icon">⌁</span><p>No other devices yet</p><small>Open ClipSync on another PC and enter this room code.</small></div>'; return; }
  for (const device of visible) {
    const row = document.createElement("div"); row.className = "device-row";
    const avatar = document.createElement("span"); avatar.className = "device-avatar"; avatar.textContent = device.id === ownId ? "●" : "▣";
    const info = document.createElement("div"); info.className = "device-info";
    const name = document.createElement("strong"); name.textContent = device.name || "Unknown device";
    const detail = document.createElement("small"); detail.textContent = device.id === ownId ? "This browser · connected now" : `${device.medium === "relay" ? "Relay" : device.medium} · connected now`;
    info.append(name, detail);
    const state = document.createElement("span"); state.className = "device-state"; state.textContent = "ONLINE";
    row.append(avatar, info, state); ui.devices.append(row);
  }
}

function setConnection(state, label) {
  ui.pill.classList.toggle("connected", state === "connected");
  ui.connection.textContent = label;
  ui.path.textContent = state === "connected" ? "Relay" : "—";
  ui.pathSummary.textContent = state === "connected" ? "WebSocket · internet ready" : "Connect to see the live path";
  ui.connect.textContent = state === "connected" ? "Disconnect" : state === "connecting" ? "Connecting…" : "Connect";
  ui.connect.disabled = state === "connecting";
  if (state !== "connected") renderDevices();
}

function recordTransfer(payload, direction) { ui.last.textContent = payloadLabel(payload); ui.lastSummary.textContent = `${direction} · ${formatBytes(payloadSize(payload))} · just now`; }
async function copyRoom() { const value = normalizeRoom(ui.room.value); if (value.length < 8) { showToast("Enter a room code first."); return; } try { await navigator.clipboard.writeText(value); showToast("Room code copied."); } catch { showToast("Select and copy the code manually."); } }
function clearHistory() { for (const url of imageUrls) URL.revokeObjectURL(url); imageUrls.clear(); history = []; saveHistory(); renderHistory(); showToast("Clipboard history cleared."); }
function payloadLabel(payload) { if (payload.image && (payload.text || payload.html || payload.rtf)) return "Image + rich text"; if (payload.image) return "Image"; if (payload.html || payload.rtf) return "Rich text"; return "Text"; }
function payloadSize(payload) { return new TextEncoder().encode(JSON.stringify(payload)).length; }
function makeImageUrl(image) { const url = URL.createObjectURL(new Blob([fromBase64(image.data)], { type: image.mime || "image/png" })); imageUrls.add(url); return url; }
function formatBytes(value) { if (value < 1024) return `${value} B`; if (value < 1048576) return `${(value / 1024).toFixed(1)} KB`; return `${(value / 1048576).toFixed(1)} MB`; }
function truncate(value, length) { return value.length > length ? value.slice(0, length - 1) + "…" : value; }
function relativeTime(value) { const seconds = Math.max(0, Math.floor((Date.now() - value) / 1000)); return seconds < 60 ? "now" : `${Math.floor(seconds / 60)}m`; }
function cleanName() { return (ui.name.value || "Web browser").trim().replace(/\s+/g, " ").slice(0, 64) || "Web browser"; }
function normalizeRoom(value) { return (value || "").toUpperCase().replace(/[^A-Z0-9]/g, ""); }
function formatRoom(value) { const clean = normalizeRoom(value); return clean.match(/.{1,4}/g)?.join("-") || ""; }
function randomId() { return globalThis.crypto?.randomUUID?.() || `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`; }
function getId(key) { try { const saved = localStorage.getItem(key); if (saved) return saved; const value = randomId(); localStorage.setItem(key, value); return value; } catch { return randomId(); } }
function toBase64(bytes) { let text = ""; for (let index = 0; index < bytes.length; index += 0x8000) text += String.fromCharCode(...bytes.subarray(index, Math.min(index + 0x8000, bytes.length))); return btoa(text); }
function fromBase64(value) { const text = atob(value); const bytes = new Uint8Array(text.length); for (let index = 0; index < text.length; index++) bytes[index] = text.charCodeAt(index); return bytes; }
function wait(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function showToast(message) { ui.toast.textContent = message; ui.toast.classList.add("show"); clearTimeout(toastTimer); toastTimer = setTimeout(() => ui.toast.classList.remove("show"), 3200); }
function loadHistory() { try { return JSON.parse(localStorage.getItem("clipsync-browser-history") || "[]").slice(0, 30); } catch { return []; } }
function saveHistory() { try { localStorage.setItem("clipsync-browser-history", JSON.stringify(history.map(({ payload, imageUrl, ...item }) => item))); } catch { /* Storage is optional. */ } }
