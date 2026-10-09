// Server sinkronisasi WeTV Bareng
// Tugasnya hanya meneruskan perintah play/pause/seek antar anggota ruang.
// Video tidak pernah lewat server ini.

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const rooms = new Map(); // kodeRuang -> { clients: Set<ws>, state }

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(`Server WeTV Bareng aktif. Ruang aktif: ${rooms.size}`);
});

const wss = new WebSocketServer({ server });

function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}

function broadcast(room, msg, except) {
  for (const c of room.clients) if (c !== except) send(c, msg);
}

function memberNames(room) {
  return [...room.clients].map((c) => c.name);
}

// Posisi video "sekarang" menurut catatan terakhir ruang
function currentState(room) {
  const s = room.state;
  if (!s) return null;
  return { ...s };
}

function leave(ws) {
  const room = rooms.get(ws.roomId);
  if (!room) return;
  room.clients.delete(ws);
  if (room.clients.size === 0) {
    rooms.delete(ws.roomId);
  } else {
    broadcast(room, { type: 'members', members: memberNames(room), left: ws.name });
  }
  ws.roomId = null;
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.name = 'Tamu';
  ws.roomId = null;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    const now = Date.now();

    switch (msg.type) {
      // Dipakai klien untuk menghitung selisih jam laptop vs server
      case 'ping':
        send(ws, { type: 'pong', t0: msg.t0, serverTime: now });
        break;

      case 'join': {
        const roomId = String(msg.room || '').trim().toLowerCase().slice(0, 40);
        if (!roomId) return;
        leave(ws);
        ws.name = String(msg.name || 'Tamu').trim().slice(0, 30) || 'Tamu';
        ws.roomId = roomId;
        if (!rooms.has(roomId)) rooms.set(roomId, { clients: new Set(), state: null });
        const room = rooms.get(roomId);
        room.clients.add(ws);
        send(ws, {
          type: 'joined',
          room: roomId,
          members: memberNames(room),
          state: currentState(room),
        });
        broadcast(room, { type: 'members', members: memberNames(room), joined: ws.name }, ws);
        break;
      }

      case 'leave':
        leave(ws);
        break;

      case 'action': {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        const out = {
          type: 'action',
          action: String(msg.action || ''),
          time: Number(msg.time) || 0,
          paused: !!msg.paused,
          url: typeof msg.url === 'string' ? msg.url.slice(0, 500) : null,
          from: ws.name,
          at: now, // dicap waktu server, dipakai untuk kompensasi delay
        };
        room.state = { time: out.time, paused: out.paused, url: out.url, at: now, from: ws.name };
        broadcast(room, out, ws);
        break;
      }

      // Klien minta posisi terakhir ruang (tombol "Sinkronkan sekarang")
      case 'sync-request': {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        send(ws, { type: 'state', state: currentState(room) });
        break;
      }
    }
  });

  ws.on('close', () => leave(ws));
});

// Putuskan koneksi yang sudah mati
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`Server WeTV Bareng berjalan di port ${PORT}`);
});
