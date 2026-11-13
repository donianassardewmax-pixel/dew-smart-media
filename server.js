
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { URL } = require('node:url');
const { WebSocketServer, WebSocket } = require('ws');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL is missing.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const rooms = new Map();
const socketsByRoom = new Map();
const writeQueues = new Map();

function defaultRoom(id) {
  return {
    id, on: true, status: 'stopped', playlist: [], index: -1,
    currentUrl: '', volume: 0.70, position: 0, startedAt: null,
    updatedAt: Date.now(), revision: 0
  };
}

async function initDatabase() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS rooms (
      id TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  const result = await pool.query('SELECT id, data FROM rooms');

  for (const row of result.rows) {
    rooms.set(row.id, row.data);
  }

  console.log(`Database ready. Loaded ${rooms.size} rooms.`);
}

function persistRoom(room) {
  const id = room.id;
  const data = JSON.stringify(room);

  const previous = writeQueues.get(id) || Promise.resolve();

  const next = previous
    .catch(() => {})
    .then(() => pool.query(
      `INSERT INTO rooms (id, data, updated_at)
       VALUES ($1, $2::jsonb, NOW())
       ON CONFLICT (id)
       DO UPDATE SET
         data = EXCLUDED.data,
         updated_at = NOW()`,
      [id, data]
    ))
    .catch(err => {
      console.error('Room database save failed:', err.message);
    });

  writeQueues.set(id, next);

  next.finally(() => {
    if (writeQueues.get(id) === next) {
      writeQueues.delete(id);
    }
  });
}

function getRoom(id) {
  if (!/^[a-f0-9-]{20,64}$/i.test(id)) return null;

  if (!rooms.has(id)) {
    const room = defaultRoom(id);
    rooms.set(id, room);
    persistRoom(room);
  }

  return rooms.get(id);
}

function currentPosition(room, now = Date.now()) {
  if (room.status === 'playing' && room.startedAt) {
    return Math.max(0, room.position + (now - room.startedAt) / 1000);
  }

  return Math.max(0, room.position || 0);
}

function snapshot(room) {
  const now = Date.now();

  return {
    id: room.id,
    on: room.on,
    status: room.status,
    playlist: room.playlist,
    index: room.index,
    currentUrl: room.currentUrl,
    volume: room.volume,
    position: currentPosition(room, now),
    startedAt: room.status === 'playing' ? now : null,
    serverTime: now,
    updatedAt: room.updatedAt,
    revision: room.revision
  };
}

function broadcast(room) {
  const set = socketsByRoom.get(room.id);
  if (!set) return;

  const payload = JSON.stringify({
    type: 'state',
    state: snapshot(room),
    viewers: set.size
  });

  for (const ws of set) {
    if (ws.readyState === WebSocket.OPEN) ws.send(payload);
  }
}

function normalizeUrl(value) {
  if (typeof value !== 'string' || value.length > 1800) return null;

  try {
    const u = new URL(value);
    if (!['http:', 'https:'].includes(u.protocol)) return null;
    return u.toString();
  } catch {
    return null;
  }
}

function applyCommand(room, command, value) {
  const now = Date.now();

  switch (command) {
    case 'on':
      room.on = true;
      break;

    case 'off':
      room.on = false;
      room.status = 'stopped';
      room.position = 0;
      room.startedAt = null;
      break;

    case 'add': {
      const url = normalizeUrl(value);

      if (!url) {
        return { ok: false, error: 'Invalid URL. Only HTTP/HTTPS links are accepted.' };
      }

      if (room.playlist.length >= 50) {
        return { ok: false, error: 'Playlist limit is 50 tracks.' };
      }

      room.playlist.push(url);

      if (room.index < 0) {
        room.index = 0;
        room.currentUrl = url;
      }

      break;
    }

    case 'clear':
      room.playlist = [];
      room.index = -1;
      room.currentUrl = '';
      room.status = 'stopped';
      room.position = 0;
      room.startedAt = null;
      break;

    case 'play':
      if (!room.currentUrl && room.playlist.length) {
        room.index = Math.max(0, room.index);
        room.currentUrl = room.playlist[room.index];
      }

      if (!room.currentUrl) {
        return { ok: false, error: 'No media URL is selected.' };
      }

      room.on = true;
      room.status = 'playing';
      room.startedAt = now;
      break;

    case 'pause':
      room.position = currentPosition(room, now);
      room.status = 'paused';
      room.startedAt = null;
      break;

    case 'stop':
      room.position = 0;
      room.status = 'stopped';
      room.startedAt = null;
      break;

    case 'seek': {
      const n = Number(value);

      if (!Number.isFinite(n) || n < 0) {
        return { ok: false, error: 'Invalid seek position.' };
      }

      room.position = n;

      if (room.status === 'playing') {
        room.startedAt = now;
      }

      break;
    }

    case 'volume': {
      const n = Number(value);

      if (!Number.isFinite(n)) {
        return { ok: false, error: 'Invalid volume.' };
      }

      room.volume = Math.min(1, Math.max(0, n));
      break;
    }

    case 'volume_up':
      room.volume = Math.min(1, Math.max(0, room.volume + Math.abs(Number(value) || 0.05)));
      break;

    case 'volume_down':
      room.volume = Math.min(1, Math.max(0, room.volume - Math.abs(Number(value) || 0.05)));
      break;

    case 'select': {
      const n = Number(value);

      if (!Number.isInteger(n) || n < 0 || n >= room.playlist.length) {
        return { ok: false, error: 'Invalid playlist index.' };
      }

      room.index = n;
      room.currentUrl = room.playlist[n];
      room.position = 0;
      room.status = 'playing';
      room.on = true;
      room.startedAt = now;
      break;
    }

    case 'next':
    case 'previous': {
      if (!room.playlist.length) {
        return { ok: false, error: 'Playlist is empty.' };
      }

      const len = room.playlist.length;

      if (room.index < 0) {
        room.index = 0;
      } else {
        room.index = (room.index + (command === 'next' ? 1 : -1) + len) % len;
      }

      room.currentUrl = room.playlist[room.index];
      room.position = 0;
      room.status = 'playing';
      room.on = true;
      room.startedAt = now;
      break;
    }

    case 'sync':
      break;

    default:
      return { ok: false, error: `Unknown command: ${command}` };
  }

  room.updatedAt = now;
  room.revision = (room.revision || 0) + 1;

  persistRoom(room);
  broadcast(room);

  return { ok: true, state: snapshot(room) };
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);

  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type'
  });

  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';

    req.on('data', chunk => {
      data += chunk;

      if (data.length > 12000) {
        reject(new Error('Body too large'));
        req.destroy();
      }
    });

    req.on('end', () => {
      if (!data) return resolve({});

      try {
        resolve(JSON.parse(data));
      } catch {
        reject(new Error('Invalid JSON'));
      }
    });

    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET,POST,OPTIONS',
      'access-control-allow-headers': 'content-type'
    });

    return res.end();
  }

  if (url.pathname === '/health') {
    return sendJson(res, 200, {
      ok: true,
      service: 'Dew Smart Media',
      time: Date.now()
    });
  }

  const roomMatch = url.pathname.match(/^\/api\/([a-f0-9-]{20,64})\/(state|cmd|lsl)$/i);

  if (roomMatch) {
    const [, id, action] = roomMatch;
    const room = getRoom(id);

    if (!room) {
      return sendJson(res, 400, { ok: false, error: 'Invalid room id.' });
    }

    if (action === 'state' && req.method === 'GET') {
      return sendJson(res, 200, snapshot(room));
    }

    if ((action === 'cmd' && req.method === 'POST') || (action === 'lsl' && req.method === 'POST')) {
      try {
        const body = await readBody(req);
        const result = applyCommand(room, body.cmd, body.value);

        return sendJson(res, result.ok ? 200 : 400, result);
      } catch (err) {
        return sendJson(res, 400, { ok: false, error: err.message });
      }
    }

    return sendJson(res, 405, { ok: false, error: 'Method not allowed.' });
  }

  const pageMatch = url.pathname.match(/^\/r\/([a-f0-9-]{20,64})$/i);

  if (pageMatch && req.method === 'GET') {
    const room = getRoom(pageMatch[1]);

    if (!room) return res.writeHead(404).end('Room not found');

    const html = fs.readFileSync(path.join(PUBLIC_DIR, 'index.html'), 'utf8')
      .replaceAll('__ROOM_ID__', room.id)
      .replaceAll('__WS_PATH__', '/ws');

    res.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-store',
      'content-security-policy': "default-src 'self' https: data: blob:; script-src 'self' 'unsafe-inline' https://www.youtube.com https://www.youtube-nocookie.com https://player.vimeo.com https://w.soundcloud.com https://connect.soundcloud.com; style-src 'self' 'unsafe-inline'; frame-src https:; connect-src 'self' wss: https:; media-src https: blob:"
    });

    return res.end(html);
  }

  if (url.pathname === '/') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });

    return res.end(`
      <!DOCTYPE html>
      <html>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Dew Smart Media</title>
      </head>
      <body style="background:#171326;color:white;text-align:center;font-family:Arial;padding:60px 20px">
        <h1>💜 DEW SMART MEDIA</h1>
        <p>Your shared media server is running successfully!</p>
        <p>Open your personal room from your Second Life object.</p>
      </body>
      </html>
    `);
  }

  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (url.pathname !== '/ws') return socket.destroy();

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit('connection', ws, req, url);
  });
});

wss.on('connection', (ws, req, url) => {
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('error', err => {
    console.warn('WebSocket client error:', err.message);
  });

  const roomId = url.searchParams.get('room');
  const room = roomId && getRoom(roomId);

  if (!room) return ws.close(1008, 'Invalid room');

  if (!socketsByRoom.has(roomId)) {
    socketsByRoom.set(roomId, new Set());
  }

  socketsByRoom.get(roomId).add(ws);

  ws.send(JSON.stringify({
    type: 'state',
    state: snapshot(room),
    viewers: socketsByRoom.get(roomId).size
  }));

  ws.on('message', raw => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type === 'command') {
        const result = applyCommand(room, msg.cmd, msg.value);

        if (!result.ok) {
          ws.send(JSON.stringify({ type: 'error', error: result.error }));
        }
      } else if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong', time: Date.now() }));
      }
    } catch {
      ws.send(JSON.stringify({ type: 'error', error: 'Invalid message.' }));
    }
  });

  ws.on('close', () => {
    const set = socketsByRoom.get(roomId);

    if (set) {
      set.delete(ws);

      if (!set.size) {
        socketsByRoom.delete(roomId);
      } else {
        broadcast(room);
      }
    }
  });
});

setInterval(() => {
  for (const room of rooms.values()) {
    if (room.on && room.status === 'playing' && socketsByRoom.has(room.id)) {
      broadcast(room);
    }
  }
}, 15000).unref();

const wsHeartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }

    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

wss.on('close', () => clearInterval(wsHeartbeat));

async function startServer() {
  try {
    await initDatabase();

    server.listen(PORT, HOST, () => {
      console.log(`Dew Smart Media listening on ${HOST}:${PORT}`);
    });
  } catch (err) {
    console.error('Database startup failed:', err.message);
    process.exit(1);
  }
}

startServer();

process.on('SIGTERM', () => {
  console.log('SIGTERM received; closing Dew Smart Media server.');

  clearInterval(wsHeartbeat);

  for (const ws of wss.clients) {
    ws.close(1001, 'Server shutting down');
  }

  server.close(async () => {
    await pool.end();
    process.exit(0);
  });

  setTimeout(() => process.exit(1), 25000).unref();
});
