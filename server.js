
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { URL } = require('node:url');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');
const MAX_TRACKS = 5000;
const MAX_BODY_SIZE = 1024 * 1024;

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
  const main = {
    id: 'main',
    name: 'Main Playlist',
    tracks: []
  };

  return {
    id,
    on: true,
    status: 'stopped',
    playlist: [],
    playlists: [main],
    activePlaylistId: 'main',
    index: -1,
    currentUrl: '',
    volume: 0.70,
    position: 0,
    startedAt: null,
    cinemaMode: false,
    shuffle: false,
    updatedAt: Date.now(),
    revision: 0
  };
}

function makeTrack(value) {
  const input = typeof value === 'string'
    ? { url: value }
    : value;

  if (!input || typeof input !== 'object') return null;

  const url = normalizeUrl(input.url);
  if (!url) return null;

  let title = typeof input.title === 'string'
    ? input.title.trim()
    : '';

  if (!title) {
    try {
      const parsed = new URL(url);
      const filename = decodeURIComponent(
        path.basename(parsed.pathname)
      );

      title = filename
        .replace(/\.[a-z0-9]{2,5}$/i, '')
        .replace(/[_-]+/g, ' ')
        .trim();

      if (!title) title = parsed.hostname;
    } catch {
      title = 'Untitled media';
    }
  }

  return {
    url,
    title: title.slice(0, 180)
  };
}

function normalizeRoom(room) {
  if (!room || typeof room !== 'object') return room;

  // Convert old single-playlist rooms to the new format.
  if (!Array.isArray(room.playlists) || !room.playlists.length) {
    const oldTracks = Array.isArray(room.playlist)
      ? room.playlist
      : [];

    room.playlists = [{
      id: 'main',
      name: 'Main Playlist',
      tracks: oldTracks.map(makeTrack).filter(Boolean)
    }];

    room.activePlaylistId = 'main';
  }

  room.playlists = room.playlists
    .filter(p => p && typeof p === 'object')
    .map((p, i) => ({
      id: String(p.id || (i === 0 ? 'main' : crypto.randomUUID())),
      name: String(p.name || (i === 0 ? 'Main Playlist' : 'Playlist'))
        .slice(0, 60),
      tracks: Array.isArray(p.tracks)
        ? p.tracks.map(makeTrack).filter(Boolean)
        : []
    }));

  if (!room.playlists.length) {
    room.playlists.push({
      id: 'main',
      name: 'Main Playlist',
      tracks: []
    });
  }

  if (!room.playlists.some(p => p.id === room.activePlaylistId)) {
    room.activePlaylistId = room.playlists[0].id;
  }

  if (typeof room.shuffle !== 'boolean') room.shuffle = false;
  if (!Number.isFinite(Number(room.volume))) room.volume = 0.70;
  if (!Number.isFinite(Number(room.position))) room.position = 0;
  if (!Number.isInteger(room.index)) room.index = -1;
  if (!Number.isFinite(Number(room.revision))) room.revision = 0;

  syncLegacyPlaylist(room);

  return room;
}

function activePlaylist(room) {
  return room.playlists.find(
    p => p.id === room.activePlaylistId
  ) || room.playlists[0];
}

function syncLegacyPlaylist(room) {
  const list = activePlaylist(room);
  room.playlist = list ? list.tracks.map(t => t.url) : [];

  if (!list || !list.tracks.length) {
    room.index = -1;
    room.currentUrl = '';
    return;
  }

  if (room.index < 0 || room.index >= list.tracks.length) {
    room.index = 0;
  }

  if (!room.currentUrl || !list.tracks.some(t => t.url === room.currentUrl)) {
    room.currentUrl = list.tracks[room.index].url;
  }
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
    rooms.set(row.id, normalizeRoom(row.data));
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

  return normalizeRoom(rooms.get(id));
}

function currentPosition(room, now = Date.now()) {
  if (room.status === 'playing' && room.startedAt) {
    return Math.max(
      0,
      room.position + (now - room.startedAt) / 1000
    );
  }

  return Math.max(0, room.position || 0);
}

function snapshot(room, options = null) {
  syncLegacyPlaylist(room);

  const now = Date.now();
  const list = activePlaylist(room);
  const tracks = list ? list.tracks : [];
  const currentTrack = room.index >= 0
    ? tracks[room.index]
    : null;

  const nextIndex = tracks.length
    ? (room.index + 1 + tracks.length) % tracks.length
    : -1;

  const nextTrack = nextIndex >= 0
    ? tracks[nextIndex]
    : null;

  const lslMode = !!(options && options.lsl);
  const trackOffset = lslMode
    ? Math.max(0, Number(options.trackOffset) || 0)
    : 0;
  const trackLimit = lslMode
    ? Math.min(12, Math.max(1, Number(options.trackLimit) || 6))
    : tracks.length;
  const playlistOffset = lslMode
    ? Math.max(0, Number(options.playlistOffset) || 0)
    : 0;
  const playlistLimit = lslMode
    ? Math.min(12, Math.max(1, Number(options.playlistLimit) || 6))
    : room.playlists.length;

  const playlistSummaries = room.playlists.map(p => ({
    id: p.id,
    name: p.name,
    count: p.tracks.length
  }));

  return {
    id: room.id,
    on: room.on,
    status: room.status,

    // Keep the full legacy URL array for the web player.
    // LSL receives an empty legacy array to avoid duplicating thousands of URLs.
    playlist: lslMode ? [] : room.playlist,

    // Web gets all tracks; LSL requests small pages to protect script memory.
    playlistItems: lslMode
      ? tracks.slice(trackOffset, trackOffset + trackLimit)
      : tracks,
    trackCount: tracks.length,
    trackOffset: lslMode ? trackOffset : 0,

    playlists: lslMode
      ? playlistSummaries.slice(playlistOffset, playlistOffset + playlistLimit)
      : playlistSummaries,
    playlistCount: room.playlists.length,
    playlistOffset: lslMode ? playlistOffset : 0,

    activePlaylistId: room.activePlaylistId,
    activePlaylistName: list ? list.name : 'Main Playlist',

    index: room.index,
    currentUrl: room.currentUrl,
    currentTitle: currentTrack ? currentTrack.title : '',
    nextTitle: nextTrack ? nextTrack.title : '',

    shuffle: !!room.shuffle,
    volume: room.volume,
    cinemaMode: !!room.cinemaMode,
    position: currentPosition(room, now),
    startedAt: room.status === 'playing' ? now : null,
    serverTime: now,
    updatedAt: room.updatedAt,
    revision: room.revision || 0
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
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  }
}

function normalizeUrl(value) {
  if (typeof value !== 'string' || value.length > 1800) {
    return null;
  }

  try {
    const u = new URL(value);

    if (!['http:', 'https:'].includes(u.protocol)) {
      return null;
    }

    return u.toString();
  } catch {
    return null;
  }
}

function startTrack(room, index, now) {
  const list = activePlaylist(room);

  if (!list || !list.tracks.length) return false;
  if (!Number.isInteger(index) || index < 0 || index >= list.tracks.length) {
    return false;
  }

  room.index = index;
  room.currentUrl = list.tracks[index].url;
  room.position = 0;
  room.status = 'playing';
  room.on = true;
  room.startedAt = now;

  return true;
}

function nextTrackIndex(room) {
  const list = activePlaylist(room);
  if (!list || !list.tracks.length) return -1;

  const length = list.tracks.length;

  if (room.shuffle && length > 1) {
    let index = room.index;

    // Avoid immediately repeating the current track.
    while (index === room.index) {
      index = Math.floor(Math.random() * length);
    }

    return index;
  }

  return (room.index + 1 + length) % length;
}

function previousTrackIndex(room) {
  const list = activePlaylist(room);
  if (!list || !list.tracks.length) return -1;

  const length = list.tracks.length;
  return (room.index - 1 + length) % length;
}

function applyCommand(room, command, value) {
  const now = Date.now();
  normalizeRoom(room);

  const list = activePlaylist(room);

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
      const track = makeTrack(value);

      if (!track) {
        return {
          ok: false,
          error: 'Invalid URL. Only HTTP/HTTPS links are accepted.'
        };
      }

      if (list.tracks.length >= MAX_TRACKS) {
        return {
          ok: false,
          error: `Playlist limit is ${MAX_TRACKS} tracks.`
        };
      }

      const duplicate = list.tracks.some(
        t => t.url === track.url
      );

      if (duplicate) {
        return {
          ok: false,
          error: 'This URL is already in the active playlist.'
        };
      }

      list.tracks.push(track);

      if (room.index < 0) {
        room.index = 0;
        room.currentUrl = track.url;
      }

      break;
    }

    case 'clear':
      list.tracks = [];
      room.index = -1;
      room.currentUrl = '';
      room.status = 'stopped';
      room.position = 0;
      room.startedAt = null;
      break;

    case 'remove': {
      const indexes = Array.isArray(value)
        ? value
        : value && Array.isArray(value.indices)
          ? value.indices
          : [value];

      const valid = [...new Set(
        indexes
          .map(Number)
          .filter(n => Number.isInteger(n) && n >= 0 && n < list.tracks.length)
      )].sort((a, b) => b - a);

      if (!valid.length) {
        return {
          ok: false,
          error: 'No valid playlist items were selected.'
        };
      }

      const oldIndex = room.index;
      const currentWasRemoved = valid.includes(oldIndex);
      const removedBeforeCurrent = valid.filter(i => i < oldIndex).length;

      for (const i of valid) {
        list.tracks.splice(i, 1);
      }

      if (!list.tracks.length) {
        room.index = -1;
        room.currentUrl = '';
        room.status = 'stopped';
        room.position = 0;
        room.startedAt = null;
      } else if (currentWasRemoved) {
        const newIndex = Math.min(oldIndex - removedBeforeCurrent, list.tracks.length - 1);
        startTrack(room, Math.max(0, newIndex), now);
      } else {
        room.index = oldIndex - removedBeforeCurrent;
        room.currentUrl = list.tracks[room.index].url;
      }

      break;
    }

    case 'play':
      if (!room.currentUrl && list.tracks.length) {
        room.index = Math.max(0, room.index);
        room.currentUrl = list.tracks[room.index].url;
      }

      if (!room.currentUrl) {
        return {
          ok: false,
          error: 'No media URL is selected.'
        };
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
        return {
          ok: false,
          error: 'Invalid seek position.'
        };
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
        return {
          ok: false,
          error: 'Invalid volume.'
        };
      }

      room.volume = Math.min(1, Math.max(0, n));
      break;
    }

    case 'volume_up':
      room.volume = Math.min(
        1,
        Math.max(0, room.volume + Math.abs(Number(value) || 0.05))
      );
      break;

    case 'volume_down':
      room.volume = Math.min(
        1,
        Math.max(0, room.volume - Math.abs(Number(value) || 0.05))
      );
      break;

    case 'select':
    case 'play_now': {
      const n = Number(
        value && typeof value === 'object' ? value.index : value
      );

      if (!startTrack(room, n, now)) {
        return {
          ok: false,
          error: 'Invalid playlist index.'
        };
      }

      break;
    }

    case 'play_next': {
      const n = Number(
        value && typeof value === 'object' ? value.index : value
      );

      if (!Number.isInteger(n) || n < 0 || n >= list.tracks.length) {
        return {
          ok: false,
          error: 'Invalid playlist index.'
        };
      }

      if (n !== room.index) {
        const track = list.tracks.splice(n, 1)[0];
        const insertAt = room.index < 0
          ? 0
          : room.index + 1;

        list.tracks.splice(insertAt, 0, track);

        if (n < room.index) room.index++;
      }

      break;
    }

    case 'next': {
      if (!list.tracks.length) {
        return {
          ok: false,
          error: 'Playlist is empty.'
        };
      }

      const next = nextTrackIndex(room);
      startTrack(room, next, now);
      break;
    }

    case 'previous': {
      if (!list.tracks.length) {
        return {
          ok: false,
          error: 'Playlist is empty.'
        };
      }

      startTrack(room, previousTrackIndex(room), now);
      break;
    }

    case 'shuffle':
      room.shuffle = typeof value === 'boolean'
        ? value
        : !room.shuffle;
      break;

    case 'sync':
      room.position = currentPosition(room, now);

      if (room.status === 'playing') {
        room.startedAt = now;
      } else {
        room.startedAt = null;
      }
      break;

    case 'auto_next': {
      const expectedRevision = Number(value);
      const currentRevision = Number(room.revision || 0);

      if (
        !Number.isFinite(expectedRevision) ||
        expectedRevision !== currentRevision ||
        room.status !== 'playing' ||
        !room.on
      ) {
        return {
          ok: true,
          ignored: true,
          state: snapshot(room)
        };
      }

      if (!list.tracks.length) {
        return {
          ok: false,
          error: 'Playlist is empty.'
        };
      }

      startTrack(room, nextTrackIndex(room), now);
      break;
    }

    case 'playlist_create': {
      const name = String(
        value && typeof value === 'object' ? value.name : value
      ).trim().slice(0, 60);

      if (!name) {
        return {
          ok: false,
          error: 'Please enter a playlist name.'
        };
      }

      if (room.playlists.some(
        p => p.name.toLowerCase() === name.toLowerCase()
      )) {
        return {
          ok: false,
          error: 'A playlist with this name already exists.'
        };
      }

      const created = {
        id: crypto.randomUUID(),
        name,
        tracks: []
      };

      room.playlists.push(created);
      room.activePlaylistId = created.id;
      room.index = -1;
      room.currentUrl = '';
      room.status = 'stopped';
      room.position = 0;
      room.startedAt = null;
      break;
    }

    case 'playlist_select': {
      const id = String(
        value && typeof value === 'object' ? value.id : value
      );

      const selected = room.playlists.find(p => p.id === id);

      if (!selected) {
        return {
          ok: false,
          error: 'Playlist not found.'
        };
      }

      room.activePlaylistId = selected.id;
      room.index = selected.tracks.length ? 0 : -1;
      room.currentUrl = selected.tracks.length
        ? selected.tracks[0].url
        : '';
      room.position = 0;
      room.startedAt = null;
      room.status = 'stopped';
      break;
    }

    case 'playlist_rename': {
      const data = value && typeof value === 'object' ? value : {};
      const target = room.playlists.find(p => p.id === String(data.id || ''));

      const name = String(data.name || '').trim().slice(0, 60);

      if (!target || !name) {
        return {
          ok: false,
          error: 'Invalid playlist or name.'
        };
      }

      if (room.playlists.some(
        p => p.id !== target.id && p.name.toLowerCase() === name.toLowerCase()
      )) {
        return {
          ok: false,
          error: 'A playlist with this name already exists.'
        };
      }

      target.name = name;
      break;
    }

    case 'playlist_delete': {
      const id = String(
        value && typeof value === 'object' ? value.id : value
      );

      if (room.playlists.length <= 1) {
        return {
          ok: false,
          error: 'At least one playlist must remain.'
        };
      }

      const targetIndex = room.playlists.findIndex(p => p.id === id);

      if (targetIndex < 0) {
        return {
          ok: false,
          error: 'Playlist not found.'
        };
      }

      const wasActive = room.playlists[targetIndex].id === room.activePlaylistId;
      room.playlists.splice(targetIndex, 1);

      if (wasActive) {
        const replacement = room.playlists[0];
        room.activePlaylistId = replacement.id;
        room.index = replacement.tracks.length ? 0 : -1;
        room.currentUrl = replacement.tracks.length
          ? replacement.tracks[0].url
          : '';
        room.status = 'stopped';
        room.position = 0;
        room.startedAt = null;
      }

      break;
    }

    case 'cinema':
      room.cinemaMode = !!value;
      break;

    default:
      return {
        ok: false,
        error: `Unknown command: ${command}`
      };
  }

  syncLegacyPlaylist(room);

  room.updatedAt = now;
  room.revision = (room.revision || 0) + 1;

  persistRoom(room);
  broadcast(room);

  return {
    ok: true,
    state: snapshot(room)
  };
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
    let tooLarge = false;

    req.on('data', chunk => {
      if (tooLarge) return;

      data += chunk.toString();

      if (Buffer.byteLength(data) > MAX_BODY_SIZE) {
        tooLarge = true;
        reject(new Error('Request body is too large.'));
        req.destroy();
      }
    });

    req.on('end', () => {
      if (tooLarge) return;
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
  const url = new URL(
    req.url,
    `http://${req.headers.host || 'localhost'}`
  );

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

  const roomMatch = url.pathname.match(
    /^\/api\/([a-f0-9-]{20,64})\/(state|cmd|lsl)$/i
  );

  if (roomMatch) {
    const [, id, action] = roomMatch;
    const room = getRoom(id);

    if (!room) {
      return sendJson(res, 400, {
        ok: false,
        error: 'Invalid room id.'
      });
    }

    if (action === 'state' && req.method === 'GET') {
      const lslMode = url.searchParams.get('lsl') === '1';

      if (lslMode) {
        return sendJson(res, 200, snapshot(room, {
          lsl: true,
          trackOffset: url.searchParams.get('trackOffset'),
          trackLimit: url.searchParams.get('trackLimit'),
          playlistOffset: url.searchParams.get('playlistOffset'),
          playlistLimit: url.searchParams.get('playlistLimit')
        }));
      }

      return sendJson(res, 200, snapshot(room));
    }

    if (
      (action === 'cmd' && req.method === 'POST') ||
      (action === 'lsl' && req.method === 'POST')
    ) {
      try {
        const body = await readBody(req);
        const result = applyCommand(room, body.cmd, body.value);

        // LSL only needs a compact state response. Returning the full playlist
        // here can send thousands of tracks into a memory-limited LSL script.
        if (action === 'lsl' && result.ok) {
          result.state = snapshot(room, {
            lsl: true,
            trackOffset: 0,
            trackLimit: 1,
            playlistOffset: 0,
            playlistLimit: 1
          });
        }

        return sendJson(res, result.ok ? 200 : 400, result);
      } catch (err) {
        return sendJson(res, 400, {
          ok: false,
          error: err.message
        });
      }
    }

    return sendJson(res, 405, {
      ok: false,
      error: 'Method not allowed.'
    });
  }

  const pageMatch = url.pathname.match(
    /^\/r\/([a-f0-9-]{20,64})$/i
  );

  if (pageMatch && req.method === 'GET') {
    const room = getRoom(pageMatch[1]);

    if (!room) {
      return res.writeHead(404).end('Room not found');
    }

    const html = fs.readFileSync(
      path.join(PUBLIC_DIR, 'index.html'),
      'utf8'
    )
      .replaceAll('__ROOM_ID__', room.id)
      .replaceAll('__WS_PATH__', '/ws');

    res.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-store',
      'content-security-policy':
        "default-src 'self' https: data: blob:; script-src 'self' 'unsafe-inline' https://www.youtube.com https://www.youtube-nocookie.com https://player.vimeo.com https://w.soundcloud.com https://connect.soundcloud.com; style-src 'self' 'unsafe-inline'; frame-src https:; connect-src 'self' wss: https:; media-src https: blob:"
    });

    return res.end(html);
  }

  if (url.pathname === '/') {
    res.writeHead(200, {
      'content-type': 'text/html; charset=utf-8'
    });

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

  res.writeHead(404, {
    'content-type': 'text/plain; charset=utf-8'
  });

  res.end('Not found');
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(
    req.url,
    `http://${req.headers.host || 'localhost'}`
  );

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
          ws.send(JSON.stringify({
            type: 'error',
            error: result.error
          }));
        }
      } else if (msg.type === 'ping') {
        ws.send(JSON.stringify({
          type: 'pong',
          time: Date.now()
        }));
      }
    } catch {
      ws.send(JSON.stringify({
        type: 'error',
        error: 'Invalid message.'
      }));
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
    if (
      room.on &&
      room.status === 'playing' &&
      socketsByRoom.has(room.id)
    ) {
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
