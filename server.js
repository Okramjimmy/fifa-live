const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const CHAT_FILE = path.join(__dirname, 'chat.json');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const PORT = process.env.PORT || 3000;
const POLL_INTERVAL_MS = 5_000;

// ─── State ───────────────────────────────────────────────────────
let liveViewers = 0;
let peakViewers = 0;
let cachedMatches = [];
let lastFetchedAt = null;
const chatHistory = []; // rolling buffer of last 80 messages
const MAX_HISTORY = 80;

// Load persisted chat from disk
try {
  const saved = JSON.parse(fs.readFileSync(CHAT_FILE, 'utf8'));
  if (Array.isArray(saved)) chatHistory.push(...saved.slice(-MAX_HISTORY));
  console.log(`[chat] Loaded ${chatHistory.length} messages from chat.json`);
} catch { /* file doesn't exist yet */ }

// Debounced JSON save — writes 3s after last message
let _saveTimer = null;
function persistChat() {
  clearTimeout(_saveTimer);
  _saveTimer = setTimeout(() => {
    fs.writeFile(CHAT_FILE, JSON.stringify(chatHistory, null, 2), err => {
      if (err) console.error('[chat] Save error:', err.message);
    });
  }, 3000);
}

// ─── Country flag map (ISO/ESPN abbreviation → emoji) ────────────
const FLAGS = {
  AUS: '🇦🇺', ARG: '🇦🇷', ALB: '🇦🇱', ALG: '🇩🇿', ARM: '🇦🇲',
  AUT: '🇦🇹', AZE: '🇦🇿', BEL: '🇧🇪', BIH: '🇧🇦', BOL: '🇧🇴',
  BRA: '🇧🇷', CAN: '🇨🇦', CHI: '🇨🇱', CHN: '🇨🇳', CIV: '🇨🇮',
  CMR: '🇨🇲', COL: '🇨🇴', CRC: '🇨🇷', CRO: '🇭🇷', CZE: '🇨🇿',
  DEN: '🇩🇰', ECU: '🇪🇨', EGY: '🇪🇬', ENG: '🏴󠁧󠁢󠁥󠁮󠁧󠁿', ESP: '🇪🇸',
  EST: '🇪🇪', FIN: '🇫🇮', FRA: '🇫🇷', GEO: '🇬🇪', GER: '🇩🇪',
  GHA: '🇬🇭', GRE: '🇬🇷', GTM: '🇬🇹', HON: '🇭🇳', HUN: '🇭🇺',
  IDN: '🇮🇩', IRL: '🇮🇪', IRN: '🇮🇷', IRQ: '🇮🇶', ISL: '🇮🇸',
  ISR: '🇮🇱', ITA: '🇮🇹', JAM: '🇯🇲', JPN: '🇯🇵', JOR: '🇯🇴',
  KAZ: '🇰🇿', KGZ: '🇰🇬', KOR: '🇰🇷', KOS: '🇽🇰', KWT: '🇰🇼',
  LAT: '🇱🇻', LTU: '🇱🇹', LUX: '🇱🇺', MAR: '🇲🇦', MEX: '🇲🇽',
  MKD: '🇲🇰', MNE: '🇲🇪', NED: '🇳🇱', NGA: '🇳🇬', NIR: '🇬🇧',
  NOR: '🇳🇴', NZL: '🇳🇿', OMN: '🇴🇲', PAN: '🇵🇦', PAR: '🇵🇾',
  PER: '🇵🇪', PHI: '🇵🇭', POL: '🇵🇱', POR: '🇵🇹', QAT: '🇶🇦',
  ROM: '🇷🇴', SAU: '🇸🇦', SCO: '🏴󠁧󠁢󠁳󠁣󠁴󠁿', SEN: '🇸🇳', SLV: '🇸🇻',
  SRB: '🇷🇸', SUI: '🇨🇭', SVK: '🇸🇰', SVN: '🇸🇮', SWE: '🇸🇪',
  THA: '🇹🇭', TJK: '🇹🇯', TRI: '🇹🇹', TUN: '🇹🇳', TUR: '🇹🇷',
  UAE: '🇦🇪', UKR: '🇺🇦', URU: '🇺🇾', USA: '🇺🇸', UZB: '🇺🇿',
  VEN: '🇻🇪', VNM: '🇻🇳', WAL: '🏴󠁧󠁢󠁷󠁬󠁳󠁿',
};

function flag(abbr) {
  return FLAGS[(abbr || '').toUpperCase()] || '🏳';
}

// ─── Parse ESPN scoreboard event ─────────────────────────────────
function parseEvent(event) {
  const comp = event.competitions?.[0];
  if (!comp) return null;

  const home = comp.competitors?.find(c => c.homeAway === 'home');
  const away = comp.competitors?.find(c => c.homeAway === 'away');
  const status = comp.status || {};
  const state = status.type?.state || 'pre'; // 'pre' | 'in' | 'post'

  return {
    id: String(event.id),
    homeTeam: home?.team?.displayName || home?.team?.name || 'TBD',
    homeAbbr: (home?.team?.abbreviation || '').toUpperCase(),
    homeFlag: flag(home?.team?.abbreviation),
    homeScore: state !== 'pre' ? parseInt(home?.score ?? 0) : null,
    awayTeam: away?.team?.displayName || away?.team?.name || 'TBD',
    awayAbbr: (away?.team?.abbreviation || '').toUpperCase(),
    awayFlag: flag(away?.team?.abbreviation),
    awayScore: state !== 'pre' ? parseInt(away?.score ?? 0) : null,
    clock: status.displayClock || '',
    period: status.period || 0,
    state,
    statusText: status.type?.shortDetail || status.type?.description || '',
    venue: comp.venue?.fullName || '',
    venueCity: comp.venue?.address?.city || '',
    date: event.date || null,
  };
}

// ─── ESPN public API — FIFA World Cup 2026 only ───────────────────
const ESPN_ENDPOINTS = [
  { url: 'https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard', comp: 'FIFA World Cup 2026' },
];

async function fetchEndpoint({ url }) {
  const res = await fetch(url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; FIFALive/1.0)' },
    signal: AbortSignal.timeout(10_000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function fetchAllScores() {
  const results = await Promise.allSettled(ESPN_ENDPOINTS.map(ep => fetchEndpoint(ep)));

  const all = [];
  for (let i = 0; i < results.length; i++) {
    const result = results[i];
    const comp = ESPN_ENDPOINTS[i].comp;
    if (result.status === 'fulfilled' && result.value?.events) {
      for (const event of result.value.events) {
        const parsed = parseEvent(event);
        if (parsed) all.push({ ...parsed, competition: comp });
      }
    }
  }

  // Deduplicate by match id
  const seen = new Set();
  const deduped = all.filter(m => !seen.has(m.id) && seen.add(m.id));

  // Sort: live → upcoming → finished
  const ORDER = { in: 0, pre: 1, post: 2 };
  return deduped.sort((a, b) => (ORDER[a.state] ?? 3) - (ORDER[b.state] ?? 3));
}

// ─── Poll & Broadcast ─────────────────────────────────────────────
async function pollAndBroadcast() {
  try {
    const matches = await fetchAllScores();
    if (matches.length > 0) {
      cachedMatches = matches;
      lastFetchedAt = new Date().toISOString();
    }

    const live = cachedMatches.filter(m => m.state === 'in').length;
    console.log(`[${new Date().toLocaleTimeString()}] Scores: ${cachedMatches.length} matches, ${live} live`);

    io.emit('scores', {
      matches: cachedMatches,
      fetchedAt: lastFetchedAt,
      liveCount: live,
    });
  } catch (err) {
    console.error('[poll] Error:', err.message);
  }
}

// ─── Socket.io ────────────────────────────────────────────────────
io.on('connection', (socket) => {
  liveViewers++;
  if (liveViewers > peakViewers) peakViewers = liveViewers;

  console.log(`[ws] connect  (viewers: ${liveViewers})`);
  io.emit('viewers', { current: liveViewers, peak: peakViewers });

  // Send cached scores immediately
  if (cachedMatches.length) {
    socket.emit('scores', {
      matches: cachedMatches,
      fetchedAt: lastFetchedAt,
      liveCount: cachedMatches.filter(m => m.state === 'in').length,
    });
  }

  // Send chat history to new client
  if (chatHistory.length) {
    socket.emit('chat:history', chatHistory);
  }

  // Real-time chat relay — broadcast to others only (prevents echo on sender)
  socket.on('chat', (data) => {
    if (!data || typeof data !== 'object') return;
    const name = String(data.name || 'Anonymous').trim().slice(0, 24);
    const text = String(data.text || '').trim();
    if (!text || text.length > 200) return;

    const msg = { name, text, ts: Date.now() };
    chatHistory.push(msg);
    if (chatHistory.length > MAX_HISTORY) chatHistory.shift();
    persistChat(); // async JSON write after 3s debounce

    socket.broadcast.emit('chat:remote', msg); // NOT sent back to sender
  });

  socket.on('disconnect', () => {
    liveViewers = Math.max(0, liveViewers - 1);
    console.log(`[ws] disconnect (viewers: ${liveViewers})`);
    io.emit('viewers', { current: liveViewers, peak: peakViewers });
  });
});

// ─── HTTP ─────────────────────────────────────────────────────────
// Expose scores as a REST endpoint as well
app.get('/api/scores', (_req, res) => {
  res.json({ matches: cachedMatches, fetchedAt: lastFetchedAt });
});

app.use(express.static(path.join(__dirname)));

// ─── Start ────────────────────────────────────────────────────────
pollAndBroadcast();
setInterval(pollAndBroadcast, POLL_INTERVAL_MS);

server.listen(PORT, () => {
  console.log(`\n⚽  FIFA Live  →  http://localhost:${PORT}`);
  console.log(`   Polling ESPN every ${POLL_INTERVAL_MS / 1000}s\n`);
});
