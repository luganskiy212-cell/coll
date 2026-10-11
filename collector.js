import http from 'node:http';
import WebSocket from 'ws';
import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;
const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY;
const PORT = Number(process.env.PORT || 3000);

for (const [name, value] of Object.entries({
  SUPABASE_URL,
  SUPABASE_KEY,
  PUMPPORTAL_API_KEY,
})) {
  if (!value) {
    console.error(`[fatal] Missing environment variable: ${name}`);
    process.exit(1);
  }
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

const state = {
  startedAt: new Date().toISOString(),
  wsConnected: false,
  reconnects: 0,
  messages: 0,
  createsSaved: 0,
  migrationsSaved: 0,
  dbErrors: 0,
  lastMessageAt: null,
  lastCreateAt: null,
  lastMigrationAt: null,
  lastError: null,
};

let ws = null;
let reconnectTimer = null;
let watchdogTimer = null;
let reconnectDelayMs = 2000;
let connecting = false;

function nowIso() {
  return new Date().toISOString();
}

function short(value, n = 8) {
  if (!value || typeof value !== 'string') return '?';
  return value.length <= n * 2 ? value : `${value.slice(0, n)}…${value.slice(-n)}`;
}

async function saveCreate(ev) {
  if (ev.txType !== 'create') return;

  const mint = ev.mint;
  const creator = ev.traderPublicKey || ev.creator;
  if (!mint || !creator) {
    console.warn('[create] skipped: missing mint/creator', Object.keys(ev));
    return;
  }

  const row = {
    mint,
    creator,
    name: ev.name || null,
    symbol: ev.symbol || null,
    updated_at: nowIso(),
  };

  const { error } = await supabase
    .from('tokens')
    .upsert(row, { onConflict: 'mint' });

  if (error) {
    state.dbErrors += 1;
    state.lastError = `create DB: ${error.message}`;
    console.error('[create][db]', error.message);
    return;
  }

  state.createsSaved += 1;
  state.lastCreateAt = nowIso();
  console.log(`[create] ${ev.symbol || '?'} ${short(mint)} dev=${short(creator)}`);
}

async function saveMigration(ev) {
  const txType = String(ev.txType || '').toLowerCase();
  if (txType !== 'migrate' && txType !== 'migration') return;

  const mint = ev.mint;
  if (!mint) return;

  const migratedAt = nowIso();
  const { data, error } = await supabase
    .from('tokens')
    .update({
      graduated: true,
      graduated_at: migratedAt,
      updated_at: migratedAt,
    })
    .eq('mint', mint)
    .select('mint');

  if (error) {
    state.dbErrors += 1;
    state.lastError = `migration DB: ${error.message}`;
    console.error('[migration][db]', error.message);
    return;
  }

  if (!data?.length) {
    // Normal after a fresh collector start: a token may have been created before we began listening.
    console.warn(`[migration] ${short(mint)} not in local DB yet; skipped`);
    return;
  }

  state.migrationsSaved += 1;
  state.lastMigrationAt = migratedAt;
  console.log(`[migration] graduated ${short(mint)}`);
}

async function handleMessage(raw) {
  state.messages += 1;
  state.lastMessageAt = nowIso();

  let ev;
  try {
    ev = JSON.parse(raw.toString());
  } catch {
    return;
  }

  // PumpPortal can also send subscription acknowledgements/status messages.
  if (!ev || typeof ev !== 'object') return;

  try {
    if (ev.txType === 'create') {
      await saveCreate(ev);
      return;
    }

    const txType = String(ev.txType || '').toLowerCase();
    if (txType === 'migrate' || txType === 'migration') {
      await saveMigration(ev);
    }
  } catch (error) {
    state.lastError = error?.message || String(error);
    console.error('[message]', state.lastError);
  }
}

function clearTimers() {
  if (watchdogTimer) clearInterval(watchdogTimer);
  watchdogTimer = null;
}

function scheduleReconnect(reason = 'closed') {
  if (reconnectTimer) return;
  state.wsConnected = false;
  state.reconnects += 1;
  state.lastError = reason;

  const delay = reconnectDelayMs;
  reconnectDelayMs = Math.min(Math.round(reconnectDelayMs * 1.7), 30_000);
  console.log(`[ws] reconnect in ${delay}ms (${reason})`);

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);
}

function connect() {
  if (connecting || (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING))) {
    return;
  }

  connecting = true;
  const url = `wss://pumpportal.fun/api/data?api-key=${encodeURIComponent(PUMPPORTAL_API_KEY)}`;
  ws = new WebSocket(url);

  ws.on('open', () => {
    connecting = false;
    state.wsConnected = true;
    state.lastError = null;
    reconnectDelayMs = 2000;
    console.log('[ws] connected to PumpPortal');

    // One socket, both free streams. Do NOT create separate connections per token.
    ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
    ws.send(JSON.stringify({ method: 'subscribeMigration' }));

    clearTimers();
    watchdogTimer = setInterval(() => {
      if (!state.lastMessageAt) return;
      const silenceMs = Date.now() - Date.parse(state.lastMessageAt);
      if (silenceMs > 10 * 60_000 && ws?.readyState === WebSocket.OPEN) {
        console.warn('[ws] no events for 10 minutes; forcing reconnect');
        ws.terminate();
      }
    }, 60_000);
  });

  ws.on('message', (raw) => {
    // Deliberately process each event without opening any additional PumpPortal socket.
    handleMessage(raw);
  });

  ws.on('close', (code, reason) => {
    connecting = false;
    clearTimers();
    const text = `close ${code}${reason?.length ? `: ${reason.toString()}` : ''}`;
    console.warn(`[ws] ${text}`);
    scheduleReconnect(text);
  });

  ws.on('error', (error) => {
    connecting = false;
    state.lastError = error.message;
    console.error('[ws]', error.message);
    // close event normally follows and schedules the reconnect.
  });
}

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: true, service: 'KINGMAKER Collector', ...state }, null, 2));
    return;
  }
  res.writeHead(404, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ ok: false, error: 'not_found' }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[http] health server listening on :${PORT}`);
  connect();
});

function shutdown(signal) {
  console.log(`[shutdown] ${signal}`);
  clearTimers();
  if (reconnectTimer) clearTimeout(reconnectTimer);
  try { ws?.close(); } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
