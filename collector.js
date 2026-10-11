import http from 'node:http';
import WebSocket from 'ws';
import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;
const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY;
const PORT = Number(process.env.PORT || 3000);
const DEX_REFRESH_MS = Number(process.env.DEX_REFRESH_MS || 120000); // 2 min
const DEX_MAX_GRADUATED = Number(process.env.DEX_MAX_GRADUATED || 300);
const DEX_MAX_RECENT = Number(process.env.DEX_MAX_RECENT || 120);

for (const [name, value] of Object.entries({ SUPABASE_URL, SUPABASE_KEY, PUMPPORTAL_API_KEY })) {
  if (!value) {
    console.error(`[fatal] Missing environment variable: ${name}`);
    process.exit(1);
  }
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
  realtime: { transport: WebSocket },
});

const state = {
  startedAt: new Date().toISOString(),
  wsConnected: false,
  reconnects: 0,
  messages: 0,
  createsSaved: 0,
  migrationsSaved: 0,
  dbErrors: 0,
  dexRuns: 0,
  dexRequests: 0,
  dexPairsSeen: 0,
  dexTokensUpdated: 0,
  lastMessageAt: null,
  lastCreateAt: null,
  lastMigrationAt: null,
  lastDexRunAt: null,
  lastDexError: null,
  lastError: null,
};

let ws = null;
let reconnectTimer = null;
let watchdogTimer = null;
let enrichTimer = null;
let reconnectDelayMs = 2000;
let connecting = false;
let enrichRunning = false;

function nowIso() { return new Date().toISOString(); }
function hoursAgoIso(hours) { return new Date(Date.now() - hours * 3600_000).toISOString(); }
function short(value, n = 8) {
  if (!value || typeof value !== 'string') return '?';
  return value.length <= n * 2 ? value : `${value.slice(0, n)}…${value.slice(-n)}`;
}
function num(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}
function chunks(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

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
  const { error } = await supabase.from('tokens').upsert(row, { onConflict: 'mint' });
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
    .update({ graduated: true, graduated_at: migratedAt, updated_at: migratedAt })
    .eq('mint', mint)
    .select('mint');
  if (error) {
    state.dbErrors += 1;
    state.lastError = `migration DB: ${error.message}`;
    console.error('[migration][db]', error.message);
    return;
  }
  if (!data?.length) {
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
  try { ev = JSON.parse(raw.toString()); } catch { return; }
  if (!ev || typeof ev !== 'object') return;
  try {
    if (ev.txType === 'create') return await saveCreate(ev);
    const txType = String(ev.txType || '').toLowerCase();
    if (txType === 'migrate' || txType === 'migration') await saveMigration(ev);
  } catch (error) {
    state.lastError = error?.message || String(error);
    console.error('[message]', state.lastError);
  }
}

async function getDexCandidates() {
  // First priority: successful/migrated tokens from the last 48h.
  const { data: graduated, error: gError } = await supabase
    .from('tokens')
    .select('*')
    .eq('graduated', true)
    .gte('created_at', hoursAgoIso(48))
    .order('updated_at', { ascending: true })
    .limit(DEX_MAX_GRADUATED);
  if (gError) throw new Error(`candidate graduated query: ${gError.message}`);

  // Also sample the newest launches. If DexScreener indexes a pre-migration market,
  // we will enrich it too; otherwise it simply returns no pair and costs no DB write.
  const { data: recent, error: rError } = await supabase
    .from('tokens')
    .select('*')
    .gte('created_at', hoursAgoIso(2))
    .order('created_at', { ascending: false })
    .limit(DEX_MAX_RECENT);
  if (rError) throw new Error(`candidate recent query: ${rError.message}`);

  const map = new Map();
  for (const row of [...(graduated || []), ...(recent || [])]) map.set(row.mint, row);
  return [...map.values()];
}

function chooseBestPairs(pairs, candidateMap) {
  const best = new Map();
  for (const pair of pairs || []) {
    const base = pair?.baseToken?.address;
    const quote = pair?.quoteToken?.address;
    const mint = candidateMap.has(base) ? base : candidateMap.has(quote) ? quote : null;
    if (!mint) continue;
    const liq = num(pair?.liquidity?.usd);
    const prev = best.get(mint);
    if (!prev || liq > num(prev?.liquidity?.usd)) best.set(mint, pair);
  }
  return best;
}

async function fetchDexBatch(addresses) {
  const joined = addresses.map(encodeURIComponent).join(',');
  const url = `https://api.dexscreener.com/tokens/v1/solana/${joined}`;
  state.dexRequests += 1;
  const response = await fetch(url, {
    headers: { accept: 'application/json', 'user-agent': 'KINGMAKER/1.2' },
  });
  if (response.status === 429) throw new Error('DexScreener rate limit (429)');
  if (!response.ok) throw new Error(`DexScreener HTTP ${response.status}`);
  return await response.json();
}

async function enrichDex() {
  if (enrichRunning) return;
  enrichRunning = true;
  try {
    const candidates = await getDexCandidates();
    state.dexRuns += 1;
    state.lastDexRunAt = nowIso();
    state.lastDexError = null;
    if (!candidates.length) return;

    const candidateMap = new Map(candidates.map(row => [row.mint, row]));
    let updatedThisRun = 0;
    let pairsThisRun = 0;

    for (const batch of chunks(candidates, 30)) {
      let pairs;
      try {
        pairs = await fetchDexBatch(batch.map(row => row.mint));
      } catch (error) {
        state.lastDexError = error.message;
        console.warn('[dex]', error.message);
        if (String(error.message).includes('429')) await sleep(10_000);
        continue;
      }

      pairsThisRun += Array.isArray(pairs) ? pairs.length : 0;
      const bestPairs = chooseBestPairs(pairs, candidateMap);
      const rows = [];

      for (const [mint, pair] of bestPairs) {
        const old = candidateMap.get(mint);
        if (!old) continue;
        const marketCap = num(pair.marketCap, num(pair.fdv));
        const peak = Math.max(num(old.peak_market_cap_usd), marketCap);
        const buys = Math.max(0, Math.trunc(num(pair?.txns?.h24?.buys)));
        const sells = Math.max(0, Math.trunc(num(pair?.txns?.h24?.sells)));
        rows.push({
          ...old,
          market_cap_usd: marketCap,
          peak_market_cap_usd: peak,
          volume_24h_usd: num(pair?.volume?.h24),
          liquidity_usd: num(pair?.liquidity?.usd),
          txns_24h: buys + sells,
          buys_24h: buys,
          sells_24h: sells,
          price_change_24h: num(pair?.priceChange?.h24),
          updated_at: nowIso(),
        });
      }

      if (rows.length) {
        const { error } = await supabase.from('tokens').upsert(rows, { onConflict: 'mint' });
        if (error) {
          state.dbErrors += 1;
          state.lastDexError = `Dex DB: ${error.message}`;
          console.error('[dex][db]', error.message);
        } else {
          updatedThisRun += rows.length;
        }
      }

      // Be deliberately gentle with the public API even though its documented limit is much higher.
      await sleep(500);
    }

    state.dexPairsSeen += pairsThisRun;
    state.dexTokensUpdated += updatedThisRun;
    console.log(`[dex] candidates=${candidates.length} pairs=${pairsThisRun} updated=${updatedThisRun}`);
  } catch (error) {
    state.lastDexError = error?.message || String(error);
    console.error('[dex]', state.lastDexError);
  } finally {
    enrichRunning = false;
  }
}

function clearWsTimers() {
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
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, delay);
}
function connect() {
  if (connecting || (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING))) return;
  connecting = true;
  const url = `wss://pumpportal.fun/api/data?api-key=${encodeURIComponent(PUMPPORTAL_API_KEY)}`;
  ws = new WebSocket(url);

  ws.on('open', () => {
    connecting = false;
    state.wsConnected = true;
    state.lastError = null;
    reconnectDelayMs = 2000;
    console.log('[ws] connected to PumpPortal');
    ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
    ws.send(JSON.stringify({ method: 'subscribeMigration' }));

    clearWsTimers();
    watchdogTimer = setInterval(() => {
      if (!state.lastMessageAt) return;
      const silenceMs = Date.now() - Date.parse(state.lastMessageAt);
      if (silenceMs > 10 * 60_000 && ws?.readyState === WebSocket.OPEN) {
        console.warn('[ws] no events for 10 minutes; forcing reconnect');
        ws.terminate();
      }
    }, 60_000);
  });
  ws.on('message', raw => handleMessage(raw));
  ws.on('close', (code, reason) => {
    connecting = false;
    clearWsTimers();
    const text = `close ${code}${reason?.length ? `: ${reason.toString()}` : ''}`;
    console.warn(`[ws] ${text}`);
    scheduleReconnect(text);
  });
  ws.on('error', error => {
    connecting = false;
    state.lastError = error.message;
    console.error('[ws]', error.message);
  });
}

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: true, service: 'KINGMAKER Collector', version: '1.2', ...state }, null, 2));
    return;
  }
  res.writeHead(404, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ ok: false, error: 'not_found' }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[http] health server listening on :${PORT}`);
  connect();
  setTimeout(enrichDex, 15_000).unref();
  enrichTimer = setInterval(enrichDex, DEX_REFRESH_MS);
});

function shutdown(signal) {
  console.log(`[shutdown] ${signal}`);
  clearWsTimers();
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (enrichTimer) clearInterval(enrichTimer);
  try { ws?.close(); } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
