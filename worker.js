// SolRadar server — Cloudflare Worker (free plan)
// Helius pushes every trade from your tracked wallets here the moment it happens.
// This worker turns it into a Telegram alert, keeps the live feed for the app,
// and every 6 hours auto-tracks the most profitable famous traders of the last 24h.
//
// Secrets (set by the GitHub workflow): HELIUS_API_KEY, SOLANA_TRACKER_KEY,
// TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, APP_PASSCODE.  KV binding: KV.

const WSOL = 'So11111111111111111111111111111111111111112';
const USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const USDT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';
const QUOTES = new Set([WSOL, USDC, USDT]);
const ADDR_RE = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;
const HELIUS_API = 'https://api-mainnet.helius-rpc.com/v0/webhooks';
const ST = 'https://data.solanatracker.io';
const DS = 'https://api.dexscreener.com';
const MAX_TRADES = 300;
const CLUSTER_WINDOW = 3600;      // seconds
const MAX_KV_WRITES = 900;        // free plan allows 1,000/day
const BOT_TRADES_PER_DAY = 300;   // auto wallets above this are dropped

const DEFAULT_CFG = { wallets: [], paused: false, autoOn: true, autoN: 10, minSol: 0.1, hookId: '', url: '', appUrl: '' };
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'x-pass, content-type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Max-Age': '86400',
};

// ---------------------------------------------------------------- helpers
const json = (v, status = 200) => new Response(JSON.stringify(v), { status, headers: { 'content-type': 'application/json', ...CORS } });
const esc = s => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const short = a => `${a.slice(0, 4)}…${a.slice(-4)}`;
const today = () => new Date().toISOString().slice(0, 10);
function fmtNum(n) {
  n = +n; if (!isFinite(n)) return '?';
  const a = Math.abs(n);
  if (a >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (a >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  if (a >= 1) return n.toFixed(2);
  return n.toPrecision(3);
}
const usd = n => (isFinite(+n) ? (n < 0 ? '−$' : '$') + fmtNum(Math.abs(n)) : '?');
const signedUsd = n => (isFinite(+n) ? (n >= 0 ? '+$' : '−$') + fmtNum(Math.abs(n)) : '?');
async function sha(s) {
  const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return [...new Uint8Array(b)].map(x => x.toString(16).padStart(2, '0')).join('');
}
const hookSecret = env => sha('solradar-hook:' + env.APP_PASSCODE);
function safeEq(a, b) {
  a = String(a || ''); b = String(b || '');
  if (!a || a.length !== b.length) return false;
  let r = 0; for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}

// ---------------------------------------------------------------- storage
async function getCfg(env) { return { ...DEFAULT_CFG, ...((await env.KV.get('cfg', 'json')) || {}) }; }
async function putCfg(env, cfg) { await env.KV.put('cfg', JSON.stringify(cfg)); }
async function getState(env) {
  const s = (await env.KV.get('state', 'json')) || {};
  s.trades ||= []; s.pos ||= {}; s.clusters ||= {}; s.cnt ||= {};
  if (s.day !== today()) { s.day = today(); s.writes = 0; s.cnt = {}; }
  return s;
}
async function putState(env, s) {
  if ((s.writes || 0) >= MAX_KV_WRITES) return false; // keep inside the free daily limit
  s.writes = (s.writes || 0) + 1;
  await env.KV.put('state', JSON.stringify(s));
  return true;
}

// ---------------------------------------------------------------- Telegram
async function tg(env, method, body) {
  const r = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
  });
  return r.json().catch(() => ({ ok: false }));
}
const send = (env, text, chat = env.TELEGRAM_CHAT_ID) =>
  tg(env, 'sendMessage', { chat_id: chat, text, parse_mode: 'HTML', disable_web_page_preview: true });

// ---------------------------------------------------------------- market data
async function tokenInfo(mint) {
  try {
    const r = await fetch(`${DS}/tokens/v1/solana/${mint}`, { cf: { cacheTtl: 30 } });
    const pairs = await r.json();
    const p = (pairs || []).filter(x => x.baseToken?.address === mint)
      .sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0];
    if (!p) return null;
    return { sym: p.baseToken.symbol, name: p.baseToken.name, price: +p.priceUsd, mc: p.marketCap || p.fdv, liq: p.liquidity?.usd,
      created: p.pairCreatedAt, url: p.url, h1: p.priceChange?.h1, img: p.info?.imageUrl || '' };
  } catch { return null; }
}
function marketLine(i) {
  if (!i) return '📊 Not on DexScreener yet (very new)';
  const age = i.created ? fmtAge(Date.now() - i.created) : '?';
  const h1 = i.h1 != null ? ` · 1h ${i.h1 > 0 ? '+' : ''}${i.h1}%` : '';
  return `📊 MC ${usd(i.mc)} · Liq ${usd(i.liq)} · Age ${age}${h1}`;
}
function fmtAge(ms) {
  const m = ms / 60000;
  if (m < 60) return Math.max(1, Math.round(m)) + 'm';
  if (m < 1440) return Math.round(m / 60) + 'h';
  return Math.round(m / 1440) + 'd';
}
async function stGet(env, path) {
  const r = await fetch(ST + path, { headers: { 'x-api-key': env.SOLANA_TRACKER_KEY } });
  if (!r.ok) throw new Error(`Solana Tracker ${r.status}`);
  return r.json();
}
async function tokenBalance(env, wallet, mint) {
  try {
    const r = await fetch(`https://mainnet.helius-rpc.com/?api-key=${env.HELIUS_API_KEY}`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getTokenAccountsByOwner', params: [wallet, { mint }, { encoding: 'jsonParsed' }] }),
    });
    const d = await r.json();
    return (d.result?.value || []).reduce((s, a) => s + (+a.account.data.parsed.info.tokenAmount.uiAmountString || 0), 0);
  } catch { return null; }
}

// ---------------------------------------------------------------- Helius webhook management
async function syncHook(env, cfg) {
  const addrs = cfg.wallets.map(w => w.addr);
  if (!cfg.url) return { ok: false, error: 'Server URL unknown. Run setup again.' };
  const body = {
    webhookURL: cfg.url + '/hook/helius', transactionTypes: ['ANY'], accountAddresses: addrs,
    webhookType: 'enhanced', authHeader: await hookSecret(env), txnStatus: 'success',
  };
  const key = encodeURIComponent(env.HELIUS_API_KEY);
  if (!cfg.hookId) {
    const list = await fetch(`${HELIUS_API}?api-key=${key}`).then(r => r.json()).catch(() => []);
    const mine = Array.isArray(list) ? list.find(h => h.webhookURL === body.webhookURL) : null;
    if (mine) cfg.hookId = mine.webhookID;
  }
  if (!addrs.length) return { ok: true, note: 'No wallets yet' };
  const r = cfg.hookId
    ? await fetch(`${HELIUS_API}/${cfg.hookId}?api-key=${key}`, { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
    : await fetch(`${HELIUS_API}?api-key=${key}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  const d = await r.json().catch(() => ({}));
  if (!r.ok) {
    if (r.status === 404 && cfg.hookId) { cfg.hookId = ''; return syncHook(env, cfg); }
    return { ok: false, error: `Helius ${r.status}: ${JSON.stringify(d).slice(0, 200)}` };
  }
  if (d.webhookID) cfg.hookId = d.webhookID;
  return { ok: true };
}

// ---------------------------------------------------------------- swap parsing (Helius enhanced format)
function parseSwap(tx, wallet) {
  if (!tx || tx.transactionError) return null;
  let sol = 0;
  const bal = {};
  for (const a of tx.accountData || []) {
    if (a.account === wallet) sol += (a.nativeBalanceChange || 0) / 1e9;
    for (const t of a.tokenBalanceChanges || []) {
      if (t.userAccount !== wallet) continue;
      const raw = t.rawTokenAmount || {};
      const amt = Number(raw.tokenAmount || 0) / 10 ** (raw.decimals || 0);
      bal[t.mint] = (bal[t.mint] || 0) + amt;
    }
  }
  // fallback when accountData lacks token changes
  if (!Object.keys(bal).length) {
    for (const t of tx.tokenTransfers || []) {
      const amt = +t.tokenAmount || 0;
      if (t.toUserAccount === wallet) bal[t.mint] = (bal[t.mint] || 0) + amt;
      if (t.fromUserAccount === wallet) bal[t.mint] = (bal[t.mint] || 0) - amt;
    }
  }
  if (tx.feePayer === wallet) sol += (tx.fee || 0) / 1e9;
  sol += bal[WSOL] || 0;
  const usdAmt = (bal[USDC] || 0) + (bal[USDT] || 0);
  let best = null;
  for (const [m, d] of Object.entries(bal)) {
    if (QUOTES.has(m) || Math.abs(d) < 1e-9) continue;
    if (!best || Math.abs(d) > Math.abs(best.d)) best = { m, d };
  }
  if (!best) return null;
  let quote, amt;
  if (Math.abs(sol) >= 0.001) { quote = 'SOL'; amt = sol; }
  else if (Math.abs(usdAmt) >= 0.01) { quote = 'USD'; amt = usdAmt; }
  else return null;
  const base = { mint: best.m, quote, time: tx.timestamp || Math.floor(Date.now() / 1000), sig: tx.signature, src: tx.source || '' };
  if (best.d > 0 && amt < 0) return { ...base, side: 'BUY', tokens: best.d, amount: -amt };
  if (best.d < 0 && amt > 0) return { ...base, side: 'SELL', tokens: -best.d, amount: amt };
  return null;
}

// ---------------------------------------------------------------- incoming trades
async function handleHelius(env, txs) {
  const cfg = await getCfg(env);
  const tracked = new Map(cfg.wallets.map(w => [w.addr, w]));
  const st = await getState(env);
  const seen = new Set(st.trades.map(t => t.key));
  let changed = false, cfgChanged = false;

  for (const tx of Array.isArray(txs) ? txs : [txs]) {
    const involved = new Set([tx.feePayer, ...(tx.accountData || []).map(a => a.account),
      ...(tx.accountData || []).flatMap(a => (a.tokenBalanceChanges || []).map(t => t.userAccount))]);
    for (const addr of involved) {
      const w = tracked.get(addr);
      if (!w) continue;
      const t = parseSwap(tx, addr);
      if (!t) continue;
      const key = t.sig + ':' + addr;
      if (seen.has(key)) continue;
      seen.add(key);

      // bot filter
      st.cnt[addr] = (st.cnt[addr] || 0) + 1;
      if (st.cnt[addr] > BOT_TRADES_PER_DAY && w.auto) {
        cfg.wallets = cfg.wallets.filter(x => x.addr !== addr); tracked.delete(addr); cfgChanged = true;
        await send(env, `🤖 Stopped tracking <b>${esc(w.label)}</b>: ${st.cnt[addr]} trades today looks like a bot.`);
        continue;
      }

      const info = await tokenInfo(t.mint);
      const now = await tokenBalance(env, addr, t.mint);
      if (now != null) { t.post = now; t.pre = t.side === 'BUY' ? Math.max(0, now - t.tokens) : now + t.tokens; }
      const rec = { key, wallet: addr, ...t, sym: info?.sym || '', img: info?.img || '', mc: info?.mc || null };
      st.trades.unshift(rec);
      if (st.trades.length > MAX_TRADES) st.trades.length = MAX_TRADES;
      changed = true;

      // position cost for PnL on sells
      st.pos[addr] ||= {};
      const pos = st.pos[addr][t.mint] || { cost: 0, q: t.quote };
      let pnlLine = '';
      if (t.side === 'BUY') { if (pos.q === t.quote) pos.cost += t.amount; st.pos[addr][t.mint] = pos; }
      else {
        const frac = t.pre > 0 ? Math.min(1, t.tokens / t.pre) : 1;
        if (pos.cost > 0 && pos.q === t.quote) {
          const out = pos.cost * frac, pnl = t.amount - out; pos.cost -= out;
          const pct = out > 0 ? (pnl / out) * 100 : 0;
          pnlLine = `📈 Profit on this sell: ${pnl >= 0 ? '+' : ''}${t.quote === 'SOL' ? pnl.toFixed(2) + ' SOL' : signedUsd(pnl)} (${pct >= 0 ? '+' : ''}${pct.toFixed(0)}%)\n`;
        }
        if (t.post != null && t.post <= (t.pre || 0) * 0.01) delete st.pos[addr][t.mint]; else st.pos[addr][t.mint] = pos;
      }

      if (!cfg.paused && !(t.quote === 'SOL' && t.amount < cfg.minSol)) await send(env, alertText(w, rec, info, pnlLine));
      if (t.side === 'BUY') await maybeCluster(env, cfg, st, t.mint, info);
      else await copyFollowSell(env, addr, t).catch(e => send(env, '⚠️ Copy-sell error: ' + esc(e.message)));
    }
  }
  if (cfgChanged) { await syncHook(env, cfg); await putCfg(env, cfg); }
  if (changed) await putState(env, st);
}

function who(w) { return `<b>${esc(w.label)}</b>${w.tw ? ` (@${esc(w.tw)})` : ''}${w.auto ? ' ⭐' : ''}`; }
function alertText(w, t, info, pnlLine) {
  const sym = esc(info?.sym || short(t.mint));
  const amt = t.quote === 'SOL' ? `${t.amount.toFixed(t.amount >= 10 ? 1 : 2)} SOL` : usd(t.amount);
  const lag = Math.max(0, Math.round(Date.now() / 1000 - t.time));
  const links = `🔗 <a href="${info?.url || `https://dexscreener.com/solana/${t.mint}`}">Chart</a> · <a href="https://solscan.io/tx/${t.sig}">Tx</a> · <a href="https://gmgn.ai/sol/address/${t.wallet}">Trader</a>\n<code>${t.mint}</code>`;
  if (t.side === 'BUY') {
    const fresh = t.pre != null ? t.pre <= 0 : true;
    return `🟢 <b>${fresh ? 'NEW BUY' : 'BOUGHT MORE'}</b> $${sym}\n👤 ${who(w)}\n💰 ${amt} → ${fmtNum(t.tokens)} tokens\n${marketLine(info)}\n⏱ ${lag}s ago\n${links}`;
  }
  const full = t.post != null && t.post <= (t.pre || 0) * 0.01;
  const pct = t.pre > 0 ? Math.round(Math.min(1, t.tokens / t.pre) * 100) : null;
  return `🔴 <b>${full ? 'SOLD ALL' : pct != null ? `SOLD ${pct}%` : 'SOLD'}</b> $${sym}\n👤 ${who(w)}\n💰 ${fmtNum(t.tokens)} tokens → ${amt}\n${pnlLine}${marketLine(info)}\n⏱ ${lag}s ago\n${links}`;
}
async function maybeCluster(env, cfg, st, mint, info) {
  const now = Date.now() / 1000;
  const buyers = [...new Set(st.trades.filter(t => t.mint === mint && t.side === 'BUY' && now - t.time < CLUSTER_WINDOW).map(t => t.wallet))];
  if (buyers.length < 2 || (st.clusters[mint] && now - st.clusters[mint] < CLUSTER_WINDOW)) return;
  st.clusters[mint] = now;
  for (const [m, t] of Object.entries(st.clusters)) if (now - t > 86400) delete st.clusters[m];
  const names = buyers.map(a => cfg.wallets.find(w => w.addr === a)).filter(Boolean).map(w => '• ' + who(w)).join('\n');
  if (!cfg.paused) await send(env, `🔥 <b>CLUSTER BUY</b> $${esc(info?.sym || short(mint))}\n${buyers.length} tracked traders bought within 1 hour:\n${names}\n${marketLine(info)}\n🔗 <a href="${info?.url || `https://dexscreener.com/solana/${mint}`}">Chart</a>\n<code>${mint}</code>`);
  await copyBuy(env, mint, info, buyers).catch(e => send(env, '⚠️ Copy-buy error: ' + esc(e.message)));
}

// ---------------------------------------------------------------- famous traders
async function famous(env, days) {
  const d = await stGet(env, `/v2/pnl/leaderboard/kols/period?period=${days}d&sort=realized&direction=desc&limit=50`);
  return d.traders || [];
}
const twOf = tw => String(tw || '').replace(/^https?:\/\/(www\.)?(x|twitter)\.com\//i, '').replace(/^@/, '').split(/[/?]/)[0];

async function autoRefresh(env, announce = true) {
  const cfg = await getCfg(env);
  if (!cfg.autoOn) return { ok: true, note: 'auto off' };
  const top = (await famous(env, 1)).filter(t => (t.period?.realized || 0) > 0).slice(0, cfg.autoN);
  if (!top.length) return { ok: false, error: 'No leaderboard data' };
  const keep = cfg.wallets.filter(w => !w.auto);
  const manual = new Set(keep.map(w => w.addr));
  const prevAuto = new Set(cfg.wallets.filter(w => w.auto).map(w => w.addr));
  const next = top.filter(t => !manual.has(t.wallet)).map(t => ({
    addr: t.wallet, label: t.identity?.name || short(t.wallet), tw: twOf(t.identity?.twitter), auto: true, pnl24: t.period?.realized,
  }));
  const added = next.filter(w => !prevAuto.has(w.addr));
  const removed = cfg.wallets.filter(w => w.auto && !next.find(n => n.addr === w.addr));
  cfg.wallets = [...keep, ...next];
  const r = await syncHook(env, cfg);
  await putCfg(env, cfg);
  if (announce && (added.length || removed.length)) {
    let msg = '⭐ <b>Auto-tracking the top famous traders (last 24h profit)</b>\n';
    if (added.length) msg += '\nNow tracking:\n' + added.map(w => `• ${who(w)} ${signedUsd(w.pnl24)}`).join('\n');
    if (removed.length) msg += '\n\nStopped: ' + removed.map(w => esc(w.label)).join(', ');
    await send(env, msg);
  }
  return r;
}
async function leaderboardText(env, days) {
  const list = (await famous(env, days)).slice(0, 10);
  if (!list.length) return 'No leaderboard data right now.';
  return `🏆 <b>Top famous traders · ${days === 1 ? 'last 24h' : `last ${days} days`}</b>\n\n` + list.map((t, i) => {
    const tw = twOf(t.identity?.twitter);
    return `${i + 1}. <b>${esc(t.identity?.name || short(t.wallet))}</b>${tw ? ` @${esc(tw)}` : ''} · ${signedUsd(t.period?.realized)}\n    <code>${t.wallet}</code>`;
  }).join('\n') + '\n\nTrack one with /add followed by the address.';
}

// ---------------------------------------------------------------- Telegram commands
const HELP = `<b>SolRadar</b>
/top — famous traders, last 24h profit
/week — famous traders, last 7 days
/list — wallets being tracked
/add &lt;wallet&gt; [name] — track a wallet
/remove &lt;wallet or name&gt; — stop tracking
/trader &lt;wallet or name&gt; — profit record
/auto on · /auto off · /auto 15 — auto-track top famous traders
/min 0.5 — ignore trades under 0.5 SOL
/pause · /resume — mute alerts
/status — health check

<b>Auto copy-trading</b>
/copy — wallet, balance, open trades, profit
/copy on · /copy off — start or stop auto-buying
/copy size 10 — % of balance per trade
/sellall — sell every open copy trade now`;

async function handleCommand(env, text, chat) {
  const [raw, ...args] = text.trim().split(/\s+/);
  const cmd = raw.toLowerCase().split('@')[0];
  const cfg = await getCfg(env);
  const find = q => cfg.wallets.find(w => w.addr === q || w.label.toLowerCase() === q.toLowerCase());
  const reply = t => send(env, t, chat);
  switch (cmd) {
    case '/start': case '/help': return reply(HELP);
    case '/top': case '/week': return reply(await leaderboardText(env, cmd === '/top' ? 1 : 7).catch(e => '⚠️ ' + e.message));
    case '/list': {
      if (!cfg.wallets.length) return reply('No wallets yet. Use /add or /auto on.');
      return reply(`<b>Tracking ${cfg.wallets.length} wallets</b> (⭐ = auto famous trader)\n\n` +
        cfg.wallets.map(w => `${w.auto ? '⭐' : '📌'} ${esc(w.label)}${w.tw ? ' @' + esc(w.tw) : ''}\n    <code>${w.addr}</code>`).join('\n'));
    }
    case '/add': {
      const a = args[0];
      if (!a || !ADDR_RE.test(a)) return reply('Usage: /add &lt;solana wallet&gt; [name]');
      const ex = cfg.wallets.find(w => w.addr === a);
      if (ex) { ex.auto = false; if (args[1]) ex.label = args.slice(1).join(' '); }
      else cfg.wallets.push({ addr: a, label: args.slice(1).join(' ') || short(a), auto: false });
      const r = await syncHook(env, cfg); await putCfg(env, cfg);
      return reply(r.ok ? `✅ Tracking <b>${esc(args.slice(1).join(' ') || short(a))}</b>. Alerts arrive seconds after each trade.` : '⚠️ Saved, but Helius update failed: ' + esc(r.error));
    }
    case '/remove': {
      const w = find(args.join(' '));
      if (!w) return reply('Not found. Use /list.');
      cfg.wallets = cfg.wallets.filter(x => x.addr !== w.addr);
      await syncHook(env, cfg); await putCfg(env, cfg);
      return reply(`🗑 Stopped tracking ${esc(w.label)}${w.auto ? '. It may come back at the next auto refresh; send /auto off to stop that.' : ''}`);
    }
    case '/trader': {
      const q = args.join(' ');
      const addr = find(q)?.addr || (ADDR_RE.test(q) ? q : null);
      if (!addr) return reply('Usage: /trader &lt;wallet or name&gt;');
      try {
        const d = await stGet(env, `/v2/pnl/wallets/${addr}`);
        const p = d.pnl || {};
        return reply(`👤 <b>${esc(d.identity?.name || short(addr))}</b>\nTotal profit: ${signedUsd(p.total)}\nRealized: ${signedUsd(p.realized)} · Unrealized: ${signedUsd(p.unrealized)}\n` +
          `Win rate: ${d.winRate != null ? Math.round(d.winRate <= 1 ? d.winRate * 100 : d.winRate) + '%' : '?'} · Trades: ${d.counts?.trades ?? '?'}\n🔗 <a href="https://gmgn.ai/sol/address/${addr}">GMGN</a>`);
      } catch (e) { return reply('⚠️ ' + esc(e.message)); }
    }
    case '/auto': {
      const a = (args[0] || '').toLowerCase();
      if (a === 'off') { cfg.autoOn = false; cfg.wallets = cfg.wallets.filter(w => !w.auto); await syncHook(env, cfg); await putCfg(env, cfg); return reply('⭐ Auto-tracking off. Famous traders removed; your own wallets stay.'); }
      if (a === 'on' || /^\d+$/.test(a)) {
        cfg.autoOn = true; if (/^\d+$/.test(a)) cfg.autoN = Math.max(1, Math.min(40, +a));
        await putCfg(env, cfg); await reply(`⭐ Auto-tracking the top ${cfg.autoN} famous traders. Updating now…`);
        const r = await autoRefresh(env, true); return r.ok ? null : reply('⚠️ ' + esc(r.error || 'failed'));
      }
      return reply(`Auto-tracking is <b>${cfg.autoOn ? 'on' : 'off'}</b> (top ${cfg.autoN}). Use /auto on, /auto off or /auto 15.`);
    }
    case '/min': {
      const v = parseFloat(args[0]);
      if (!(v >= 0)) return reply(`Current minimum: ${cfg.minSol} SOL. Usage: /min 0.5`);
      cfg.minSol = v; await putCfg(env, cfg); return reply(`✅ Ignoring trades under ${v} SOL.`);
    }
    case '/pause': cfg.paused = true; await putCfg(env, cfg); return reply('⏸ Alerts paused. Send /resume to turn them back on.');
    case '/resume': cfg.paused = false; await putCfg(env, cfg); return reply('▶️ Alerts on.');
    case '/status': {
      const st = await getState(env);
      let hook = 'unknown';
      if (cfg.hookId) {
        const h = await fetch(`${HELIUS_API}/${cfg.hookId}?api-key=${encodeURIComponent(env.HELIUS_API_KEY)}`).then(r => r.json()).catch(() => null);
        hook = h?.webhookID ? (h.active === false ? '🔴 disabled by Helius, send /fix' : '🟢 active') : '🔴 missing, send /fix';
      } else hook = cfg.wallets.length ? '🔴 not set, send /fix' : 'waiting for first wallet';
      const last = st.trades[0];
      return reply(`<b>Status</b>\nHelius feed: ${hook}\nWallets: ${cfg.wallets.length} (${cfg.wallets.filter(w => w.auto).length} auto)\nAlerts: ${cfg.paused ? '⏸ paused' : '▶️ on'} · min ${cfg.minSol} SOL\n` +
        `Last trade seen: ${last ? fmtAge(Date.now() - last.time * 1000) + ' ago' : 'none yet'}\nStorage writes today: ${st.writes || 0}/${MAX_KV_WRITES}`);
    }
    case '/copy': return reply(await copyCommand(env, args));
    case '/sellall': {
      const c = await getCopy(env);
      const mints = Object.keys(c.pos);
      if (!mints.length) return reply('No open copy trades.');
      await reply(`Selling ${mints.length} position(s)…`);
      for (const m of mints) await copySell(env, m, 1, 'you sent /sellall').catch(e => reply('⚠️ ' + esc(e.message)));
      return null;
    }
    case '/fix': { const r = await syncHook(env, cfg); await putCfg(env, cfg); return reply(r.ok ? '🔧 Helius feed reconnected.' : '⚠️ ' + esc(r.error)); }
    default: return reply('Unknown command. Send /help');
  }
}

// ---------------------------------------------------------------- setup
async function setup(env, origin, appUrl) {
  const cfg = await getCfg(env);
  cfg.url = origin;
  if (appUrl) cfg.appUrl = appUrl;
  const secret = await hookSecret(env);
  const out = {};
  const w = await tg(env, 'setWebhook', { url: origin + '/hook/telegram', secret_token: secret, allowed_updates: ['message'], drop_pending_updates: true });
  out.telegram = w.ok ? 'ok' : w.description;
  await tg(env, 'setMyCommands', { commands: [
    ['top', 'Famous traders, last 24h profit'], ['week', 'Famous traders, last 7 days'], ['list', 'Wallets being tracked'],
    ['add', 'Track a wallet'], ['remove', 'Stop tracking a wallet'], ['trader', 'Profit record of a wallet'],
    ['auto', 'Auto-track top famous traders'], ['min', 'Minimum trade size'], ['pause', 'Mute alerts'],
    ['resume', 'Unmute alerts'], ['status', 'Health check'], ['help', 'All commands'],
  ].map(([command, description]) => ({ command, description })) });
  await putCfg(env, cfg);
  let auto = { ok: true };
  if (cfg.autoOn && !cfg.wallets.some(x => x.auto)) auto = await autoRefresh(env, true).catch(e => ({ ok: false, error: e.message }));
  const c2 = await getCfg(env);
  const hook = await syncHook(env, c2); await putCfg(env, c2);
  out.helius = hook.ok ? 'ok' : hook.error;
  out.famous = auto.ok ? 'ok' : auto.error;
  const code = btoa(JSON.stringify({ u: origin, p: env.APP_PASSCODE })).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  const app = c2.appUrl || '';
  const sent = await send(env, `🚀 <b>SolRadar is live</b>\n\nTracking ${c2.wallets.length} wallets. Alerts arrive seconds after each trade, 24/7.\n\n` +
    `<b>Connect the iPhone app:</b>\n1. Open ${app ? `<a href="${esc(app)}">your app</a>` : 'your app'} → Settings\n2. Tap and hold the code below to copy it, then paste it in "Connection code"\n\n<code>${code}</code>\n\n` +
    (out.famous === 'ok' ? '' : `⚠️ Famous traders: ${esc(out.famous)}\n`) + (out.helius === 'ok' ? '' : `⚠️ Helius: ${esc(out.helius)}\n`) + 'Send /help for commands.');
  out.message = sent.ok ? 'ok' : sent.description;
  out.ok = out.telegram === 'ok' && out.message === 'ok';
  return out;
}

// ---------------------------------------------------------------- app API
async function api(env, req, url) {
  if (!safeEq(req.headers.get('x-pass'), env.APP_PASSCODE)) return json({ error: 'Wrong passcode' }, 401);
  const p = url.pathname;
  if (p === '/api/state') {
    const [cfg, st] = await Promise.all([getCfg(env), getState(env)]);
    return json({ wallets: cfg.wallets, trades: st.trades, paused: cfg.paused, autoOn: cfg.autoOn, autoN: cfg.autoN, minSol: cfg.minSol });
  }
  if (p === '/api/wallets' && req.method === 'POST') {
    const b = await req.json().catch(() => ({}));
    const cfg = await getCfg(env);
    const w = cfg.wallets.find(x => x.addr === b.addr);
    if (b.op === 'add') {
      if (!ADDR_RE.test(b.addr || '')) return json({ error: 'Not a Solana address' }, 400);
      if (w) { w.auto = false; if (b.label) w.label = b.label; if (b.tw) w.tw = b.tw; }
      else cfg.wallets.push({ addr: b.addr, label: b.label || short(b.addr), tw: b.tw || '', auto: false });
    } else if (b.op === 'remove') cfg.wallets = cfg.wallets.filter(x => x.addr !== b.addr);
    else if (b.op === 'rename' && w) w.label = b.label || w.label;
    else return json({ error: 'Unknown op' }, 400);
    const r = b.op === 'rename' ? { ok: true } : await syncHook(env, cfg);
    await putCfg(env, cfg);
    return json({ ok: r.ok, error: r.error, wallets: cfg.wallets });
  }
  if (p === '/api/settings' && req.method === 'POST') {
    const b = await req.json().catch(() => ({}));
    const cfg = await getCfg(env);
    if (typeof b.paused === 'boolean') cfg.paused = b.paused;
    if (b.minSol >= 0) cfg.minSol = +b.minSol;
    if (b.autoN >= 1) cfg.autoN = Math.min(40, Math.round(b.autoN));
    let r = { ok: true };
    if (typeof b.autoOn === 'boolean' && b.autoOn !== cfg.autoOn) {
      cfg.autoOn = b.autoOn;
      if (!b.autoOn) { cfg.wallets = cfg.wallets.filter(w => !w.auto); await putCfg(env, cfg); r = await syncHook(env, cfg); }
    }
    await putCfg(env, cfg);
    if (cfg.autoOn && (b.autoOn === true || b.autoN)) r = await autoRefresh(env, true).catch(e => ({ ok: false, error: e.message }));
    return json({ ok: r.ok, error: r.error });
  }
  if (p.startsWith('/api/st/')) {
    const path = p.slice('/api/st'.length) + url.search;
    if (!path.startsWith('/v2/')) return json({ error: 'Not allowed' }, 400);
    const cache = caches.default, ck = new Request('https://cache.solradar/' + path);
    const hit = await cache.match(ck);
    if (hit) return new Response(hit.body, { headers: { 'content-type': 'application/json', 'x-cache': 'hit', ...CORS } });
    const r = await fetch(ST + path, { headers: { 'x-api-key': env.SOLANA_TRACKER_KEY } });
    const body = await r.text();
    if (r.ok) await cache.put(ck, new Response(body, { headers: { 'content-type': 'application/json', 'cache-control': `max-age=${path.includes('leaderboard') ? 300 : 120}` } }));
    return new Response(body, { status: r.status, headers: { 'content-type': 'application/json', ...CORS } });
  }
  return json({ error: 'Not found' }, 404);
}


// ================================================================ AUTO COPY-TRADING
// Buys when 2+ tracked traders buy the same coin within 1 hour (cluster), using a % of the
// bot wallet's SOL. Sells when those traders sell, at +TP% (half), at -SL% (all) or after max hours.
// Needs secrets TRADER_PRIVATE_KEY (a separate small wallet) and JUPITER_API_KEY. Off until /copy on.
const JUP = 'https://api.jup.ag/swap/v2';
const COPY_DEFAULT = { on: false, pct: 10, maxOpen: 5, minLiq: 20000, tp: 100, sl: 40, maxHours: 24, maxBuysPerDay: 10,
  pos: {}, closed: [], realized: 0, day: '', buysToday: 0 };
const RESERVE_SOL = 0.01; // left for network fees and token-account rent

async function getCopy(env) {
  const c = { ...COPY_DEFAULT, ...((await env.KV.get('copy', 'json')) || {}) };
  c.pos ||= {}; c.closed ||= [];
  if (c.day !== today()) { c.day = today(); c.buysToday = 0; }
  return c;
}
const putCopy = (env, c) => env.KV.put('copy', JSON.stringify(c));

// ---- base58
const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function b58decode(str) {
  const bytes = [0];
  for (const ch of str) {
    const v = B58.indexOf(ch);
    if (v < 0) throw new Error('Private key has an invalid character');
    let carry = v;
    for (let i = 0; i < bytes.length; i++) { carry += bytes[i] * 58; bytes[i] = carry & 0xff; carry >>= 8; }
    while (carry) { bytes.push(carry & 0xff); carry >>= 8; }
  }
  for (const ch of str) { if (ch === '1') bytes.push(0); else break; }
  return new Uint8Array(bytes.reverse());
}
function b58encode(buf) {
  const digits = [0];
  for (const byte of buf) {
    let carry = byte;
    for (let i = 0; i < digits.length; i++) { carry += digits[i] << 8; digits[i] = carry % 58; carry = (carry / 58) | 0; }
    while (carry) { digits.push(carry % 58); carry = (carry / 58) | 0; }
  }
  let out = '';
  for (const b of buf) { if (b === 0) out += '1'; else break; }
  for (let i = digits.length - 1; i >= 0; i--) out += B58[digits[i]];
  return out;
}
const b64d = s => Uint8Array.from(atob(s), c => c.charCodeAt(0));
function b64e(u8) { let s = ''; for (let i = 0; i < u8.length; i += 0x8000) s += String.fromCharCode(...u8.subarray(i, i + 0x8000)); return btoa(s); }

// ---- wallet
let KP = null;
async function keypair(env) {
  if (KP) return KP;
  const raw = String(env.TRADER_PRIVATE_KEY || '');
  if (!raw) throw new Error('No trading wallet yet. Add the TRADER_PRIVATE_KEY secret in GitHub and re-run the workflow.');
  const sk = raw.startsWith('[') ? Uint8Array.from(JSON.parse(raw)) : b58decode(raw);
  if (sk.length !== 64) throw new Error(`Private key should be 64 bytes, got ${sk.length}. Copy it again from Phantom.`);
  const der = new Uint8Array([0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20, ...sk.slice(0, 32)]);
  let key;
  try { key = await crypto.subtle.importKey('pkcs8', der, { name: 'Ed25519' }, false, ['sign']); }
  catch { key = await crypto.subtle.importKey('pkcs8', der, { name: 'NODE-ED25519', namedCurve: 'NODE-ED25519' }, false, ['sign']); }
  KP = { key, pub: sk.slice(32), addr: b58encode(sk.slice(32)) };
  return KP;
}
async function signTx(b64, kp) {
  const tx = b64d(b64);
  const cu16 = (buf, o) => { let v = 0, s = 0, b; do { b = buf[o++]; v |= (b & 0x7f) << s; s += 7; } while (b & 0x80); return [v, o]; };
  const [nSig, sigStart] = cu16(tx, 0);
  const msgStart = sigStart + 64 * nSig;
  const msg = tx.slice(msgStart);
  let p = (msg[0] & 0x80) ? 1 : 0;
  const nReq = msg[p]; p += 3;
  const [nKeys, keysStart] = cu16(msg, p);
  let idx = -1;
  for (let i = 0; i < Math.min(nKeys, nReq); i++) {
    const k = msg.subarray(keysStart + 32 * i, keysStart + 32 * i + 32);
    if (k.every((b, j) => b === kp.pub[j])) { idx = i; break; }
  }
  if (idx < 0) throw new Error('Swap transaction is not for this wallet');
  let sig;
  try { sig = new Uint8Array(await crypto.subtle.sign({ name: 'Ed25519' }, kp.key, msg)); }
  catch { sig = new Uint8Array(await crypto.subtle.sign({ name: 'NODE-ED25519' }, kp.key, msg)); }
  tx.set(sig, sigStart + 64 * idx);
  return b64e(tx);
}

// ---- chain + Jupiter
async function rpc(env, method, params) {
  const r = await fetch(`https://mainnet.helius-rpc.com/?api-key=${env.HELIUS_API_KEY}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  const d = await r.json();
  if (d.error) throw new Error(d.error.message);
  return d.result;
}
const solBalance = async (env, addr) => ((await rpc(env, 'getBalance', [addr, { commitment: 'confirmed' }]))?.value || 0);
async function rawTokenBalance(env, owner, mint) {
  const r = await rpc(env, 'getTokenAccountsByOwner', [owner, { mint }, { encoding: 'jsonParsed', commitment: 'confirmed' }]);
  return (r?.value || []).reduce((s, a) => s + BigInt(a.account.data.parsed.info.tokenAmount.amount || '0'), 0n);
}
async function jupOrder(env, inputMint, outputMint, amount, taker) {
  const q = new URLSearchParams({ inputMint, outputMint, amount: String(amount) });
  if (taker) q.set('taker', taker);
  const r = await fetch(`${JUP}/order?${q}`, { headers: { 'x-api-key': env.JUPITER_API_KEY || '' } });
  const d = await r.json().catch(() => ({}));
  if (!r.ok || d.errorCode || d.error) throw new Error(`Jupiter: ${d.errorMessage || d.error || r.status}`);
  return d;
}
async function swap(env, inputMint, outputMint, amount) {
  const kp = await keypair(env);
  const o = await jupOrder(env, inputMint, outputMint, amount, kp.addr);
  if (!o.transaction) throw new Error('Jupiter returned no transaction' + (o.errorMessage ? ': ' + o.errorMessage : ''));
  const signed = await signTx(o.transaction, kp);
  const r = await fetch(`${JUP}/execute`, {
    method: 'POST', headers: { 'content-type': 'application/json', 'x-api-key': env.JUPITER_API_KEY || '' },
    body: JSON.stringify({ signedTransaction: signed, requestId: o.requestId }),
  });
  const d = await r.json().catch(() => ({}));
  if (d.status !== 'Success') throw new Error(`Swap failed: ${d.error || d.code || r.status}` + (d.signature ? ` (tx ${d.signature})` : ''));
  return {
    sig: d.signature,
    inAmt: BigInt(d.totalInputAmount || d.inputAmountResult || o.inAmount || amount),
    outAmt: BigInt(d.totalOutputAmount || d.outputAmountResult || o.outAmount || 0),
  };
}
const lamportsToSol = l => Number(l) / 1e9;

// ---- buy on cluster
async function copyBuy(env, mint, info, buyers) {
  const c = await getCopy(env);
  if (!c.on || c.pos[mint]) return;
  const sym = esc(info?.sym || short(mint));
  const skip = why => send(env, `⏭ <b>Copy skipped</b> $${sym}: ${why}`);
  if (!info) return skip('not on DexScreener yet, too new to trade safely');
  if ((info.liq || 0) < c.minLiq) return skip(`liquidity ${usd(info.liq)} is under ${usd(c.minLiq)}`);
  if (Object.keys(c.pos).length >= c.maxOpen) return skip(`already ${c.maxOpen} open trades`);
  if (c.buysToday >= c.maxBuysPerDay) return skip(`daily limit of ${c.maxBuysPerDay} buys reached`);
  const kp = await keypair(env);
  const bal = await solBalance(env, kp.addr);
  const spend = Math.floor((bal - RESERVE_SOL * 1e9) * c.pct / 100);
  if (spend < 0.005 * 1e9) return skip(`wallet balance too low (${lamportsToSol(bal).toFixed(3)} SOL). Send SOL to <code>${kp.addr}</code>`);
  c.pos[mint] = { sym: info.sym, pending: true, at: Date.now() }; // lock against double buys
  c.buysToday++;
  await putCopy(env, c);
  try {
    const r = await swap(env, WSOL, mint, spend);
    const c2 = await getCopy(env);
    c2.pos[mint] = { sym: info.sym, sol: lamportsToSol(r.inAmt), cost0: lamportsToSol(r.inAmt), raw: r.outAmt.toString(), at: Date.now(), buyers, tpDone: false, peak: 0 };
    await putCopy(env, c2);
    await send(env, `🤖🟢 <b>COPY BUY</b> $${sym}\n💰 ${lamportsToSol(r.inAmt).toFixed(4)} SOL (${c.pct}% of balance)\n${marketLine(info)}\n🎯 Sell: when traders sell · +${c.tp}% half · −${c.sl}% all · ${c.maxHours}h\n🔗 <a href="https://solscan.io/tx/${r.sig}">Tx</a> · <a href="${info.url}">Chart</a>`);
  } catch (e) {
    const c2 = await getCopy(env); delete c2.pos[mint]; await putCopy(env, c2);
    await send(env, `⚠️ <b>Copy buy failed</b> $${sym}: ${esc(e.message)}`);
  }
}

// ---- sell (fraction 0..1)
async function copySell(env, mint, fraction, reason) {
  const c = await getCopy(env);
  const p = c.pos[mint];
  if (!p || p.pending) return;
  const kp = await keypair(env);
  const have = await rawTokenBalance(env, kp.addr, mint);
  if (have <= 0n) { delete c.pos[mint]; await putCopy(env, c); return; }
  const amount = fraction >= 1 ? have : (have * BigInt(Math.round(fraction * 1000))) / 1000n;
  if (amount <= 0n) return;
  const r = await swap(env, mint, WSOL, amount);
  const c2 = await getCopy(env);
  const q = c2.pos[mint] || p;
  const costPart = (q.sol || 0) * (fraction >= 1 ? 1 : fraction);
  const got = lamportsToSol(r.outAmt);
  const pnl = got - costPart;
  c2.realized = (c2.realized || 0) + pnl;
  if (fraction >= 1) {
    delete c2.pos[mint];
    c2.closed.unshift({ sym: q.sym, mint, sol: q.cost0 || q.sol, back: (q.back || 0) + got, at: Date.now() });
    c2.closed.length = Math.min(c2.closed.length, 30);
  } else { q.sol -= costPart; q.back = (q.back || 0) + got; q.tpDone = true; q.raw = (have - amount).toString(); c2.pos[mint] = q; }
  await putCopy(env, c2);
  await send(env, `🤖🔴 <b>COPY SELL ${fraction >= 1 ? 'ALL' : Math.round(fraction * 100) + '%'}</b> $${esc(q.sym || short(mint))}\nReason: ${esc(reason)}\n💰 Got ${got.toFixed(4)} SOL · ${pnl >= 0 ? '+' : ''}${pnl.toFixed(4)} SOL (${costPart > 0 ? ((pnl / costPart) * 100).toFixed(0) : '?'}%)\n📊 Total copy profit: ${c2.realized >= 0 ? '+' : ''}${c2.realized.toFixed(4)} SOL\n🔗 <a href="https://solscan.io/tx/${r.sig}">Tx</a>`);
}

// ---- follow the traders out
async function copyFollowSell(env, trader, t) {
  const c = await getCopy(env);
  const p = c.pos[t.mint];
  if (!p || p.pending || !(p.buyers || []).includes(trader)) return;
  const frac = t.pre > 0 ? t.tokens / t.pre : 1;
  if (frac < 0.5) return; // ignore small trims
  const cfg = await getCfg(env);
  const name = cfg.wallets.find(w => w.addr === trader)?.label || short(trader);
  await copySell(env, t.mint, 1, `${name} sold ${Math.round(Math.min(1, frac) * 100)}% of their position`);
}

// ---- every minute: take profit / stop loss / time limit
async function copyMonitor(env) {
  const c = await getCopy(env);
  const mints = Object.keys(c.pos);
  if (!mints.length) return;
  for (const m of mints) {
    const p = c.pos[m];
    if (p.pending) { if (Date.now() - p.at > 180000) { const c2 = await getCopy(env); delete c2.pos[m]; await putCopy(env, c2); } continue; }
    try {
      if (Date.now() - p.at > c.maxHours * 3600000) { await copySell(env, m, 1, `${c.maxHours}h time limit`); continue; }
      const raw = BigInt(p.raw || '0');
      if (raw <= 0n || !p.sol) continue;
      const q = await jupOrder(env, m, WSOL, raw);
      const value = lamportsToSol(q.outAmount || 0); // what the tokens we still hold would sell for now
      const pct = (value / p.sol - 1) * 100;
      if (pct <= -c.sl) await copySell(env, m, 1, `stop loss (${pct.toFixed(0)}%)`);
      else if (!p.tpDone && pct >= c.tp) await copySell(env, m, 0.5, `take profit (+${pct.toFixed(0)}%)`);
    } catch (e) { console.log('monitor', m, e.message); }
  }
}

// ---- /copy command
async function copyCommand(env, args) {
  const c = await getCopy(env);
  const a = (args[0] || '').toLowerCase();
  if (a === 'on' || a === 'off') {
    if (a === 'on') {
      const kp = await keypair(env).catch(e => null);
      if (!kp) return '⚠️ No trading wallet yet. Add the TRADER_PRIVATE_KEY secret in GitHub, then re-run the workflow.';
      if (!env.JUPITER_API_KEY) return '⚠️ Add the JUPITER_API_KEY secret in GitHub, then re-run the workflow.';
    }
    c.on = a === 'on'; await putCopy(env, c);
    return c.on
      ? `🤖 <b>Copy-trading ON</b>\nBuys ${c.pct}% of the wallet balance when 2+ of your traders buy the same coin within 1 hour.\nSells when they sell, +${c.tp}% (half), −${c.sl}% (all) or after ${c.maxHours}h.\nSend /copy off to stop.`
      : '⏸ Copy-trading OFF. Open trades are still managed (TP/SL/time). Send /sellall to close them now.';
  }
  if (a === 'size') {
    const v = parseFloat(args[1]);
    if (!(v >= 1 && v <= 50)) return 'Usage: /copy size 10  (1–50% of balance per trade)';
    c.pct = v; await putCopy(env, c); return `✅ Each copy trade now uses ${v}% of the wallet balance.`;
  }
  // status
  let wallet = 'not set (add TRADER_PRIVATE_KEY)', bal = '';
  try { const kp = await keypair(env); wallet = `<code>${kp.addr}</code>`; bal = `${lamportsToSol(await solBalance(env, kp.addr)).toFixed(4)} SOL`; } catch {}
  const open = Object.entries(c.pos).map(([m, p]) => `• $${esc(p.sym || short(m))}: ${p.pending ? 'buying…' : (p.sol || 0).toFixed(4) + ' SOL in, ' + fmtAge(Date.now() - p.at) + ' ago'}`).join('\n') || 'none';
  const last = c.closed.slice(0, 5).map(x => `• $${esc(x.sym)}: ${(x.back - x.sol) >= 0 ? '+' : ''}${(x.back - x.sol).toFixed(4)} SOL`).join('\n') || 'none yet';
  return `🤖 <b>Copy-trading ${c.on ? 'ON' : 'OFF'}</b>\nWallet: ${wallet}\nBalance: ${bal || '?'}\nSize: ${c.pct}% per trade · max ${c.maxOpen} open · ${c.buysToday}/${c.maxBuysPerDay} buys today\n\n<b>Open</b>\n${open}\n\n<b>Last closed</b>\n${last}\n\nTotal copy profit: ${(c.realized || 0) >= 0 ? '+' : ''}${(c.realized || 0).toFixed(4)} SOL`;
}

// ---------------------------------------------------------------- entry
// Remove stray spaces / new lines that can sneak in when secrets are pasted on a phone.
function cleanEnv(env) {
  const o = { ...env };
  for (const k of ['HELIUS_API_KEY', 'SOLANA_TRACKER_KEY', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'APP_PASSCODE', 'TRADER_PRIVATE_KEY', 'JUPITER_API_KEY'])
    if (typeof o[k] === 'string') o[k] = o[k].replace(/\s+/g, '');
  if (o.TELEGRAM_BOT_TOKEN) o.TELEGRAM_BOT_TOKEN = o.TELEGRAM_BOT_TOKEN.replace(/^bot(?=\d)/i, '');
  return o;
}

export default {
  async fetch(req, env, ctx) {
    env = cleanEnv(env);
    const url = new URL(req.url);
    if (req.method === 'OPTIONS') return new Response(null, { headers: CORS });
    try {
      if (url.pathname === '/hook/helius' && req.method === 'POST') {
        if (!safeEq(req.headers.get('authorization'), await hookSecret(env))) return new Response('no', { status: 401 });
        const body = await req.json();
        ctx.waitUntil(handleHelius(env, body).catch(e => console.log('helius', e.stack || e)));
        return new Response('ok');
      }
      if (url.pathname === '/hook/telegram' && req.method === 'POST') {
        if (!safeEq(req.headers.get('x-telegram-bot-api-secret-token'), await hookSecret(env))) return new Response('no', { status: 401 });
        const u = await req.json();
        const m = u.message;
        if (m?.text && String(m.chat.id) === String(env.TELEGRAM_CHAT_ID) && m.text.startsWith('/'))
          ctx.waitUntil(handleCommand(env, m.text, m.chat.id).catch(e => send(env, '⚠️ ' + esc(e.message))));
        else if (m?.chat && String(m.chat.id) !== String(env.TELEGRAM_CHAT_ID))
          ctx.waitUntil(send(env, 'This is a private bot.', m.chat.id));
        return new Response('ok');
      }
      if (url.pathname === '/setup') {
        if (!safeEq(req.headers.get('x-pass') || url.searchParams.get('p'), env.APP_PASSCODE)) return json({ ok: false, error: 'Wrong passcode' }, 401);
        return json(await setup(env, url.origin, url.searchParams.get('app') || ''));
      }
      if (url.pathname.startsWith('/api/')) return api(env, req, url);
      return new Response('SolRadar server is running.', { headers: CORS });
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  },
  async scheduled(event, env, ctx) {
    env = cleanEnv(env);
    if (event.cron === '* * * * *') { ctx.waitUntil(copyMonitor(env).catch(e => console.log('monitor', e.message))); return; }
    ctx.waitUntil((async () => {
      await autoRefresh(env, true).catch(e => console.log('auto', e.message));
      if (new Date(event.scheduledTime).getUTCHours() === 4) {
        const t = await leaderboardText(env, 1).catch(() => null);
        if (t) await send(env, '☀️ Good morning\n\n' + t);
      }
    })());
  },
};

