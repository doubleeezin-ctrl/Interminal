import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import { insertTransactionBatch, getExistingSignatures, testConnection, getBySignature, getTransactions, supabase } from './kv-store-db.js';

const app = express();
const PORT = process.env.PORT || 3000;
const HELIUS_KEY = process.env.HELIUS_KEY;
const JUP_BASE_URL = process.env.JUP_BASE_URL || 'https://api.jup.ag';
const JUP_KEY = process.env.JUP_KEY;
const JUP_REFRESH_ENABLED = (process.env.JUP_REFRESH_ENABLED || 'true').toLowerCase() !== 'false';
const JUP_REFRESH_BATCH_SIZE = parseInt(process.env.JUP_REFRESH_BATCH_SIZE || '50', 10);
const JUP_REFRESH_RPS = parseInt(process.env.JUP_REFRESH_RPS || '3', 10); // for search batches
const JUP_HOLDINGS_RPS = parseInt(process.env.JUP_HOLDINGS_RPS || '3', 10); // per-wallet holdings
const JUP_HOLDINGS_PATH = process.env.JUP_HOLDINGS_PATH || '/ultra/v1/holdings';
const LOG_FULL_MINTS = (process.env.LOG_FULL_MINTS || 'false').toLowerCase() === 'true';
// Detailed table logs (enable/disable via env)
const LOG_TABLE_FULL_MINTS = (process.env.LOG_TABLE_FULL_MINTS || 'true').toLowerCase() !== 'false';
const LOG_TABLE_FULL_WALLETS = (process.env.LOG_TABLE_FULL_WALLETS || 'true').toLowerCase() !== 'false';
// Refresh summary config
const LOG_REFRESH_SUMMARY_INTERVAL_MS = parseInt(process.env.LOG_REFRESH_SUMMARY_INTERVAL_MS || '10000', 10);
const LOG_HOLDINGS_TICK = (process.env.LOG_HOLDINGS_TICK || 'false').toLowerCase() === 'true';
// Compact full-mint table to fit terminal width
const FULLMINT_TABLE_COMPACT = (process.env.FULLMINT_TABLE_COMPACT || 'true').toLowerCase() !== 'false';
// Only refresh mints whose total (sum of last_amount across wallets) meets this threshold
// Atualiza todos por padrão (pode ajustar via env)
const MINT_MIN_TOTAL_FOR_REFRESH = parseFloat(process.env.MINT_MIN_TOTAL_FOR_REFRESH || '0');
// Cleanup: if a mint stays under the threshold for this long, remove it from cache
const MINT_CLEANUP_UNDER_TOTAL_MS = parseInt(process.env.MINT_CLEANUP_UNDER_TOTAL_MS || '600000', 10); // 10 minutes
const MINT_CLEANUP_INTERVAL_MS = parseInt(process.env.MINT_CLEANUP_INTERVAL_MS || '30000', 10); // run every 30s

// Helius settings for fallback holdings by mint
const HELIUS_RPC_URL = process.env.HELIUS_RPC_URL || `https://mainnet.helius-rpc.com/?api-key=${HELIUS_KEY}`;
const HELIUS_REFRESH_RPS = parseInt(process.env.HELIUS_REFRESH_RPS || '8', 10);
const HELIUS_REFRESH_INTERVAL_MS = parseInt(process.env.HELIUS_REFRESH_INTERVAL_MS || '15000', 10); // 15s por padrão

// Global buffer for batching
let pendingSignatures = [];
let pendingSource = null;
// Optional funding metadata for pending signatures: Map<signature, {fundOrigin, fundAgeLiteral}>
let pendingFunding = new Map();

// Backoff windows per provider to avoid hitting rate limits (HTTP 429)
let jupiterBackoffUntil = 0; // epoch ms
let heliusBackoffUntil = 0; // epoch ms

// ==================== SSE STATE ====================
// Connected SSE clients
const sseClients = new Set();
// Circular buffer for last N events (for Last-Event-ID replay)
const EVENT_BUFFER_SIZE = 1000;
const eventBuffer = [];
// Middlewares
app.use(cors());
app.use(express.json());

// ==================== LOG UTILS ====================
const LOG_DIR = path.join(process.cwd(), 'logs');
try { fs.mkdirSync(LOG_DIR, { recursive: true }); } catch {}
const API_LOG_DIR = path.join(LOG_DIR, 'api');
try { fs.mkdirSync(API_LOG_DIR, { recursive: true }); } catch {}
const LOG_API_RESPONSES = (process.env.LOG_API_RESPONSES || 'true').toLowerCase() !== 'false';

function writeApiLogOnce(type, key, payload) {
  if (!LOG_API_RESPONSES) return;
  try {
    const safe = String(key || 'unknown').replace(/[^a-zA-Z0-9_-]+/g, '_').slice(0, 200);
    const file = path.join(API_LOG_DIR, `${type}_${safe}.txt`);
    if (!fs.existsSync(file)) {
      fs.writeFileSync(file, typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2));
    }
  } catch {}
}

// ==================== HTTP FETCH WITH RETRY ====================
function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }

async function fetchWithRetry(url, options = {}, retries = 1) {
  let attempt = 0;
  let lastErr;
  while (attempt <= retries) {
    try {
      const res = await fetch(url, options);
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        const err = new Error(`HTTP ${res.status} for ${url}: ${text.slice(0,200)}`);
        // Backoff on 429/5xx
        if ((res.status === 429 || res.status >= 500) && attempt < retries) {
          await sleep(1000 * Math.pow(2, attempt));
          attempt++;
          continue;
        }
        throw err;
      }
      const ct = (res.headers.get('content-type') || '').toLowerCase();
      if (ct.includes('application/json')) return await res.json();
      return await res.text();
    } catch (e) {
      lastErr = e;
      if (attempt < retries) { await sleep(1000 * Math.pow(2, attempt)); attempt++; continue; }
      throw e;
    }
  }
  throw lastErr || new Error('fetchWithRetry failed');
}

// In-memory cache: per-mint album with all wallets (fee_payer) entries
// Map<mint, {
//   mint, token_symbol, token_name, token_icon,
//   usd_price, liquidity, holder_count, mcap,
//   twitter, website,
//   last_signature, last_slot, last_timestamp, source_url,
//   accounts: Map<wallet(fee_payer), {
//     account, first_seen, last_seen, last_signature, last_slot, last_amount, txCount
//   }>
// }>
const mintCache = new Map();

function mapSourceToTypeLabel(sourceUrl) {
  try {
    const s = String(sourceUrl || '');
    if (!s) return null;
    if (s.includes('channels/958046672473194556/1241009019494072370')) return 'AG';
    if (s.includes('channels/1372291116853887077/1382098297891717252')) return 'Fresh';
    if (s.includes('channels/1372291116853887077/1382099149842812988')) return 'Dormant';
    if (s.includes('channels/1372291116853887077/1387731366212538390')) return 'SNS';
    if (s.includes('channels/1372291116853887077/1387731454577872936')) return 'AboveAVG';
    return null;
  } catch { return null; }
}

function updateMintCache(tx) {
  if (!tx || !tx.mint) return;
  const ts = sanitizeTimestampToSeconds(tx.timestamp);
  let card = mintCache.get(tx.mint);
  if (!card) {
    card = {
      mint: tx.mint,
      token_symbol: tx.token_symbol ?? null,
      token_name: tx.token_name ?? null,
      token_icon: tx.token_icon ?? null,
      usd_price: tx.usd_price ?? null,
      liquidity: tx.liquidity ?? null,
      holder_count: tx.holder_count ?? null,
      mcap: tx.mcap ?? null,
      top_holders_percentage: tx.top_holders_percentage ?? null,
      dev_migrations: tx.dev_migrations ?? null,
      dev: tx.dev ?? null,
      launchpad: tx.launchpad ?? null,
      first_pool_created_at: tx.first_pool_created_at ?? null,
      twitter: tx.twitter ?? null,
      website: tx.website ?? null,
      last_signature: tx.signature ?? null,
      last_slot: tx.slot ?? null,
      last_timestamp: ts,
      source_url: tx.source_url ?? null,
      type_label: mapSourceToTypeLabel(tx.source_url) ?? null,
      fund_origin: tx.fund_origin ?? null,
      fund_age_literal: tx.fund_age_literal ?? null,
      fund_age_seconds: tx.fund_age_seconds ?? null,
      accounts: new Map(),
    };
    mintCache.set(tx.mint, card);
  } else {
    // Update token-level fields when we get new non-null info
    card.token_symbol = card.token_symbol ?? tx.token_symbol ?? null;
    card.token_name = card.token_name ?? tx.token_name ?? null;
    card.token_icon = card.token_icon ?? tx.token_icon ?? null;
    card.usd_price = tx.usd_price ?? card.usd_price ?? null;
    card.liquidity = tx.liquidity ?? card.liquidity ?? null;
    card.holder_count = tx.holder_count ?? card.holder_count ?? null;
    card.mcap = tx.mcap ?? card.mcap ?? null;
    if (tx.top_holders_percentage != null) card.top_holders_percentage = tx.top_holders_percentage;
    if (tx.dev_migrations != null) card.dev_migrations = tx.dev_migrations;
    card.dev = tx.dev ?? card.dev ?? null;
    card.launchpad = tx.launchpad ?? card.launchpad ?? null;
    card.first_pool_created_at = tx.first_pool_created_at ?? card.first_pool_created_at ?? null;
    card.twitter = card.twitter ?? tx.twitter ?? null;
    card.website = card.website ?? tx.website ?? null;
    card.source_url = card.source_url ?? tx.source_url ?? null;
    if (!card.type_label) {
      const t = mapSourceToTypeLabel(tx.source_url);
      if (t) card.type_label = t;
    }
    // Fill funding fields if not yet set and we have new info
    card.fund_origin = card.fund_origin ?? tx.fund_origin ?? null;
    card.fund_age_literal = card.fund_age_literal ?? tx.fund_age_literal ?? null;
    card.fund_age_seconds = card.fund_age_seconds ?? tx.fund_age_seconds ?? null;
    if (!card.last_timestamp || ts >= card.last_timestamp) {
      card.last_timestamp = ts;
      card.last_slot = tx.slot ?? card.last_slot ?? null;
      card.last_signature = tx.signature ?? card.last_signature ?? null;
    }
  }

  // Track all wallet (fee_payer) entries for this mint
  if (tx.to_user_account) {
    const acct = tx.to_user_account; // now populated from fee_payer
    let entry = card.accounts.get(acct);
    if (!entry) {
      entry = {
        account: acct,
        first_seen: ts,
        last_seen: ts,
        last_signature: tx.signature ?? null,
        last_slot: tx.slot ?? null,
        last_amount: tx.token_amount ?? null,
        sold_at: null,
        txCount: 1,
        // Per-wallet funding captured from this specific signature when available
        funding_origin: tx.fund_origin ?? null,
        fund_age_literal: tx.fund_age_literal ?? null,
        fund_age_seconds: tx.fund_age_seconds ?? null,
        funding_signature: tx.signature ?? null,
        type_label: mapSourceToTypeLabel(tx.source_url) ?? null,
        type_tags: (() => { const t = mapSourceToTypeLabel(tx.source_url); return t ? [t] : []; })(),
      };
      card.accounts.set(acct, entry);
    } else {
      entry.last_seen = Math.max(entry.last_seen || 0, ts);
      entry.last_signature = tx.signature ?? entry.last_signature;
      entry.last_slot = tx.slot ?? entry.last_slot;
      entry.last_amount = tx.token_amount ?? entry.last_amount;
      if (entry.last_amount > 0) entry.sold_at = null;
      entry.txCount = (entry.txCount || 0) + 1;
      // Update funding to the latest non-null info for this wallet
      if (tx.fund_origin != null) entry.funding_origin = tx.fund_origin;
      if (tx.fund_age_literal != null) entry.fund_age_literal = tx.fund_age_literal;
      if (tx.fund_age_seconds != null) entry.fund_age_seconds = tx.fund_age_seconds;
      if (tx.signature) entry.funding_signature = tx.signature;
      const t = mapSourceToTypeLabel(tx.source_url);
      if (t) {
        if (!entry.type_label) entry.type_label = t;
        if (!Array.isArray(entry.type_tags)) entry.type_tags = [];
        if (!entry.type_tags.includes(t)) entry.type_tags.push(t);
      }
    }
  }
  // Track threshold timing for cleanup
  try { updateCardThreshold(mintCache.get(tx.mint)); } catch {}
}

function getMintCacheRows({ limit = 100 } = {}) {
  const rows = Array.from(mintCache.values())
    .sort((a, b) => (b.last_timestamp || 0) - (a.last_timestamp || 0))
    .slice(0, limit)
    .map(card => ({
      mint: card.mint,
      token_symbol: card.token_symbol,
      token_name: card.token_name,
      token_icon: card.token_icon,
      usd_price: card.usd_price,
      liquidity: card.liquidity,
      holder_count: card.holder_count,
      mcap: card.mcap,
      top_holders_percentage: card.top_holders_percentage ?? null,
      dev_migrations: card.dev_migrations ?? null,
      dev: card.dev,
      launchpad: card.launchpad,
      first_pool_created_at: card.first_pool_created_at,
      twitter: card.twitter,
      website: card.website,
      last_signature: card.last_signature,
      last_slot: card.last_slot,
      last_timestamp: card.last_timestamp,
      source_url: card.source_url,
      fund_origin: card.fund_origin ?? null,
      fund_age_literal: card.fund_age_literal ?? null,
      fund_age_seconds: card.fund_age_seconds ?? null,
      accounts: Array.from(card.accounts.values())
    }));
  return rows;
}

function logMintTable(limit = 25) {
  const rows = getMintCacheRows({ limit }).map(r => {
    const walletsCount = r.accounts ? r.accounts.length : 0;
    const total = (r.accounts || []).reduce((sum, a) => {
      const n = Number(a.last_amount);
      return sum + (Number.isFinite(n) ? n : 0);
    }, 0);
    return {
      mint: r.mint,
      symbol: r.token_symbol,
      wallets: walletsCount,
      total,
      price: r.usd_price,
      liq: r.liquidity,
      mcap: r.mcap,
      ts: r.last_timestamp,
    };
  });
  if (rows.length) {
    console.log(`Latest per mint (showing ${rows.length}):`);
    console.table(rows);
  }
}

// Optional detailed log: accounts per mint (flattened)
function logMintAccountsTable({ mintLimit = 15, accountsPerMint = 5, sortBy = 'last_seen' } = {}) {
  const cards = getMintCacheRows({ limit: mintLimit });
  const rows = [];
  for (const c of cards) {
    const acctList = (c.accounts || [])
      .sort((a, b) => (b[sortBy] || 0) - (a[sortBy] || 0))
      .slice(0, accountsPerMint);
    for (const a of acctList) {
      rows.push({
        mint: c.mint,
        symbol: c.token_symbol,
        wallet: a.account,
        txCount: a.txCount || 0,
        last_amount: a.last_amount,
        last_seen: a.last_seen,
        last_signature: a.last_signature,
        sold_at: a.sold_at || null,
      });
    }
  }
  if (rows.length) {
    console.log(`Wallets (fee_payer) per mint (up to ${accountsPerMint} each, ${rows.length} rows):`);
    console.table(rows);
  }
}

// Full mint cards as a table (all columns, excluding nested accounts)
function short(s, left = 6, right = 6) {
  if (s == null) return s;
  const str = String(s);
  if (str.length <= left + right + 3) return str;
  return str.slice(0, left) + '…' + str.slice(-right);
}

function logMintFullTableForMints(mints) {
  try {
    const set = new Set((mints || []).filter(Boolean));
    const rows = [];
    for (const mint of set) {
      const card = mintCache.get(mint);
      if (!card) continue;
      let obj;
      if (FULLMINT_TABLE_COMPACT) {
        obj = {
          mint: short(card.mint, 8, 8),
          token_symbol: card.token_symbol,
          token_name: card.token_name,
          usd_price: card.usd_price,
          liquidity: card.liquidity,
          holder_count: card.holder_count,
          mcap: card.mcap,
          dev: card.dev,
          launchpad: card.launchpad,
          first_pool_created_at: card.first_pool_created_at,
          last_signature: short(card.last_signature, 8, 8),
          last_slot: card.last_slot,
          last_timestamp: card.last_timestamp,
          source_url: card.source_url ? short(card.source_url, 18, 12) : null,
          wallets_count: card.accounts ? card.accounts.size : 0,
        };
      } else {
        obj = {
          mint: card.mint,
          token_symbol: card.token_symbol,
          token_name: card.token_name,
          token_icon: card.token_icon,
          usd_price: card.usd_price,
          liquidity: card.liquidity,
          holder_count: card.holder_count,
          mcap: card.mcap,
          dev: card.dev,
          launchpad: card.launchpad,
          first_pool_created_at: card.first_pool_created_at,
          twitter: card.twitter,
          website: card.website,
          last_signature: card.last_signature,
          last_slot: card.last_slot,
          last_timestamp: card.last_timestamp,
          source_url: card.source_url,
          wallets_count: card.accounts ? card.accounts.size : 0,
        };
      }
      rows.push(obj);
    }
    if (rows.length) {
      console.log(`Full mint cards (table) for ${rows.length} mints:`);
      console.table(rows);
    }
  } catch (e) {
    console.error('Failed to log full mint table:', e);
  }
}

// Full wallets (fee_payers) table for given mints (all wallet fields)
function logWalletsFullTableForMints(mints, { sortBy = 'last_seen' } = {}) {
  try {
    const set = new Set((mints || []).filter(Boolean));
    const rows = [];
    for (const mint of set) {
      const card = mintCache.get(mint);
      if (!card) continue;
      const accts = Array.from(card.accounts.values())
        .sort((a, b) => (b[sortBy] || 0) - (a[sortBy] || 0));
      for (const a of accts) {
        rows.push({
          mint: card.mint,
          token_symbol: card.token_symbol,
          wallet: a.account,
          first_seen: a.first_seen,
          last_seen: a.last_seen,
          last_signature: a.last_signature,
          last_slot: a.last_slot,
          last_amount: a.last_amount,
          sold_at: a.sold_at || null,
          txCount: a.txCount || 0,
          funding_origin: a.funding_origin ?? null,
          fund_age_literal: a.fund_age_literal ?? null,
        });
      }
    }
    if (rows.length) {
      console.log(`Full wallets (fee_payers) table, ${rows.length} rows:`);
      console.table(rows);
    }
  } catch (e) {
    console.error('Failed to log full wallets table:', e);
  }
}

// Build a full card shape from internal cache card
function buildMintCardObject(card) {
  if (!card) return null;
  const walletsArr = Array.from(card.accounts.values())
    .sort((a, b) => (b.last_seen || 0) - (a.last_seen || 0))
    .map(a => ({
      wallet: a.account,
      first_seen: a.first_seen,
      last_seen: a.last_seen,
      last_signature: a.last_signature,
      last_slot: a.last_slot,
      last_amount: a.last_amount,
      sold_at: a.sold_at || null,
      txCount: a.txCount || 0,
      funding_origin: a.funding_origin ?? null,
      fund_age_literal: a.fund_age_literal ?? null,
      fund_age_seconds: a.fund_age_seconds ?? null,
      funding_signature: a.funding_signature ?? a.last_signature ?? null,
      type_label: a.type_label ?? null,
      type_tags: Array.isArray(a.type_tags) ? a.type_tags : [],
    }));
  return {
    mint: card.mint,
    token_symbol: card.token_symbol,
    token_name: card.token_name,
    token_icon: card.token_icon,
    usd_price: card.usd_price,
    liquidity: card.liquidity,
    mcap: card.mcap,
    holder_count: card.holder_count,
    top_holders_percentage: card.top_holders_percentage ?? null,
    dev_migrations: card.dev_migrations ?? null,
    dev: card.dev,
    launchpad: card.launchpad,
    first_pool_created_at: card.first_pool_created_at,
    twitter: card.twitter,
    website: card.website,
    last_signature: card.last_signature,
    last_slot: card.last_slot,
    last_timestamp: card.last_timestamp,
    source_url: card.source_url,
    fund_origin: card.fund_origin ?? null,
    fund_age_literal: card.fund_age_literal ?? null,
    fund_age_seconds: card.fund_age_seconds ?? null,
    wallets_count: walletsArr.length,
    fee_payers: walletsArr,
    type_label: card.type_label ?? null,
  };
}

// Log full cards and complete fee_payers for the given set of mints
function logFullCardsForMints(mints) {
  try {
    const uniqueMints = Array.from(new Set(mints.filter(Boolean)));
    if (!uniqueMints.length) return;
    console.log(`Full mint cards (${uniqueMints.length}):`);
    for (const mint of uniqueMints) {
      const card = mintCache.get(mint);
      if (!card) continue;
      const obj = buildMintCardObject(card);
      console.log(`--- Mint: ${mint} ---`);
      console.log(JSON.stringify(obj, null, 2));
    }
  } catch (e) {
    console.error('Failed logging full cards:', e);
  }
}

// Helper: sum of last_amount for a card
function getMintTotal(card) {
  try {
    let sum = 0;
    if (!card || !card.accounts) return 0;
    // card.accounts is a Map
    for (const entry of card.accounts.values()) {
      const n = Number(entry.last_amount);
      if (Number.isFinite(n)) sum += n;
    }
    return sum;
  } catch { return 0; }
}

// Mark since when the card is under threshold; clear when back above
function updateCardThreshold(card) {
  if (!card) return false;
  const total = getMintTotal(card);
  if (total >= MINT_MIN_TOTAL_FOR_REFRESH) {
    if (card.under_threshold_since) { card.under_threshold_since = null; return true; }
    return false;
  } else {
    if (!card.under_threshold_since) { card.under_threshold_since = Date.now(); return true; }
    return false;
  }
}

function cleanupMintCache() {
  const now = Date.now();
  const removed = [];
  for (const [mint, card] of mintCache.entries()) {
    const total = getMintTotal(card);
    if (total >= MINT_MIN_TOTAL_FOR_REFRESH) {
      card.under_threshold_since = null;
      continue;
    }
    if (!card.under_threshold_since) {
      card.under_threshold_since = now;
      continue;
    }
    if (now - card.under_threshold_since >= MINT_CLEANUP_UNDER_TOTAL_MS) {
      mintCache.delete(mint);
      removed.push({ mint, total });
    }
  }
  if (removed.length) {
    const mins = Math.round(MINT_CLEANUP_UNDER_TOTAL_MS / 60000);
    console.log(`Cache cleanup: removed ${removed.length} mints under total < ${MINT_MIN_TOTAL_FOR_REFRESH} for > ${mins}m`);
    try {
      emitMintCleanup(removed);
    } catch {}
  }
}

// ==================== HOLDINGS REFRESH (Jupiter primary, Helius fallback) ====================
function extractHoldingsMapFromJupiter(data) {
  // Returns Map<mint, uiAmount>
  // Supports multiple Jupiter response shapes:
  // - Array of token balances
  // - Object with `holdings`/`data` array
  // - Object with `tokens` as a map: { [mint]: TokenAccount[] }
  const map = new Map();
  const toUi = (t) => {
    if (t == null) return undefined;
    if (t.uiAmount != null) return Number(t.uiAmount);
    if (t.amount != null && t.decimals != null) {
      const dec = Number(t.decimals);
      const amt = Number(t.amount);
      if (Number.isFinite(dec) && Number.isFinite(amt)) return amt / Math.pow(10, dec);
    }
    if (t.balance != null) return Number(t.balance);
    if (t.uiBalance != null) return Number(t.uiBalance);
    if (t.uiAmountString != null) return Number(t.uiAmountString);
    return undefined;
  };
  const push = (mint, ui) => {
    if (!mint) return;
    const val = Number(ui);
    if (Number.isFinite(val)) map.set(mint, val);
  };
  if (!data) return map;

  // Case 1: response is an array of tokens
  if (Array.isArray(data)) {
    for (const t of data) {
      const mint = t.mint || t.address || t.id;
      push(mint, toUi(t));
    }
    return map;
  }

  // Case 2: `holdings` or `data` is an array
  const arr = data.holdings || data.data;
  if (Array.isArray(arr)) {
    for (const t of arr) {
      const mint = t.mint || t.address || t.id;
      push(mint, toUi(t));
    }
    return map;
  }

  // Case 3: `tokens` is a map keyed by mint -> TokenAccount[]
  const tokens = data.tokens;
  if (tokens && typeof tokens === 'object' && !Array.isArray(tokens)) {
    for (const [mint, accounts] of Object.entries(tokens)) {
      if (!Array.isArray(accounts)) continue;
      let sum = 0;
      for (const acc of accounts) {
        const ui = toUi(acc);
        if (Number.isFinite(ui)) sum += Number(ui);
      }
      push(mint, sum);
    }
    return map;
  }

  return map;
}

async function jupiterGetHoldings(wallet) {
  const headers = { 'X-API-KEY': JUP_KEY, 'Accept': 'application/json' };
  const candidates = [];
  // 1) Primary Ultra API path style: /ultra/v1/holdings/:address
  candidates.push(`${JUP_BASE_URL}${JUP_HOLDINGS_PATH}/${encodeURIComponent(wallet)}`);
  if (JUP_HOLDINGS_PATH !== '/ultra/v1/holdings') {
    candidates.push(`${JUP_BASE_URL}/ultra/v1/holdings/${encodeURIComponent(wallet)}`);
  }
  // 2) Older wallet-based variants
  candidates.push(`${JUP_BASE_URL}${JUP_HOLDINGS_PATH}?wallet=${encodeURIComponent(wallet)}`);
  candidates.push(`${JUP_BASE_URL}/v1/wallet/holdings?wallet=${encodeURIComponent(wallet)}`);
  candidates.push(`${JUP_BASE_URL}/v1/wallet/holdings?publicKey=${encodeURIComponent(wallet)}`);
  candidates.push(`${JUP_BASE_URL}/wallet/holdings?wallet=${encodeURIComponent(wallet)}`);

  let lastErr;
  for (const url of candidates) {
    try {
      const data = await fetchWithRetry(url, { headers }, 0);
      return data;
    } catch (e) {
      lastErr = e;
      if (String(e?.message || '').includes('HTTP 404')) {
        // Try next candidate silently
        continue;
      }
      // For other errors (e.g., 429, 5xx), bubble up to retry next tick
      throw e;
    }
  }
  throw lastErr || new Error('All Jupiter holdings endpoint candidates failed');
}

async function heliusGetTokenAccounts(mint) {
  try {
    const body = {
      jsonrpc: '2.0',
      id: '1',
      method: 'getTokenAccounts',
      params: { mint, limit: 1000 }
    };
    const data = await fetchWithRetry(HELIUS_RPC_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    }, 1);
    // Normalize to Map<wallet, uiAmount>
    const map = new Map();
    const accounts = data?.result?.token_accounts || [];
    for (const acc of accounts) {
      const owner = acc.owner || acc.owner_address || acc.address;
      let ui = undefined;
      if (acc.token_amount && acc.token_amount.amount != null && acc.token_amount.decimals != null) {
        ui = Number(acc.token_amount.amount) / Math.pow(10, Number(acc.token_amount.decimals));
      } else if (acc.amount != null) {
        // Fallback assume 6 decimals if not provided
        ui = Number(acc.amount) / 1e6;
      }
      if (owner && Number.isFinite(ui)) map.set(owner, ui);
    }
    return map;
  } catch (e) {
    const msg = String(e?.message || '');
    if (msg.includes('HTTP 429')) {
      const waitMs = 60_000;
      heliusBackoffUntil = Date.now() + waitMs;
      console.log(`Helius rate limited (429). Backing off for ${Math.round(waitMs/1000)}s`);
    } else {
      console.log('Helius getTokenAccounts failed:', msg);
    }
    return new Map();
  }
}

function updateWalletAmountInCache(mint, wallet, uiAmount) {
  const card = mintCache.get(mint);
  if (!card) return false;
  const entry = card.accounts.get(wallet);
  if (!entry) return false;
  const prev = Number(entry.last_amount) || 0;
  const next = Number(uiAmount) || 0;
  if (prev === next) return false;
  entry.last_amount = next;
  entry.last_seen = Math.floor(Date.now() / 1000);
  entry.sold_at = next <= 0 ? Math.floor(Date.now() / 1000) : null;
  try { updateCardThreshold(card); } catch {}
  emitHoldingUpdate(mint, wallet, next);
  return true;
}

// Build a prioritized wallet list (by most recent mint/seen)
function buildWalletPriorityList() {
  const scoreByWallet = new Map();
  for (const card of mintCache.values()) {
    if (getMintTotal(card) < MINT_MIN_TOTAL_FOR_REFRESH) continue;
    const base = card.last_timestamp || 0;
    for (const entry of card.accounts.values()) {
      const score = Math.max(base, entry.last_seen || 0);
      const prev = scoreByWallet.get(entry.account) || 0;
      if (score > prev) scoreByWallet.set(entry.account, score);
    }
  }
  return Array.from(scoreByWallet.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([w]) => w);
}

let walletPriority = [];
let walletCursor = 0;
// Aggregate refresh counters for periodic summary
const refreshCounters = { j_checked: 0, j_updated: 0, j_mints: 0, h_scanned: 0, h_updated: 0, h_mints: 0 };

async function holdingsJupiterTick() {
  try {
    // Respect backoff if previously rate-limited
    if (Date.now() < jupiterBackoffUntil) return;
    if (walletCursor >= walletPriority.length) {
      walletPriority = buildWalletPriorityList();
      walletCursor = 0;
      if (!walletPriority.length) return;
    }
    const used = new Set();
    const batchWallets = [];
    while (batchWallets.length < JUP_HOLDINGS_RPS && walletCursor < walletPriority.length) {
      const w = walletPriority[walletCursor++];
      if (!used.has(w)) { used.add(w); batchWallets.push(w); }
    }

    const updates = [];
    let updatedWallets = 0;
    for (const wallet of batchWallets) {
      try {
        const data = await jupiterGetHoldings(wallet);
        const map = extractHoldingsMapFromJupiter(data);
        // For each mint we track with this wallet, update amount or zero if missing
        for (const [mint, card] of mintCache.entries()) {
          if (!card.accounts.has(wallet)) continue;
          const ui = map.has(mint) ? map.get(mint) : 0;
          if (updateWalletAmountInCache(mint, wallet, ui)) { updates.push(mint); updatedWallets++; }
        }
      } catch (e) {
        const msg = String(e?.message || '');
        if (msg.includes('HTTP 429')) {
          const waitMs = 30_000;
          jupiterBackoffUntil = Date.now() + waitMs;
          console.log(`Jupiter holdings rate limited (429). Backing off for ${Math.round(waitMs/1000)}s`);
        }
        // ignore other errors
      }
    }
    const uniq = Array.from(new Set(updates));
    refreshCounters.j_checked += batchWallets.length;
    refreshCounters.j_updated += updatedWallets;
    refreshCounters.j_mints += uniq.length;
    if (LOG_HOLDINGS_TICK) {
      console.log(`Holdings refresh tick (Jupiter): checked ${batchWallets.length} wallets, updated ${updatedWallets}, mintsChanged ${uniq.length}`);
    }
  } catch (e) {
    console.log('holdingsJupiterTick error:', e.message);
  }
}

let heliusScanActive = false;
let heliusCursor = 0;
let heliusMints = [];
let heliusLastStart = 0;
async function holdingsHeliusTick() {
  const now = Date.now();
  // Respect backoff if previously rate-limited
  if (now < heliusBackoffUntil) return;
  if (!heliusScanActive && now - heliusLastStart < getHeliusIntervalMs()) return;
  if (!heliusScanActive) {
    // Start new scan prioritizing recent mints
    heliusMints = Array.from(mintCache.values())
      .filter(c => getMintTotal(c) >= MINT_MIN_TOTAL_FOR_REFRESH)
      .sort((a, b) => (b.last_timestamp || 0) - (a.last_timestamp || 0))
      .map(c => c.mint);
    heliusCursor = 0;
    heliusScanActive = true;
    heliusLastStart = now;
  }
  if (!heliusMints.length) { heliusScanActive = false; return; }

  const maxReq = HELIUS_REFRESH_RPS;
  const changed = new Set();
  let updatedWallets = 0;
  let processedMints = 0;
  for (let i = 0; i < maxReq && heliusCursor < heliusMints.length; i++) {
    const mint = heliusMints[heliusCursor++];
    processedMints++;
    try {
      const map = await heliusGetTokenAccounts(mint);
      const card = mintCache.get(mint);
      if (!card) continue;
      // For known wallets for this mint: update to current UI balance, or zero if absent
      for (const entry of card.accounts.values()) {
        const ui = map.has(entry.account) ? map.get(entry.account) : 0;
        if (updateWalletAmountInCache(mint, entry.account, ui)) { changed.add(mint); updatedWallets++; }
      }
    } catch {}
  }
  refreshCounters.h_scanned += processedMints;
  refreshCounters.h_updated += updatedWallets;
  refreshCounters.h_mints += changed.size;
  if (LOG_HOLDINGS_TICK) {
    console.log(`Holdings refresh tick (Helius): scanned ${processedMints} mints, updated ${updatedWallets} wallets, mintsChanged ${changed.size}, progress ${heliusCursor}/${heliusMints.length}`);
  }
  if (heliusCursor >= heliusMints.length) {
    heliusScanActive = false;
  }
}

function getHeliusIntervalMs() {
  // Ensure at least 60s between scans unless explicitly configured higher
  return Math.max(HELIUS_REFRESH_INTERVAL_MS, 60_000);
}

// Schedule ticks per second (respect 10 req/s for each service)
setInterval(() => { holdingsJupiterTick().catch(() => {}); }, 1000);
setInterval(() => { holdingsHeliusTick().catch(() => {}); }, 1000);
// Print periodic refresh summary
setInterval(() => {
  try {
    const j = refreshCounters;
    console.log(`Refresh summary: Jupiter wallets checked=${j.j_checked}, updated=${j.j_updated}, mints=${j.j_mints} | Helius mints scanned=${j.h_scanned}, wallets updated=${j.h_updated}, mints=${j.h_mints}`);
    j.j_checked = j.j_updated = j.j_mints = j.h_scanned = j.h_updated = j.h_mints = 0;
  } catch {}
}, LOG_REFRESH_SUMMARY_INTERVAL_MS);
// Periodic cleanup of low-total mints
setInterval(() => { cleanupMintCache(); }, Math.max(5000, MINT_CLEANUP_INTERVAL_MS));

function addToBuffer(event) {
  // event: { id: string, data: object, ts?: number }
  eventBuffer.push(event);
  if (eventBuffer.length > EVENT_BUFFER_SIZE) {
    eventBuffer.shift();
  }
}

function broadcastEvent(event) {
  const payload = `id: ${event.id}\n` +
                  `data: ${JSON.stringify(event.data)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch (e) {
      // Best-effort; drop broken client
      try { res.end(); } catch {}
      sseClients.delete(res);
    }
  }
}

function sanitizeTimestampToSeconds(ts) {
  if (!ts) return Math.floor(Date.now() / 1000);
  // If ts looks like ms, convert to s
  return ts > 10000000000 ? Math.floor(ts / 1000) : ts;
}

app.use(cors());
app.use(express.json());
// Serve simple test UI from / (public/index.html)
app.use(express.static('public'));

// Emit SSE para card atualizado (metadados/preço/etc)
function emitMintCardUpdate(input) {
  try {
    const card = typeof input === 'string' ? mintCache.get(input) : input;
    if (!card) return;
    const obj = buildMintCardObject(card);
    const ts = Math.floor(Date.now() / 1000);
    const id = `mint-card-${card.mint}-${ts}`;
    const evt = { id, data: { type: 'mint_card_update', ...obj, timestamp: ts } };
    addToBuffer(evt);
    broadcastEvent(evt);
  } catch {}
}

// ==================== EXAMPLE RESPONSE LOGGING ====================
// Saves one example of full JSON response per route in /logs

// Emit SSE for holdings update events
function emitHoldingUpdate(mint, wallet, last_amount) {
  const ts = Math.floor(Date.now() / 1000);
  const id = `hold-${mint}-${wallet}-${ts}`;
  const evt = { id, data: { type: 'holding_update', mint, wallet, last_amount, timestamp: ts } };
  addToBuffer(evt);
  broadcastEvent(evt);
}

 

 

// Emit SSE informing which mints were cleaned up from cache
function emitMintCleanup(removedList) {
  try {
    if (!Array.isArray(removedList) || removedList.length === 0) return;
    const ts = Math.floor(Date.now() / 1000);
    const mints = removedList.map(r => r.mint).filter(Boolean);
    const evt = {
      id: `cleanup-${ts}-${mints.length}`,
      data: {
        type: 'mint_cleanup',
        mints,
        details: removedList,
        threshold: MINT_MIN_TOTAL_FOR_REFRESH,
        older_than_ms: MINT_CLEANUP_UNDER_TOTAL_MS,
        timestamp: ts,
      }
    };
    addToBuffer(evt);
    broadcastEvent(evt);
  } catch {}
}

// Parse time literal like "2d", "3h", "45m", "30s", "2w", "1mo", "1y" into seconds
function parseTimeLiteral(expr) {
  if (expr == null) return null;
  const m = String(expr).trim().match(/^(\d+(?:\.\d+)?)\s*(s|m|h|d|w|mo|y)$/i);
  if (!m) return null;
  const q = parseFloat(m[1]);
  const u = m[2].toLowerCase();
  const mult = { s: 1, m: 60, h: 3600, d: 86400, w: 604800, mo: 2592000, y: 31536000 };
  const secs = q * (mult[u] || 1);
  return Number.isFinite(secs) ? Math.round(secs) : null;
}

app.use((req, res, next) => {
  const originalJson = res.json.bind(res);
  res.json = (body) => {
    try {
      const routePath = (req.route && req.route.path) || req.path || 'unknown';
      const method = (req.method || 'GET').toLowerCase();
      const sanitized = String(routePath)
        .replace(/[^a-zA-Z0-9_-]+/g, '_')
        .replace(/^_+|_+$/g, '');
      const filename = `EXAMPLE_${method}_${sanitized || 'root'}.txt`;
      const filepath = path.join(LOG_DIR, filename);
      const shouldOverwrite = String(process.env.OVERWRITE_EXAMPLE_LOGS || '').toLowerCase() === 'true';
      if (shouldOverwrite || !fs.existsSync(filepath)) {
        const content = typeof body === 'string' ? body : JSON.stringify(body, null, 2);
        fs.writeFile(filepath, content, (err) => {
          if (err) console.error('Failed to write example log:', err.message);
        });
      }
    } catch (err) {
      console.error('Error logging example response:', err.message);
    }
    return originalJson(body);
  };
  next();
});

// Teste de conexão na inicialização
testConnection().then(connected => {
  if (connected) {
    console.log('🚀 Server ready with Supabase kv_store connection');
  } else {
    console.error('❌ Failed to connect to Supabase kv_store');
  }
});

// ==================== JUPITER BATCH REFRESH ====================
function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
  return chunks;
}

async function jupiterBatchSearch(queries) {
  const url = `${JUP_BASE_URL}/tokens/v2/search`;
  try {
    const data = await fetchWithRetry(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-KEY': JUP_KEY,
      },
      body: JSON.stringify({ queries })
    }, 1);
    return data;
  } catch (e) {
    const msg = String(e?.message || '');
    if (msg.includes('HTTP 429')) {
      // Backoff on rate limit
      const waitMs = 30_000;
      jupiterBackoffUntil = Date.now() + waitMs;
      console.log(`Jupiter search rate limited (429). Backing off for ${Math.round(waitMs/1000)}s`);
      throw e;
    }
    // Fallback to individual GETs if batch POST fails (non-429)
    const results = [];
    for (const q of queries) {
      try {
        const singleUrl = `${JUP_BASE_URL}/tokens/v2/search?query=${encodeURIComponent(q)}`;
        const r = await fetchWithRetry(singleUrl, { headers: { 'X-API-KEY': JUP_KEY } }, 1);
        if (Array.isArray(r)) {
          for (const t of r) results.push({ __query: q, ...t });
        }
      } catch {}
    }
    return results;
  }
}

function updateCardFromJupToken(card, token) {
  if (!card || !token) return;
  if (token.holderCount !== undefined && token.holderCount !== null) card.holder_count = token.holderCount;
  if (token.mcap !== undefined && token.mcap !== null) card.mcap = token.mcap;
  if (token.usdPrice !== undefined && token.usdPrice !== null) card.usd_price = token.usdPrice;
  if (token.liquidity !== undefined && token.liquidity !== null) card.liquidity = token.liquidity;
  if (token.topHoldersPercentage !== undefined && token.topHoldersPercentage !== null) card.top_holders_percentage = token.topHoldersPercentage;
  if (token.devMigrations !== undefined && token.devMigrations !== null) card.dev_migrations = token.devMigrations;
  card.token_name = card.token_name ?? token.name ?? null;
  card.token_symbol = card.token_symbol ?? token.symbol ?? null;
  card.token_icon = card.token_icon ?? token.icon ?? null;
  card.twitter = card.twitter ?? token.twitter ?? null;
  card.website = card.website ?? token.website ?? null;
}

let jupRefreshInProgress = false;
let jupRefreshCursor = 0;
async function refreshMintStatsFromCache() {
  if (!JUP_REFRESH_ENABLED) return;
  if (jupRefreshInProgress) return;
  if (Date.now() < jupiterBackoffUntil) return;
  // Filter mints by minimum total threshold
  const mints = Array.from(mintCache.entries())
    .filter(([_, card]) => getMintTotal(card) >= MINT_MIN_TOTAL_FOR_REFRESH)
    .map(([mint]) => mint);
  if (!mints.length) return;
  jupRefreshInProgress = true;
  try {
    // Per-second pacing: up to JUP_REFRESH_RPS requests per tick, each with up to JUP_REFRESH_BATCH_SIZE mints
    const batchSize = Math.max(1, JUP_REFRESH_BATCH_SIZE);
    if (jupRefreshCursor >= mints.length) jupRefreshCursor = 0;
    const maxReq = Math.max(1, Math.min(JUP_REFRESH_RPS, Math.ceil(mints.length / batchSize)));
    const promises = [];
    for (let r = 0; r < maxReq && jupRefreshCursor < mints.length; r++) {
      const start = jupRefreshCursor;
      const end = Math.min(mints.length, start + batchSize);
      const queries = mints.slice(start, end);
      jupRefreshCursor = end;
      promises.push((async () => {
        let updated = 0;
        try {
          const data = await jupiterBatchSearch(queries);
          if (Array.isArray(data)) {
            for (const token of data) {
              const mint = token.mint || token.address || token.id || token.__query;
              if (!mint) continue;
              const card = mintCache.get(mint);
              if (card) { updateCardFromJupToken(card, token); updated++; }
            }
          } else if (data && typeof data === 'object') {
            if (data.results && typeof data.results === 'object') {
              for (const q of Object.keys(data.results)) {
                const list = data.results[q] || [];
                const token = Array.isArray(list) && list.length ? list[0] : null;
                const mint = token?.mint || token?.address || token?.id || q;
                const card = mintCache.get(mint);
                if (card && token) { updateCardFromJupToken(card, token); updated++; }
              }
            } else if (Array.isArray(data.data)) {
              for (const token of data.data) {
                const mint = token.mint || token.address || token.id;
                if (!mint) continue;
                const card = mintCache.get(mint);
                if (card) { updateCardFromJupToken(card, token); updated++; }
              }
            }
          }
          // Emite atualização de card para todos os mints consultados neste slice
          for (const m of queries) emitMintCardUpdate(m);
        } catch (e) {
          const msg = String(e?.message || '');
          if (msg.includes('HTTP 429')) {
            const waitMs = 30_000;
            jupiterBackoffUntil = Date.now() + waitMs;
            console.log(`Jupiter refresh rate limited (429). Backing off for ${Math.round(waitMs/1000)}s`);
          } else {
            console.log('Jupiter refresh slice failed:', msg);
          }
        }
        return updated;
      })());
    }
    const results = await Promise.all(promises);
    const totalUpdated = results.reduce((a, b) => a + b, 0);
    if (totalUpdated > 0) {
      console.log(`Jupiter refresh tick: updated ${totalUpdated} mint cards (mcap, holder_count, price, liq).`);
    }
  } finally {
    jupRefreshInProgress = false;
  }
}

if (JUP_REFRESH_ENABLED) {
  setInterval(() => { refreshMintStatsFromCache().catch(() => {}); }, 1000);
}

 

async function processPending() {
  const signatures = [...pendingSignatures];
  const source = pendingSource;
  pendingSignatures = [];
  pendingSource = null;

  // Capture and clear funding metadata for this set
  const fundingBySig = new Map();
  for (const sig of signatures) {
    if (pendingFunding.has(sig)) {
      fundingBySig.set(sig, pendingFunding.get(sig));
      pendingFunding.delete(sig);
    }
  }

  console.log(`Processing ${signatures.length} signatures...`);

  try {
    // Verificar quais signatures já existem
    const existing = await getExistingSignatures(signatures);
    const newSignatures = signatures.filter(sig => !existing.has(sig));
    
    console.log(`Found ${existing.size} existing, ${newSignatures.length} new signatures`);

    let totalInserted = 0, totalSkipped = existing.size, totalFailed = 0;
    const allResults = [];

    // Adicionar resultados das signatures que já existem
    for (const sig of signatures) {
      if (existing.has(sig)) {
        allResults.push({ signature: sig, status: 'skipped' });
      }
    }

    if (newSignatures.length > 0) {
      // Processar novas signatures em lotes menores
      const batchSize = 10; // Reduzido para melhor performance
      
      for (let i = 0; i < newSignatures.length; i += batchSize) {
        const batch = newSignatures.slice(i, i + batchSize);
        console.log(`Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(newSignatures.length/batchSize)}`);
        
        const batchResults = await processBatch(batch, source, fundingBySig);
        allResults.push(...batchResults.results);
        
        totalInserted += batchResults.inserted;
        totalFailed += batchResults.failed;
      }
    }

    console.log(`✅ Batch completed: inserted=${totalInserted}, skipped=${totalSkipped}, failed=${totalFailed}`);
    return allResults;

  } catch (error) {
    console.error('❌ Error processing pending signatures:', error);
    return signatures.map(sig => ({ signature: sig, status: 'failed', reason: error.message }));
  }
}

async function processBatch(signatures, source_url, fundingBySig) {
  const transactionDataArray = [];

  try {
    // Batch notice
    console.log(`[Batch] Processing ${signatures.length} tx from ${source_url || 'unknown'}`);
    // Fetch from Helius with POST
    const heliusUrl = `https://api.helius.xyz/v0/transactions?api-key=${HELIUS_KEY}`;
    const heliusData = await fetchWithRetry(heliusUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ transactions: signatures })
    }, 2);

    // Log per-signature raw Helius response (one file per signature)
    try {
      if (Array.isArray(heliusData)) {
        for (let i = 0; i < signatures.length; i++) {
          const sig = signatures[i];
          const tx = heliusData[i];
          if (tx) writeApiLogOnce('helius', sig, tx);
        }
      }
    } catch (apiLogErr) {
      console.error('Failed to log Helius responses:', apiLogErr);
    }

    if (!heliusData || !Array.isArray(heliusData)) {
      console.log('No data from Helius for batch');
      return {
        inserted: 0,
        skipped: 0,
        failed: signatures.length,
        results: signatures.map(sig => ({ signature: sig, status: 'failed', reason: 'No data from Helius' }))
      };
    }

    // Processar cada transação
    for (let i = 0; i < signatures.length; i++) {
      const sig = signatures[i];
      const tx = heliusData[i];

      if (!tx) {
        transactionDataArray.push({
          signature: sig,
          source_url,
          timestamp: Date.now(),
          // Outros campos serão null
        });
        continue;
      }

      // Extract Helius fields
      const heliusFields = {
        type: tx.type || null,
        source_label: tx.source || null,
        fee: tx.fee || null,
        fee_payer: tx.feePayer || null,
        slot: tx.slot || null,
        timestamp: tx.timestamp || Date.now(),
      };

      // Find first fungible token transfer
      let tokenTransfer = null;
      if (tx.tokenTransfers && tx.tokenTransfers.length > 0) {
        tokenTransfer = tx.tokenTransfers.find(t => t.tokenStandard === 'Fungible');
      }

      const tokenFields = tokenTransfer ? {
        mint: tokenTransfer.mint,
        // Store wallet (fee_payer) in the to_user_account field for compatibility
        to_user_account: tx.feePayer || null,
        token_amount: tokenTransfer.tokenAmount,
        token_standard: tokenTransfer.tokenStandard,
      } : {
        mint: null,
        to_user_account: tx.feePayer || null,
        token_amount: null,
        token_standard: null,
      };

      // Inicializar campos Jupiter como null
      let jupiterFields = {
        token_name: null,
        token_symbol: null,
        token_icon: null,
        dev: null,
        launchpad: null,
        first_pool_created_at: null,
        holder_count: null,
        mcap: null,
        usd_price: null,
        liquidity: null,
        twitter: null,
        website: null,
        top_holders_percentage: null,
        dev_migrations: null,
        audit_mintAuthorityDisabled: null,
        audit_freezeAuthorityDisabled: null,
        // Todas as estatísticas de trading
        stats5m_priceChange: null,
        stats5m_holderChange: null,
        stats5m_liquidityChange: null,
        stats5m_buyVolume: null,
        stats5m_sellVolume: null,
        stats5m_numBuys: null,
        stats5m_numSells: null,
        stats5m_numTraders: null,
        stats5m_numNetBuyers: null,
        stats1h_priceChange: null,
        stats1h_holderChange: null,
        stats1h_liquidityChange: null,
        stats1h_buyVolume: null,
        stats1h_sellVolume: null,
        stats1h_numBuys: null,
        stats1h_numSells: null,
        stats1h_numTraders: null,
        stats1h_numNetBuyers: null,
        stats6h_priceChange: null,
        stats6h_holderChange: null,
        stats6h_liquidityChange: null,
        stats6h_buyVolume: null,
        stats6h_sellVolume: null,
        stats6h_numBuys: null,
        stats6h_numSells: null,
        stats6h_numTraders: null,
        stats6h_numNetBuyers: null,
        stats24h_priceChange: null,
        stats24h_holderChange: null,
        stats24h_liquidityChange: null,
        stats24h_buyVolume: null,
        stats24h_sellVolume: null,
        stats24h_numBuys: null,
        stats24h_numSells: null,
        stats24h_numTraders: null,
        stats24h_numNetBuyers: null,
      };

      // Buscar dados do Jupiter se temos mint
      if (tokenFields.mint) {
        try {
          const jupUrl = `${JUP_BASE_URL}/tokens/v2/search?query=${encodeURIComponent(tokenFields.mint)}`;
          const jupData = await fetchWithRetry(jupUrl, {
            headers: { 'X-API-KEY': JUP_KEY }
          }, 1);

          if (jupData && jupData.length > 0) {
            try { writeApiLogOnce('jupiter', tokenFields.mint, jupData); } catch {}
            const token = jupData[0];
            jupiterFields = {
              token_name: token.name || null,
              token_symbol: token.symbol || null,
              token_icon: token.icon || null,
              dev: token.dev || null,
              dev_migrations: token.devMigrations ?? null,
              launchpad: token.launchpad || null,
              first_pool_created_at: token.firstPool ? token.firstPool.createdAt : null,
              holder_count: token.holderCount || null,
              mcap: token.mcap || null,
              usd_price: token.usdPrice || null,
              liquidity: token.liquidity || null,
              twitter: token.twitter || null,
              website: token.website || null,
              top_holders_percentage: token.topHoldersPercentage ?? null,
              audit_mintAuthorityDisabled: token.audit ? (token.audit.mintAuthorityDisabled ? 1 : 0) : null,
              audit_freezeAuthorityDisabled: token.audit ? (token.audit.freezeAuthorityDisabled ? 1 : 0) : null,
              stats5m_priceChange: token.stats5m ? token.stats5m.priceChange : null,
              stats5m_holderChange: token.stats5m ? token.stats5m.holderChange : null,
              stats5m_liquidityChange: token.stats5m ? token.stats5m.liquidityChange : null,
              stats5m_buyVolume: token.stats5m ? token.stats5m.buyVolume : null,
              stats5m_sellVolume: token.stats5m ? token.stats5m.sellVolume : null,
              stats5m_numBuys: token.stats5m ? token.stats5m.numBuys : null,
              stats5m_numSells: token.stats5m ? token.stats5m.numSells : null,
              stats5m_numTraders: token.stats5m ? token.stats5m.numTraders : null,
              stats5m_numNetBuyers: token.stats5m ? token.stats5m.numNetBuyers : null,
              stats1h_priceChange: token.stats1h ? token.stats1h.priceChange : null,
              stats1h_holderChange: token.stats1h ? token.stats1h.holderChange : null,
              stats1h_liquidityChange: token.stats1h ? token.stats1h.liquidityChange : null,
              stats1h_buyVolume: token.stats1h ? token.stats1h.buyVolume : null,
              stats1h_sellVolume: token.stats1h ? token.stats1h.sellVolume : null,
              stats1h_numBuys: token.stats1h ? token.stats1h.numBuys : null,
              stats1h_numSells: token.stats1h ? token.stats1h.numSells : null,
              stats1h_numTraders: token.stats1h ? token.stats1h.numTraders : null,
              stats1h_numNetBuyers: token.stats1h ? token.stats1h.numNetBuyers : null,
              stats6h_priceChange: token.stats6h ? token.stats6h.priceChange : null,
              stats6h_holderChange: token.stats6h ? token.stats6h.holderChange : null,
              stats6h_liquidityChange: token.stats6h ? token.stats6h.liquidityChange : null,
              stats6h_buyVolume: token.stats6h ? token.stats6h.buyVolume : null,
              stats6h_sellVolume: token.stats6h ? token.stats6h.sellVolume : null,
              stats6h_numBuys: token.stats6h ? token.stats6h.numBuys : null,
              stats6h_numSells: token.stats6h ? token.stats6h.numSells : null,
              stats6h_numTraders: token.stats6h ? token.stats6h.numTraders : null,
              stats6h_numNetBuyers: token.stats6h ? token.stats6h.numNetBuyers : null,
              stats24h_priceChange: token.stats24h ? token.stats24h.priceChange : null,
              stats24h_holderChange: token.stats24h ? token.stats24h.holderChange : null,
              stats24h_liquidityChange: token.stats24h ? token.stats24h.liquidityChange : null,
              stats24h_buyVolume: token.stats24h ? token.stats24h.buyVolume : null,
              stats24h_sellVolume: token.stats24h ? token.stats24h.sellVolume : null,
              stats24h_numBuys: token.stats24h ? token.stats24h.numBuys : null,
              stats24h_numSells: token.stats24h ? token.stats24h.numSells : null,
              stats24h_numTraders: token.stats24h ? token.stats24h.numTraders : null,
              stats24h_numNetBuyers: token.stats24h ? token.stats24h.numNetBuyers : null,
            };
          }
        } catch (jupError) {
          console.log(`Jupiter fetch failed for ${sig}: ${jupError.message}`);
          // Continue with null Jupiter fields
        }
      }

      // Funding metadata (if present)
      const f = fundingBySig && fundingBySig.get ? fundingBySig.get(sig) : null;
      const fund_age_literal = f?.fundAgeLiteral ?? f?.fundAge ?? null;
      const fund_origin = f?.fundOrigin ?? null;
      const fund_source_tags = Array.isArray(f?.tags) ? f.tags.filter(Boolean) : [];
      const fund_age_seconds = fund_age_literal ? parseTimeLiteral(fund_age_literal) : null;

      // Preparar dados completos
      const completeData = {
        signature: sig,
        source_url,
        ...heliusFields,
        ...tokenFields,
        ...jupiterFields,
        fund_origin: fund_origin ?? null,
        fund_age_literal: fund_age_literal ?? null,
        fund_age_seconds: fund_age_seconds ?? null,
        fund_source_tags,
      };

      transactionDataArray.push(completeData);
    }

    // Inserir no Supabase usando batch
    const result = await insertTransactionBatch(transactionDataArray);
    console.log(`Batch result: ${result.inserted} inserted, ${result.skipped} skipped, ${result.failed} failed`);

    // Atualizar cache por mint e logar como tabela (um card por mint)
    try {
      const affectedMints = new Set();
      for (const tx of transactionDataArray) {
        updateMintCache(tx);
        if (tx.mint) affectedMints.add(tx.mint);
      }
      // Imprimir apenas duas tabelas: mints (cards completos) e wallets (completas)
      if (LOG_TABLE_FULL_MINTS) {
        logMintFullTableForMints(Array.from(affectedMints));
      }
      if (LOG_TABLE_FULL_WALLETS) {
        logWalletsFullTableForMints(Array.from(affectedMints));
      }
      // Emitir um evento de atualização do card para cada mint afetado
      try {
        for (const mint of affectedMints) {
          const card = mintCache.get(mint);
          if (card) emitMintCardUpdate(card);
        }
      } catch {}
      // Opcional: JSON completo por mint (habilitar definindo LOG_FULL_MINTS=true)
      if (LOG_FULL_MINTS) {
        logFullCardsForMints(Array.from(affectedMints));
      }
    } catch (cacheErr) {
      console.error('Failed updating/logging mint cache:', cacheErr);
    }

    // Emitir eventos SSE para cada transação nova processada neste batch
    try {
      for (const tx of transactionDataArray) {
        const evt = {
          // Use a própria signature como id do SSE
          id: tx.signature,
          data: {
            ...tx,
            // Garantir timestamp em segundos
            timestamp: sanitizeTimestampToSeconds(tx.timestamp)
          }
        };
        addToBuffer(evt);
        broadcastEvent(evt);
      }
    } catch (emitErr) {
      console.error('Failed to broadcast SSE events:', emitErr);
    }

    return result;

  } catch (error) {
    console.error(`Error processing batch: ${error.message}`);
    return {
      inserted: 0,
      skipped: 0,
      failed: signatures.length,
      results: signatures.map(sig => ({ signature: sig, status: 'failed', reason: error.message }))
    };
  }
}

// ==================== ENDPOINTS ====================

// Endpoint principal para receber transações
app.post('/tx', async (req, res) => {
  const { source } = req.body || {};
  let entries = Array.isArray(req.body?.entries) ? req.body.entries : null;
  let signatures = Array.isArray(req.body?.signatures) ? req.body.signatures : null;

  // Backward compatibility: accept either `entries` or plain `signatures`
  if ((!entries || entries.length === 0) && signatures && signatures.length > 0) {
    entries = signatures.map((s) => ({ signature: s }));
  }

  if (!entries || !Array.isArray(entries) || entries.length === 0) {
    return res.status(400).json({ error: 'Invalid or empty entries/signatures array' });
  }

  // Normalize and collect signatures; capture optional funding
  const sigs = [];
  for (const e of entries) {
    const sig = e?.signature;
    if (!sig) continue;
    sigs.push(sig);
    const fOrigin = e?.fundOrigin;
    const fAgeLit = e?.fundAge;
    const fTags = Array.isArray(e?.tags) ? e.tags.filter(Boolean) : (e?.tags ? [e.tags] : []);
    if (fOrigin || fAgeLit) {
      pendingFunding.set(sig, {
        fundOrigin: fOrigin ?? null,
        fundAgeLiteral: fAgeLit ?? null,
        tags: fTags,
      });
    }
  }

  if (!sigs.length) {
    return res.status(400).json({ error: 'No valid signatures in entries' });
  }

  console.log(`Received ${sigs.length} ${entries ? 'entrie(s)' : 'signature(s)'} from ${source}`);

  // Adicionar ao buffer global
  if (pendingSignatures.length === 0) {
    pendingSource = source;
  }
  pendingSignatures.push(...sigs);

  // Se temos suficientes signatures, processar imediatamente
  if (pendingSignatures.length >= 20) {
    try {
      const results = await processPending();
      res.json({
        success: true,
        processed: results.length,
        results: results
      });
    } catch (error) {
      console.error('Error processing batch:', error);
      res.status(500).json({ error: 'Failed to process batch', details: error.message });
    }
  } else {
    // Aguardar por mais signatures (até 10 segundos)
    setTimeout(async () => {
      try {
        const results = await processPending();
        // Não podemos enviar resposta aqui pois já foi enviada
      } catch (error) {
        console.error('Error in delayed processing:', error);
      }
    }, 10000);

    // Responder imediatamente que foi aceito
    res.json({
      success: true,
      message: 'Signatures queued for processing',
      queued: sigs.length,
      totalPending: pendingSignatures.length
    });
  }
});

// TEST: Emit manual SSE event(s) without Helius/Supabase pipeline
// Use only for local testing
app.post('/feed/test', (req, res) => {
  const payload = req.body;
  const events = Array.isArray(payload) ? payload : [payload];
  try {
    for (const data of events) {
      const id = data.signature || `${Date.now()}-${Math.random().toString(36).slice(2)}`;
      const normalized = { ...data, timestamp: sanitizeTimestampToSeconds(data.timestamp) };
      // Update mint cache and log as well during tests
      updateMintCache(normalized);
      const evt = { id, data: normalized };
      addToBuffer(evt);
      broadcastEvent(evt);
    }
    res.json({ ok: true, emitted: events.length });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// ==================== LIVE FEED (SSE) ====================
app.get('/feed/sse', (req, res) => {
  // Setup SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  // Register client
  sseClients.add(res);

  // Heartbeat to keep the connection alive
  const heartbeat = setInterval(() => {
    try {
      res.write(':heartbeat\n\n');
    } catch (e) {
      clearInterval(heartbeat);
    }
  }, 15000);

  // If client sent Last-Event-ID, replay from that event forward
  const lastEventId = req.header('Last-Event-ID') || req.header('last-event-id');
  if (lastEventId) {
    const startIdx = eventBuffer.findIndex(e => e.id === lastEventId);
    if (startIdx >= 0 && startIdx < eventBuffer.length - 1) {
      const replay = eventBuffer.slice(startIdx + 1);
      for (const evt of replay) {
        try {
          res.write(`id: ${evt.id}\n`);
          res.write(`data: ${JSON.stringify(evt.data)}\n\n`);
        } catch (e) {
          break;
        }
      }
    }
  }

  // Cleanup on disconnect
  req.on('close', () => {
    clearInterval(heartbeat);
    sseClients.delete(res);
    try { res.end(); } catch {}
  });
});

// Endpoint para buscar transação por signature
app.get('/tx/:signature', async (req, res) => {
  try {
    const transaction = await getBySignature(req.params.signature);
    
    if (transaction) {
      res.json(transaction);
    } else {
      res.status(404).json({ error: 'Transaction not found' });
    }
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch transaction', details: error.message });
  }
});

// Endpoint para buscar transações com filtros
app.get('/tx', async (req, res) => {
  try {
    const options = {
      mint: req.query.mint,
      signature: req.query.signature,
      fromTimestamp: req.query.fromTimestamp ? parseInt(req.query.fromTimestamp) : undefined,
      toTimestamp: req.query.toTimestamp ? parseInt(req.query.toTimestamp) : undefined,
      token_symbol: req.query.token_symbol,
      limit: req.query.limit ? parseInt(req.query.limit) : 100,
      offset: req.query.offset ? parseInt(req.query.offset) : 0,
      orderBy: req.query.orderBy || 'timestamp',
      ascending: req.query.ascending === 'true'
    };

    const result = await getTransactions(options);
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch transactions', details: error.message });
  }
});

// Endpoint para estatísticas
app.get('/stats', async (req, res) => {
  try {
    // Contagem total
    const { count: totalCount } = await supabase
      .from('kv_store_15406bac')
      .select('*', { count: 'exact', head: true });

    // Contagem por símbolo de token (top 10)
    const { data: topTokens } = await supabase
      .from('kv_store_15406bac')
      .select('token_symbol')
      .not('token_symbol', 'is', null)
      .limit(1000);

    const tokenCounts = {};
    topTokens.forEach(item => {
      if (item.token_symbol) {
        tokenCounts[item.token_symbol] = (tokenCounts[item.token_symbol] || 0) + 1;
      }
    });

    const sortedTokens = Object.entries(tokenCounts)
      .sort(([,a], [,b]) => b - a)
      .slice(0, 10)
      .map(([symbol, count]) => ({ symbol, count }));

    res.json({
      totalTransactions: totalCount,
      topTokens: sortedTokens,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch stats', details: error.message });
  }
});

// Endpoint de saúde
app.get('/health', async (req, res) => {
  try {
    const connected = await testConnection();
    res.json({
      status: 'ok',
      supabase: connected ? 'connected' : 'disconnected',
      table: 'kv_store_15406bac',
      sseClients: sseClients.size,
      bufferSize: eventBuffer.length,
      pending: pendingSignatures.length,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Endpoint para consultar o cache agregando por mint (um por mint)
app.get('/cache/mints', (req, res) => {
  const limit = req.query.limit ? parseInt(req.query.limit) : 100;
  try {
    const rows = getMintCacheRows({ limit }).map(r => ({
      mint: r.mint,
      token_symbol: r.token_symbol,
      token_name: r.token_name,
      token_icon: r.token_icon,
      usd_price: r.usd_price,
      liquidity: r.liquidity,
      holder_count: r.holder_count,
      mcap: r.mcap,
      dev: r.dev,
      launchpad: r.launchpad,
      first_pool_created_at: r.first_pool_created_at,
      twitter: r.twitter,
      website: r.website,
      last_signature: r.last_signature,
      last_slot: r.last_slot,
      last_timestamp: r.last_timestamp,
      source_url: r.source_url,
      fund_origin: r.fund_origin ?? null,
      fund_age_literal: r.fund_age_literal ?? null,
      fund_age_seconds: r.fund_age_seconds ?? null,
      wallets: (r.accounts || []).map(a => ({
        wallet: a.account,
        first_seen: a.first_seen,
        last_seen: a.last_seen,
        last_signature: a.last_signature,
        last_slot: a.last_slot,
        last_amount: a.last_amount,
        txCount: a.txCount || 0,
      }))
    }));
    res.json({ count: rows.length, data: rows });
  } catch (e) {
    res.status(500).json({ error: 'Failed to read mint cache', details: e.message });
  }
});

// Public-friendly endpoints for the site (stable naming)
// GET /mints?limit=... -> per-mint summary cards (no wallets array by default)
app.get('/mints', (req, res) => {
  const limit = req.query.limit ? parseInt(req.query.limit) : 100;
  const includeWallets = String(req.query.includeWallets || 'false').toLowerCase() === 'true';
  try {
    const cards = getMintCacheRows({ limit });
    const data = cards.map(c => ({
      mint: c.mint,
      type_label: c.type_label ?? null,
      token_symbol: c.token_symbol,
      token_name: c.token_name,
      token_icon: c.token_icon,
      usd_price: c.usd_price,
      liquidity: c.liquidity,
      mcap: c.mcap,
      holder_count: c.holder_count,
      dev: c.dev,
      launchpad: c.launchpad,
      first_pool_created_at: c.first_pool_created_at,
      twitter: c.twitter,
      website: c.website,
      last_signature: c.last_signature,
      last_slot: c.last_slot,
      last_timestamp: c.last_timestamp,
      fund_origin: c.fund_origin ?? null,
      fund_age_literal: c.fund_age_literal ?? null,
      fund_age_seconds: c.fund_age_seconds ?? null,
      wallets_count: (c.accounts || []).length,
      ...(includeWallets ? { wallets: (c.accounts || []).map(a => ({
        wallet: a.account,
        first_seen: a.first_seen,
        last_seen: a.last_seen,
        last_signature: a.last_signature,
        last_slot: a.last_slot,
        last_amount: a.last_amount,
        txCount: a.txCount || 0,
        funding_origin: a.funding_origin ?? null,
        fund_age_literal: a.fund_age_literal ?? null,
        fund_age_seconds: a.fund_age_seconds ?? null,
        funding_signature: a.funding_signature ?? a.last_signature ?? null,
        type_label: a.type_label ?? null,
        type_tags: Array.isArray(a.type_tags) ? a.type_tags : [],
      })) } : {})
    }));
    res.json({ count: data.length, data });
  } catch (e) {
    res.status(500).json({ error: 'Failed to build mints', details: e.message });
  }
});

// GET /mints/:mint -> full card for a mint (with wallets, optional paging)
app.get('/mints/:mint', (req, res) => {
  const mint = req.params.mint;
  const limit = req.query.limit ? parseInt(req.query.limit) : 100;
  const offset = req.query.offset ? parseInt(req.query.offset) : 0;
  try {
    const card = mintCache.get(mint);
    if (!card) return res.status(404).json({ error: 'Mint not found' });
    const walletsArr = Array.from(card.accounts.values())
      .sort((a, b) => (b.last_seen || 0) - (a.last_seen || 0));
    const slice = walletsArr.slice(offset, offset + limit).map(a => ({
      wallet: a.account,
      first_seen: a.first_seen,
      last_seen: a.last_seen,
      last_signature: a.last_signature,
      last_slot: a.last_slot,
      last_amount: a.last_amount,
      txCount: a.txCount || 0,
      funding_origin: a.funding_origin ?? null,
      fund_age_literal: a.fund_age_literal ?? null,
      fund_age_seconds: a.fund_age_seconds ?? null,
      funding_signature: a.funding_signature ?? a.last_signature ?? null,
      type_label: a.type_label ?? null,
      type_tags: Array.isArray(a.type_tags) ? a.type_tags : [],
    }));
    res.json({
      mint: card.mint,
      type_label: card.type_label ?? null,
      token_symbol: card.token_symbol,
      token_name: card.token_name,
      token_icon: card.token_icon,
      usd_price: card.usd_price,
      liquidity: card.liquidity,
      mcap: card.mcap,
      holder_count: card.holder_count,
      dev: card.dev,
      launchpad: card.launchpad,
      first_pool_created_at: card.first_pool_created_at,
      twitter: card.twitter,
      website: card.website,
      last_signature: card.last_signature,
      last_slot: card.last_slot,
      last_timestamp: card.last_timestamp,
      wallets_count: walletsArr.length,
      wallets: slice,
      page: { limit, offset }
    });
  } catch (e) {
    res.status(500).json({ error: 'Failed to build mint detail', details: e.message });
  }
});

// Serve frontend static build when present
try {
  const staticDir = path.join(process.cwd(), 'web', 'build');
  if (fs.existsSync(staticDir)) {
    app.use(express.static(staticDir));
    app.get('*', (req, res) => {
      res.sendFile(path.join(staticDir, 'index.html'));
    });
  }
} catch {}

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
  console.log(`📊 Using Supabase kv_store_15406bac table`);
  console.log(`🔗 Health check: http://localhost:${PORT}/health`);
  console.log(`📈 Stats: http://localhost:${PORT}/stats`);
  console.log(`🔍 Query transactions: GET http://localhost:${PORT}/tx`);
});
