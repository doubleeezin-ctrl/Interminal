import { useEffect, useState } from 'react';
import { TokenFeedRow } from './TokenFeedRow';
import { FilterPopup, HoldMins, SortBy } from './FilterPopup';
import { Button } from './ui/button';
import { Filter, ChevronDown } from 'lucide-react';
import { API_URL, fetchMints, fetchMintDetail, MintCard, HoldingUpdateEvent } from '../utils/api';
import { computeTotals } from '../utils/hold';

// Mock data para simular o feed
const mockTokens = [
  {
    id: "1",
    name: "World Liberty Financial",
    symbol: "USDT",
    icon: "💰",
    verified: true,
    chartData: Array.from({ length: 20 }, (_, i) => ({ value: 100 + Math.sin(i * 0.3) * 10 })),
    chartColor: "#ef4444",
    marketCap: "$179M",
    marketCapChange: -0.31,
    liquidity: "$3.46M",
    volume: "$637K",
    txns: "179K",
    txnsRatio: "965 / 842",
    tokenInfo: {
      holders: "87.6",
      supply: "6.965",
      age: "Unload"
    },
    percentageChanges: {
      h1: -1.09,
      h6: -63.4,
      h24: -0.26
    },
    tableData: [
      { hora: "14:23", tipo: "Buy", funding: "Long", wallet: "0x7a9f...c3b2", amount: "$2,350.00" },
      { hora: "14:21", tipo: "Sell", funding: "Short", wallet: "0x4d8e...f7a1", amount: "$1,850.75" },
      { hora: "14:19", tipo: "Buy", funding: "Long", wallet: "0x9c1b...e4d5", amount: "$5,200.40" }
    ]
  },
  {
    id: "2", 
    name: "project wings",
    symbol: "WINGS",
    icon: "🦅",
    verified: true,
    chartData: Array.from({ length: 20 }, (_, i) => ({ value: 80 + Math.cos(i * 0.2) * 15 })),
    chartColor: "#10b981",
    marketCap: "$123K",
    marketCapChange: -18.49,
    liquidity: "$22.2K",
    volume: "$246K",
    txns: "159K",
    txnsRatio: "955 / 900",
    tokenInfo: {
      holders: "2.40K",
      supply: "69.540",
      age: "Unload"
    },
    percentageChanges: {
      h1: -77.40,
      h6: -10.59,
      h24: -0.26
    },
    tableData: [
      { hora: "14:20", tipo: "Sell", funding: "Short", wallet: "0x2f5a...8b9c", amount: "$890.25" },
      { hora: "14:18", tipo: "Buy", funding: "Long", wallet: "0x6e3d...a2f7", amount: "$1,250.60" },
      { hora: "14:15", tipo: "Sell", funding: "Short", wallet: "0x8b4c...d1e9", amount: "$3,400.00" }
    ]
  },
  {
    id: "3",
    name: "koleexposure",
    symbol: "KOLE",
    icon: "⚖️",
    verified: true,
    chartData: Array.from({ length: 20 }, (_, i) => ({ value: 120 + Math.sin(i * 0.4) * 8 })),
    chartColor: "#10b981",
    marketCap: "$144K",
    marketCapChange: 4.80,
    liquidity: "$17.3K",
    volume: "$49.6K",
    txns: "95",
    txnsRatio: "51 / 44",
    tokenInfo: {
      holders: "84",
      supply: "44",
      age: "Unload"
    },
    percentageChanges: {
      h1: -77.40,
      h6: 0,
      h24: -0.26
    },
    tableData: [
      { hora: "14:22", tipo: "Buy", funding: "Long", wallet: "0x1a7b...f3c8", amount: "$750.80" },
      { hora: "14:17", tipo: "Buy", funding: "Long", wallet: "0x5d9e...c4a2", amount: "$2,100.50" },
      { hora: "14:14", tipo: "Sell", funding: "Short", wallet: "0x3c6f...b8e1", amount: "$1,680.25" }
    ]
  },
  {
    id: "4",
    name: "Pentagon Pizza",
    symbol: "PPW",
    icon: "🍕",
    verified: true,
    chartData: Array.from({ length: 20 }, (_, i) => ({ value: 90 + Math.sin(i * 0.5) * 12 })),
    chartColor: "#ef4444",
    marketCap: "$247K",
    marketCapChange: -31.6,
    liquidity: "$54.6K",
    volume: "$170K",
    txns: "833",
    txnsRatio: "465 / 368",
    tokenInfo: {
      holders: "835",
      supply: "382",
      age: "69.540"
    },
    percentageChanges: {
      h1: -19.13,
      h6: -9.93,
      h24: -0.34
    },
    tableData: [
      { hora: "14:16", tipo: "Sell", funding: "Short", wallet: "0x9e2a...d7f4", amount: "$1,920.75" },
      { hora: "14:13", tipo: "Buy", funding: "Long", wallet: "0x4b8c...e5a3", amount: "$850.40" },
      { hora: "14:11", tipo: "Sell", funding: "Short", wallet: "0x7f1d...c9b6", amount: "$4,150.90" }
    ]
  },
  {
    id: "5",
    name: "PolyNoob",
    symbol: "POLYNOOB",
    icon: "🎮",
    verified: true,
    chartData: Array.from({ length: 20 }, (_, i) => ({ value: 110 + Math.cos(i * 0.3) * 10 })),
    chartColor: "#10b981",
    marketCap: "$44.6K",
    marketCapChange: 49.4,
    liquidity: "$16.5K",
    volume: "$32K",
    txns: "177",
    txnsRatio: "91 / 86",
    tokenInfo: {
      holders: "93",
      supply: "59",
      age: "Unload"
    },
    percentageChanges: {
      h1: -27.26,
      h6: -1.80,
      h24: 0
    },
    tableData: [
      { hora: "14:25", tipo: "Buy", funding: "Long", wallet: "0x6a4e...f2b7", amount: "$680.30" },
      { hora: "14:12", tipo: "Buy", funding: "Long", wallet: "0x8c5f...a1d9", amount: "$2,750.60" },
      { hora: "14:09", tipo: "Sell", funding: "Short", wallet: "0x2d7b...e8c4", amount: "$1,340.85" }
    ]
  }
];

export function TokenFeed() {
  const [isFilterOpen, setIsFilterOpen] = useState(false);
  const [tokens, setTokens] = useState(mockTokens);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<{ minTotal: number | null; maxTotal: number | null }>({ minTotal: null, maxTotal: null });
  const [sortBy, setSortBy] = useState<SortBy>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [holdMins, setHoldMins] = useState<HoldMins>({ Global: 0, AG: 0, Dormant: 0, SNS: 0, Fresh: 0, AboveAVG: 0, Default: 0 });
  const [blacklist, setBlacklist] = useState<string[]>([]);

  // Persist/restore preferences (filter, sort, hold)
  useEffect(() => {
    try {
      const savedFilter = localStorage.getItem('feed.filter');
      if (savedFilter) setFilter(JSON.parse(savedFilter));
    } catch {}
    try {
      const savedSort = localStorage.getItem('feed.sort');
      if (savedSort) {
        const s = JSON.parse(savedSort);
        if (s.by !== undefined) setSortBy(s.by);
        if (s.dir) setSortDir(s.dir);
      }
    } catch {}
    try {
      const savedHold = localStorage.getItem('feed.holdMins');
      if (savedHold) {
        const parsed = JSON.parse(savedHold);
        // Back-compat: garantir 'Global'
        if (parsed && typeof parsed === 'object' && parsed.Global === undefined) parsed.Global = 0;
        setHoldMins(parsed);
      }
    } catch {}
    try {
      const bl = localStorage.getItem('feed.blacklist');
      if (bl) setBlacklist(JSON.parse(bl));
    } catch {}
  }, []);
  useEffect(() => { try { localStorage.setItem('feed.filter', JSON.stringify(filter)); } catch {} }, [filter]);
  useEffect(() => { try { localStorage.setItem('feed.sort', JSON.stringify({ by: sortBy, dir: sortDir })); } catch {} }, [sortBy, sortDir]);
  useEffect(() => { try { localStorage.setItem('feed.holdMins', JSON.stringify(holdMins)); } catch {} }, [holdMins]);
  useEffect(() => { try { localStorage.setItem('feed.blacklist', JSON.stringify(blacklist)); } catch {} }, [blacklist]);

  // Fetch initial cards
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const cards = await fetchMints(25, true);
        if (cancelled) return;
        const mapped = cards.map(mapMintCardToRowData);
        setTokens(mapped);
      } catch (e) {
        console.error('Failed to load mints', e);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // SSE for holding updates
  useEffect(() => {
    const es = new EventSource(`${API_URL}/feed/sse`);
    es.onmessage = (evt) => {
      try {
        const data = JSON.parse(evt.data);
        if (data && data.type === 'holding_update') {
          const ev = data as HoldingUpdateEvent;
          setTokens(prev => applyHoldingUpdate(prev, ev));
          // Se o token ainda não existe no feed, buscar e inserir
          setTokens(asyncPrev => {
            const exists = asyncPrev.some((t: any) => t.id === ev.mint);
            if (exists) return asyncPrev;
            fetchMintDetail(ev.mint).then(card => {
              setTokens(prev2 => upsertFromCard(prev2, card));
            }).catch(() => {});
            return asyncPrev;
          });
        }
        // Atualização de card (metadados / preço / liq)
        else if (data && data.type === 'mint_card_update' && data.mint) {
          const card = data as MintCard;
          setTokens(prev => upsertFromCard(prev, card));
        }
        // Evento de transação (sem type explícito): se trouxer mint, garantir presença no feed
        else if (data && data.mint && data.signature) {
          const mint: string = data.mint;
          setTokens(asyncPrev => {
            const exists = asyncPrev.some((t: any) => t.id === mint);
            if (exists) return asyncPrev;
            fetchMintDetail(mint).then(card => {
              setTokens(prev2 => upsertFromCard(prev2, card));
            }).catch(() => {});
            return asyncPrev;
          });
        }
      } catch {
        // ignore parse errors
      }
    };
    // Listen separately for cleanup events to prune tokens
    es.addEventListener('message', (evt: any) => {
      try {
        const data = JSON.parse((evt?.data ?? '') as any);
        if (data && data.type === 'mint_cleanup') {
          const list: string[] = Array.isArray(data.mints)
            ? data.mints
            : (data.mint ? [data.mint] : []);
          if (list.length) {
            setTokens(prev => prev.filter((t: any) => !list.includes(t.id)));
          }
        }
      } catch {}
    });
    es.onerror = () => { /* browser retries automatically */ };
    return () => { es.close(); };
  }, []);

  function handleBlacklist(mint: string) {
    setBlacklist(prev => {
      const set = new Set(prev || []);
      set.add(mint);
      return Array.from(set);
    });
    setTokens(prev => prev.filter((t: any) => t.id !== mint));
  }

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200">
      {/* Header */}
      <div className="flex items-center justify-between py-3 px-4 bg-gray-50 border-b border-gray-200 text-sm font-medium text-gray-600">
        <div>Tokens</div>
        <div className="flex items-center gap-2">
          {/* Sort pills: Time / Total */}
          <Button
            variant={sortBy === 'hora' ? 'default' : 'outline'}
            size="sm"
            className="rounded-full h-8 px-3"
            onClick={() => {
              if (sortBy === 'hora') {
                setSortDir(prev => (prev === 'asc' ? 'desc' : 'asc'));
              } else {
                setSortBy('hora');
                setSortDir('desc');
              }
            }}
          >
            <span className="mr-1">Time</span>
            <ChevronDown className={`h-4 w-4 transition-transform ${sortBy === 'hora' && sortDir === 'asc' ? 'rotate-180' : ''}`} />
          </Button>

          <Button
            variant={sortBy === 'total' ? 'default' : 'outline'}
            size="sm"
            className="rounded-full h-8 px-3"
            onClick={() => {
              if (sortBy === 'total') {
                setSortDir(prev => (prev === 'asc' ? 'desc' : 'asc'));
              } else {
                setSortBy('total');
                setSortDir('desc');
              }
            }}
          >
            <span className="mr-1">Total</span>
            <ChevronDown className={`h-4 w-4 transition-transform ${sortBy === 'total' && sortDir === 'asc' ? 'rotate-180' : ''}`} />
          </Button>

          <Button
            variant="outline"
            size="sm"
            onClick={() => setIsFilterOpen(true)}
            className="flex items-center gap-2"
          >
            <Filter className="h-4 w-4" />
            Filters
          </Button>
        </div>
      </div>

      {/* Token Rows */}
      <div>
        {(() => {
          const now = Date.now();
          const decorate = (t: any) => {
            const { filtered, raw } = computeTotals(t, holdMins, now);
            return { ...t, filteredTotal: filtered, rawTotal: raw, total: filtered, volume: String(filtered) };
          };
          const list = (loading ? mockTokens : tokens).map(decorate)
            .filter((t: any) => {
              const total = Number(t.total || 0);
              if (filter.minTotal != null && total < filter.minTotal) return false;
              if (filter.maxTotal != null && total > filter.maxTotal) return false;
              if (blacklist.includes(t.id)) return false;
              return true;
            })
            .sort((a: any, b: any) => {
              if (!sortBy) return 0;
              const dir = sortDir === 'asc' ? 1 : -1;
              if (sortBy === 'total') {
                const av = Number(a.total || 0);
                const bv = Number(b.total || 0);
                if (av === bv) return 0;
                return av < bv ? -1 * dir : 1 * dir;
              }
              // sortBy === 'hora' => ordenar pelo mais recente timestamp das wallets
              const aTs = Array.isArray(a.tableData) && a.tableData.length ? Math.max(...a.tableData.map((r: any) => Number(r.timestamp || 0))) : 0;
              const bTs = Array.isArray(b.tableData) && b.tableData.length ? Math.max(...b.tableData.map((r: any) => Number(r.timestamp || 0))) : 0;
              if (aTs === bTs) return 0;
              return aTs < bTs ? -1 * dir : 1 * dir;
            });
          return list.map((token: any) => (<TokenFeedRow key={token.id} token={token} onBlacklist={handleBlacklist} />));
        })()}
      </div>

      {/* Filter Popup */}
      <FilterPopup 
        isOpen={isFilterOpen}
        onClose={() => setIsFilterOpen(false)}
        value={filter}
        onApply={(v) => setFilter({ minTotal: v.minTotal ?? null, maxTotal: v.maxTotal ?? null })}
        onClear={() => setFilter({ minTotal: null, maxTotal: null })}
        sortBy={sortBy}
        sortDir={sortDir}
        onSort={(by, dir) => { setSortBy(by); setSortDir(dir); }}
        holdMins={holdMins}
        onHoldMinsChange={setHoldMins}
      />
    </div>
  );
}

// Compose funding label from origin + age
function formatFunding(origin?: string | null, age?: string | null) {
  const o = (origin && String(origin).trim()) || '';
  const a = (age && String(age).trim()) || '';
  if (o && a) return `${o} ${a}`;
  if (o) return o;
  if (a) return `UnknownCex ${a}`;
  return '';
}

// Map backend card to the UI row shape
function mapMintCardToRowData(card: MintCard) {
  const wallets = card.wallets || [];
  const total = wallets.reduce((s, w) => s + (Number(w.last_amount) || 0), 0);
  // Prefer Jupiter's value when available; otherwise compute approx by top 10 wallets
  let topHoldersPct: number | undefined = card.top_holders_percentage ?? undefined;
  try {
    if (topHoldersPct == null) {
      const sorted = [...wallets].sort((a, b) => (Number(b.last_amount || 0)) - (Number(a.last_amount || 0)));
      const n = Math.min(10, sorted.length);
      const topSum = sorted.slice(0, n).reduce((s, w) => s + (Number(w.last_amount) || 0), 0);
      topHoldersPct = total > 0 ? (topSum / total) * 100 : undefined;
    }
  } catch {}
  const type = card.type_label || getTypeFromSource(card.source_url || '');
  const tableData = wallets.map(w => ({
    // Guardamos timestamp para exibir "how old" na tabela
    timestamp: (w.last_seen || 0) * 1000,
    // Mantemos o horário absoluto para tooltip
    hora: new Date((w.last_seen || 0) * 1000).toLocaleTimeString(),
    // "Tipo" deve refletir a origem (AG/DORMANT/SNS/...)
    tipo: (() => {
      const tags = (w as any).type_tags as string[] | undefined;
      const t = (w as any).type_label || type || '';
      const label = tags && tags.length ? tags.join(' ') : t;
      return (label || '').toUpperCase();
    })(),
    funding: formatFunding((w as any).funding_origin, (w as any).fund_age_literal),
    wallet: w.wallet,
    amount: String(w.last_amount ?? 0),
  }));
  return {
    id: card.mint,
    name: card.token_name || '',
    symbol: card.token_symbol || card.mint.slice(0, 4),
    icon: '',
    iconUrl: card.token_icon || undefined,
    mint: card.mint,
    website: card.website || undefined,
    twitter: card.twitter || undefined,
    firstPoolCreatedAt: card.first_pool_created_at || null,
    typeLabel: type,
    verified: false,
    chartData: Array.from({ length: 20 }, () => ({ value: Number(card.usd_price || 0) })),
    chartColor: '#10b981',
    marketCap: String(card.mcap ?? 0),
    mcapNum: Number(card.mcap ?? 0),
    marketCapChange: 0,
    liquidity: String(card.liquidity ?? 0),
    volume: String(total),
    total,
    txns: String(card.holder_count ?? 0),
    txnsRatio: '',
    tokenInfo: { holders: String(card.holder_count ?? 0), supply: '', age: '' },
    topHoldersPct,
    devMigrations: card.dev_migrations ?? undefined,
    percentageChanges: { h1: 0, h6: 0, h24: 0 },
    tableData,
  };
}

// Insere/atualiza um token no feed a partir de um MintCard
function upsertFromCard(prev: any[], card: any) {
  const normalized: MintCard = {
    ...card,
    // Alguns eventos SSE vêm com 'fee_payers' ao invés de 'wallets'
    wallets: (card.wallets ?? card.fee_payers) || [],
  } as MintCard;
  const row = mapMintCardToRowData(normalized);
  const idx = prev.findIndex((t: any) => t.id === row.id);
  if (idx >= 0) {
    const next = prev.slice();
    next[idx] = { ...next[idx], ...row };
    return next;
  }
  return [row, ...prev];
}

function applyHoldingUpdate(prev: any[], ev: HoldingUpdateEvent) {
  return prev.map(t => {
    if (t.id !== ev.mint) return t;
    const next = { ...t };
    const idx = next.tableData.findIndex((r: any) => r.wallet === ev.wallet);
    const prevRow = idx >= 0 ? next.tableData[idx] : null;
    const row = {
      // Timestamp para cálculo relativo e horário absoluto para tooltip
      timestamp: ev.timestamp * 1000,
      hora: new Date(ev.timestamp * 1000).toLocaleTimeString(),
      // Mantemos "Tipo" como a origem do token (AG/DORMANT/SNS/...)
      tipo: (next.typeLabel || '').toUpperCase(),
      // Mantemos funding existente (origem + idade) — SSE no traz funding
      funding: prevRow?.funding || '',
      wallet: ev.wallet,
      amount: String(ev.last_amount),
    };
    if (idx >= 0) next.tableData[idx] = row; else next.tableData.unshift(row);
    const total = next.tableData.reduce((s: number, r: any) => s + (Number(r.amount) || 0), 0);
    next.volume = String(total);
    next.total = total;
    return next;
  });
}

function getTypeFromSource(sourceUrl: string): string | undefined {
  if (!sourceUrl) return undefined;
  if (sourceUrl.includes('channels/958046672473194556/1241009019494072370')) return 'AG';
  if (sourceUrl.includes('channels/1372291116853887077/1382098297891717252')) return 'Fresh';
  if (sourceUrl.includes('channels/1372291116853887077/1382099149842812988')) return 'Dormant';
  if (sourceUrl.includes('channels/1372291116853887077/1387731366212538390')) return 'SNS';
  if (sourceUrl.includes('channels/1372291116853887077/1387731454577872936')) return 'AboveAVG';
  return undefined;
}
