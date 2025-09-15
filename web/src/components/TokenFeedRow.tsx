import { useMemo, useState } from "react";
import { LineChart, Line, ResponsiveContainer } from "recharts";
import { Button } from "./ui/button";
import { Badge } from "./ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table";

import { ExternalLink, Copy, Trophy, X as XIcon } from "lucide-react";

interface TokenData {
  id: string;
  name: string;
  symbol: string;
  icon?: string;
  iconUrl?: string;
  mint?: string;
  website?: string;
  twitter?: string;
  firstPoolCreatedAt?: number | string | null;
  typeLabel?: string;
  verified: boolean;
  chartData: Array<{ value: number }>;
  chartColor: string;
  marketCap: string;
  marketCapChange: number;
  liquidity: string;
  volume: string;
  txns: string;
  txnsRatio: string;
  mcapNum?: number;
  total?: number;
  filteredTotal?: number;
  rawTotal?: number;
  topHoldersPct?: number;
  devMigrations?: number;
  tokenInfo: {
    holders: string;
    supply: string;
    age: string;
  };
  percentageChanges: {
    h1: number;
    h6: number;
    h24: number;
  };
  tableData: Array<{
    // horário absoluto (para tooltip)
    hora?: string;
    // timestamp em ms para exibir relativo (how old)
    timestamp?: number;
    tipo: string;
    funding: string;
    wallet: string;
    amount: string;
  }>;
}

interface TokenFeedRowProps {
  token: TokenData;
  onBlacklist?: (mint: string) => void;
}

export function TokenFeedRow({ token, onBlacklist }: TokenFeedRowProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const PAGE = 10;
  const [visibleCount, setVisibleCount] = useState(PAGE);
  const ageText = useMemo(() => formatRelativeTime(token.firstPoolCreatedAt), [token.firstPoolCreatedAt]);

  return (
    <div className="border-b border-gray-100">
      {/* Main Token Row */}
      <div
        className="relative flex items-start justify-between py-3 px-4 hover:bg-gray-50 border border-red-500 rounded-2xl bg-white cursor-pointer"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        {onBlacklist && token.mint && (
          <button
            title="Ocultar este token do feed"
            className="absolute top-1.5 right-1.5 rounded-full p-1 hover:bg-gray-100 text-gray-500"
            onClick={(e) => { e.stopPropagation(); onBlacklist?.(token.mint!); }}
          >
            <XIcon className="w-4 h-4" />
          </button>
        )}
        {/* Left side - Token Info */}
        <div className="flex items-start gap-3 flex-1">
          <div className="relative">
            {token.iconUrl ? (
              <img src={token.iconUrl} alt={token.symbol} className="w-12 h-12 rounded-full object-cover" />
            ) : (
              <div className="w-12 h-12 rounded-full bg-gradient-to-br from-yellow-400 to-orange-500 flex items-center justify-center text-[36px]">
                <span className="text-white text-sm font-medium">{token.symbol?.charAt(0) || "?"}</span>
              </div>
            )}
            {token.verified && (
              <div className="absolute -top-1 -right-1 w-3 h-3 bg-blue-500 rounded-full flex items-center justify-center">
                <span className="text-white text-xs">✓</span>
              </div>
            )}
          </div>
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-1">
              <span className="font-medium text-sm">
                {token.symbol}
              </span>
              <button
                className="text-gray-500 text-sm hover:text-gray-700"
                title="Clique para copiar o mint"
                onClick={(e) => { e.stopPropagation(); if (token.mint) navigator.clipboard.writeText(token.mint); }}
              >
                {token.name}
              </button>
              {token.typeLabel && (
                <span className="ml-2 text-[10px] px-1.5 py-0.5 rounded bg-gray-100 text-gray-600 border border-gray-200" title="Tipo pela origem">{token.typeLabel}</span>
              )}
              {token.mint && (
                <span
                  className="ml-1 inline-flex items-center gap-1 rounded-full border border-gray-200 bg-gray-50 px-2 py-0.5 text-[10px] text-gray-600"
                  title={token.mint}
                  onClick={(e)=>e.stopPropagation()}
                >
                  <Trophy className="w-3 h-3 text-amber-500" />
                  {typeof token.devMigrations === 'number' ? (
                    <span className="text-amber-600 font-medium">{token.devMigrations}</span>
                  ) : null}
                  {shortAddr(token.mint)}
                </span>
              )}
              {false && (
                <span className="text-blue-500 text-xs">
                  📋
                </span>
              )}
            </div>
            {/* Mapped metadata from backend */}
            <div className="flex items-center gap-3 text-xs text-gray-500 mt-1">
              {ageText && <span title="first_pool_created_at">{ageText}</span>}
              {/* 🌐 Website */}
              {token.website && (
                <a href={token.website} target="_blank" rel="noreferrer" className="hover:text-gray-700" title="Website" onClick={(e)=>e.stopPropagation()}>
                  🌐
                </a>
              )}
              {/* 𝕏 Twitter */}
              {token.twitter && (
                <a href={token.twitter} target="_blank" rel="noreferrer" className="hover:text-gray-700" title="Twitter" onClick={(e)=>e.stopPropagation()}>
                  𝕏
                </a>
              )}
              {/* 🔍 Search by mint */}
              {token.mint && (
                <a href={`https://x.com/search?q=${encodeURIComponent(token.mint)}&src=typed_query&f=live`} target="_blank" rel="noreferrer" className="hover:text-gray-700" title="Search on X" onClick={(e)=>e.stopPropagation()}>
                  🔍
                </a>
              )}
              {/* 🦕 GMGN token */}
              {token.mint && (
                <a href={`https://gmgn.ai/sol/token/Hs2WZmHW_${encodeURIComponent(token.mint)}`} target="_blank" rel="noreferrer" className="hover:text-gray-700" title="GMGN token" onClick={(e)=>e.stopPropagation()}>
                  🦕
                </a>
              )}
              {/* Holders count */}
              {token?.tokenInfo?.holders && (
                <span className="inline-flex items-center gap-1 rounded-md border border-gray-200 bg-white px-1.5 py-0.5 text-[10px] text-gray-700" title="Holders">
                  <span className="text-gray-500">👥</span>
                  {token.tokenInfo.holders}
                </span>
              )}
              {/* Top holders % */}
              {typeof token.topHoldersPct === 'number' && (
                <span className="inline-flex items-center gap-1 rounded-md border border-purple-200 bg-purple-50 px-1.5 py-0.5 text-[10px] text-purple-700" title="Top holders % (top10)">
                  {token.topHoldersPct.toFixed(2)}%
                </span>
              )}
            </div>
            <div className="hidden">
              <span>54m</span>
              <span>🌐</span>
              <span className="font-bold">𝕏</span>
              <span>🔍</span>
              <span>🦕</span>
              <div className="flex items-center gap-1">
                <span>👥</span>
                <span>{token.tokenInfo.holders}</span>
              </div>
              <div className="flex items-center gap-1">
                <span className="font-medium text-blue-600">IH</span>
                <span className="font-medium text-blue-600">87%</span>
              </div>
            </div>
          </div>
        </div>

        {/* Right side - MC and Total stacked (target UI) */}
        <div className="flex flex-col items-end gap-1 min-w-[140px]">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-gray-600">MC</span>
            <span className="font-semibold text-orange-500">
              {formatK((token.mcapNum ?? Number(token.marketCap)) || 0)}
            </span>
            <span className={`text-gray-600 transition-transform ${isExpanded ? 'rotate-180' : ''}`}>▼</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-gray-600">Total</span>
            <span className="font-medium text-blue-600">
              {formatMillions(Number((token as any).filteredTotal ?? token.total ?? 0))}
            </span>
          </div>
        </div>

        {/* Expand/Collapse Icon (kept for click target, hidden to match UI) */}
        <div className="ml-4 mt-1 hidden">
          <span
            className={`transition-transform duration-200 ${isExpanded ? "rotate-180" : "rotate-0"}`}
          >
            ▼
          </span>
        </div>
      </div>

      {/* Token Details Table - Collapsible */}
      {isExpanded && (
        <div className="px-4 pb-4 bg-gray-50/50 animate-in slide-in-from-top-2 duration-200">
          <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="bg-gray-50">
                  <TableHead className="font-medium text-gray-700 py-2">
                    Hora
                  </TableHead>
                  <TableHead className="font-medium text-gray-700 py-2">
                    Tipo
                  </TableHead>
                  <TableHead className="font-medium text-gray-700 py-2">
                    Funding
                  </TableHead>
                  <TableHead className="font-medium text-gray-700 py-2">
                    Wallet
                  </TableHead>
                  <TableHead className="font-medium text-gray-700 py-2">
                    Amount
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {token.tableData.slice(0, visibleCount).map((row, index) => (
                  <TableRow
                    key={index}
                    className="hover:bg-gray-50"
                  >
                    <TableCell className="py-2 text-sm text-gray-600" title={row.hora || (row.timestamp ? new Date(row.timestamp).toLocaleTimeString() : undefined)}>
                      {row.timestamp ? formatRelativeTime(row.timestamp) : (row.hora || "")}
                    </TableCell>
                    <TableCell className="py-2 text-sm text-gray-600">
                      {row.tipo}
                    </TableCell>
                    <TableCell className="py-2 text-sm font-medium">
                      <span
                        className={`px-2 py-1 rounded text-xs ${
                          row.funding === "Long"
                            ? "text-green-600 bg-green-50"
                            : row.funding === "Short"
                              ? "text-red-500 bg-red-50"
                              : "text-gray-600 bg-gray-50"
                        }`}
                      >
                        {row.funding}
                      </span>
                    </TableCell>
                    <TableCell className="py-2 text-sm font-mono text-blue-600 underline">
                      <a
                        href={`https://gmgn.ai/sol/address/Hs2WZmHW_${encodeURIComponent(row.wallet)}`}
                        target="_blank"
                        rel="noreferrer"
                        onClick={(e)=>e.stopPropagation()}
                      >
                        {row.wallet}
                      </a>
                    </TableCell>
                    <TableCell className="py-2 text-sm font-medium">
                      {formatMillions(Number(row.amount) || 0)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          {/* Ver mais / Ver menos */}
          {token.tableData.length > 10 && (
            <div className="flex justify-end mt-2">
              {visibleCount < token.tableData.length ? (
                <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); setVisibleCount(Math.min(visibleCount + PAGE, token.tableData.length)); }}>
                  Ver mais
                </Button>
              ) : (
                <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); setVisibleCount(PAGE); }}>
                  Ver menos
                </Button>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function shortAddr(a?: string) {
  if (!a) return '';
  if (a.length <= 10) return a;
  return `${a.slice(0,4)}…${a.slice(-4)}`;
}

function formatRelativeTime(ts?: number | string | null) {
  if (ts === undefined || ts === null) return '';
  let ms: number | null = null;
  if (typeof ts === 'number') {
    ms = ts > 1e12 ? ts : ts * 1000;
  } else {
    const n = Number(ts);
    if (Number.isFinite(n)) ms = n > 1e12 ? n : n * 1000; else ms = Date.parse(ts);
  }
  if (!ms || !Number.isFinite(ms)) return '';
  const diff = Date.now() - ms;
  const m = Math.floor(diff / 60000);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h`;
  const d = Math.floor(h / 24);
  return `${d}d`;
}

function formatK(n: number) {
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `${(n/1_000_000).toFixed(1)}m`;
  if (abs >= 1_000) return `${(n/1_000).toFixed(1)}k`;
  return String(Math.round(n));
}

function formatMillions(n: number) {
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `${Math.round(n / 1_000_000)}M`;
  // Mantém valores menores sem sufixo; pode-se ajustar para 'k' se desejar
  return String(Math.round(n));
}


