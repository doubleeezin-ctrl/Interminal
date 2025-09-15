// Base da API:
// - Em dev, pode usar VITE_API_URL (ex.: http://localhost:3000)
// - Em produção, ignoramos valores de localhost para usar a mesma origem
const RAW_API = (import.meta.env.VITE_API_URL ?? '').trim();
const IS_LOCAL_HOST = /^https?:\/\/(localhost|127\.0\.0\.1)(?::\d+)?(\/|$)/i.test(RAW_API);
const BASE_API = import.meta.env.PROD && IS_LOCAL_HOST ? '' : RAW_API;
export const API_URL = BASE_API.replace(/\/+$|^\/$/g, '');
async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_URL}${path}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export type MintWallet = {
  wallet: string;
  first_seen: number;
  last_seen: number;
  last_signature: string | null;
  last_slot: number | null;
  last_amount: number | null;
  sold_at?: number | null;
  txCount?: number;
  // Optional funding metadata per wallet (from backend v2)
  funding_origin?: string | null;
  fund_age_literal?: string | null;
  fund_age_seconds?: number | null;
  funding_signature?: string | null;
  type_label?: string | null;
  type_tags?: string[];
};

export type MintCard = {
  mint: string;
  type_label?: string | null;
  token_symbol: string | null;
  token_name: string | null;
  token_icon: string | null;
  usd_price: number | null;
  liquidity: number | null;
  mcap: number | null;
  holder_count: number | null;
  top_holders_percentage?: number | null;
  dev_migrations?: number | null;
  dev: string | null;
  launchpad: string | null;
  first_pool_created_at: number | null;
  twitter: string | null;
  website: string | null;
  last_signature: string | null;
  last_slot: number | null;
  last_timestamp: number | null;
  source_url?: string | null;
  wallets_count: number;
  wallets?: MintWallet[];
};

export async function fetchMints(limit = 100, includeWallets = true): Promise<MintCard[]> {
  const data = await getJson<{ count: number; data: MintCard[] }>(`/mints?limit=${limit}&includeWallets=${includeWallets}`);
  return data.data || [];
}

export async function fetchMintDetail(mint: string, limit = 200, offset = 0): Promise<MintCard> {
  return getJson<MintCard>(`/mints/${mint}?limit=${limit}&offset=${offset}`);
}

export type HoldingUpdateEvent = { type: 'holding_update'; mint: string; wallet: string; last_amount: number; timestamp: number };
