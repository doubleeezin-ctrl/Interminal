export type HoldMins = { [key: string]: number };

// Parser robusto: aceita números, k/m/b, moeda e separadores
function parseAmountLoose(v: unknown): number {
  if (typeof v === 'number') return v;
  const raw = String(v ?? '').trim().toLowerCase();
  const suffix = raw.match(/[kmb]$/)?.[0] as 'k' | 'm' | 'b' | undefined;
  const mul = suffix === 'k' ? 1_000 : suffix === 'm' ? 1_000_000 : suffix === 'b' ? 1_000_000_000 : 1;
  const numPart = raw.replace(/[kmb]$/,'').replace(/[^0-9.,-]/g,'');
  let normalized = numPart;
  if (numPart.includes(',') && numPart.includes('.')) {
    normalized = numPart.replace(/,/g, '');
  } else if (numPart.includes(',') && !numPart.includes('.')) {
    normalized = numPart.replace(/,/g, '.');
  }
  const n = Number(normalized);
  if (Number.isFinite(n)) return n * mul;
  const n2 = Number(raw);
  return Number.isFinite(n2) ? n2 : 0;
}

// Calcula o Total filtrado por mínimos de hold (global e por tipo)
export function computeFilteredTotal(token: any, hold: HoldMins, nowMs: number): number {
  const rows = Array.isArray(token?.tableData) ? token.tableData : [];
  let filteredSum = 0;

  const globalMin = Number((hold as any)?.Global || 0) || 0;
  const anyHoldActive = globalMin > 0 || Object.keys(hold || {}).some(k => k !== 'Global' && Number(hold[k] || 0) > 0);

  let rawSum = 0;
  for (const r of rows) {
    let amount = parseAmountLoose((r as any).amount);
    if (!Number.isFinite(amount) || amount <= 0) continue;
    rawSum += amount;
    if (!anyHoldActive) { filteredSum += amount; continue; }

    const type = String((r as any).tipo || 'Default').toUpperCase();
    const key =
      type === 'AG' ? 'AG' :
      type === 'DORMANT' ? 'Dormant' :
      type === 'SNS' ? 'SNS' :
      type === 'FRESH' ? 'Fresh' :
      type === 'ABOVEAVG' ? 'AboveAVG' :
      'Default';
    const minMinRaw = (hold && key in hold) ? hold[key] : (hold?.['Default'] ?? 0);
    const minMin = Math.max(globalMin, Number(minMinRaw) || 0);
    const firstMs = Number((r as any).timestamp || 0);
    const heldMin = firstMs ? (nowMs - firstMs) / 60000 : 0;
    if (heldMin >= minMin) {
      filteredSum += amount;
    }
  }

  return anyHoldActive ? (filteredSum || rawSum) : filteredSum;
}

// Retorna ambos: soma bruta e soma filtrada por hold (global + tipo)
export function computeTotals(token: any, hold: HoldMins, nowMs: number): { filtered: number; raw: number; anyHoldActive: boolean } {
  const rows = Array.isArray(token?.tableData) ? token.tableData : [];
  const globalMin = Number((hold as any)?.Global || 0) || 0;
  const anyHoldActive = globalMin > 0 || Object.keys(hold || {}).some(k => k !== 'Global' && Number(hold[k] || 0) > 0);

  let raw = 0;
  let filtered = 0;

  for (const r of rows) {
    let amount = parseAmountLoose((r as any).amount);
    if (!Number.isFinite(amount) || amount <= 0) continue;
    raw += amount;

    if (!anyHoldActive) { filtered += amount; continue; }

    const type = String((r as any).tipo || 'Default').toUpperCase();
    const key =
      type === 'AG' ? 'AG' :
      type === 'DORMANT' ? 'Dormant' :
      type === 'SNS' ? 'SNS' :
      type === 'FRESH' ? 'Fresh' :
      type === 'ABOVEAVG' ? 'AboveAVG' :
      'Default';
    const minMinRaw = (hold && key in hold) ? hold[key] : (hold?.['Default'] ?? 0);
    const minMin = Math.max(globalMin, Number(minMinRaw) || 0);
    const firstMs = Number((r as any).timestamp || 0);
    const heldMin = firstMs ? (nowMs - firstMs) / 60000 : 0;
    if (heldMin >= minMin) {
      filtered += amount;
    }
  }

  if (raw === 0 && Number(token?.total) > 0) {
    raw = Number(token.total);
    if (!anyHoldActive) filtered = raw;
  }

  return { filtered, raw, anyHoldActive };
}

