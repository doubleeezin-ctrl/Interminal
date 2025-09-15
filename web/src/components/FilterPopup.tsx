import { useEffect, useState } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from './ui/dialog';
import { Button } from './ui/button';
import { X } from 'lucide-react';
import { Input } from './ui/input';

export type SortBy = 'total' | 'hora' | null;
export type HoldMins = { Global: number; AG: number; Dormant: number; SNS: number; Fresh: number; AboveAVG: number; Default: number };

interface FilterPopupProps {
  isOpen: boolean;
  onClose: () => void;
  // Filtro de visibilidade por faixa de Total (soma filtrada)
  value: { minTotal?: number | null; maxTotal?: number | null };
  onApply: (v: { minTotal?: number | null; maxTotal?: number | null }) => void;
  onClear?: () => void;
  // Ordenação
  sortBy?: SortBy;
  sortDir?: 'asc' | 'desc';
  onSort?: (by: SortBy, dir: 'asc' | 'desc') => void;
  // Mínimos de hold por tipo (minutos)
  holdMins: HoldMins;
  onHoldMinsChange: (v: HoldMins) => void;
}

export function FilterPopup({ isOpen, onClose, value, onApply, onClear, sortBy = null, sortDir = 'desc', onSort, holdMins, onHoldMinsChange }: FilterPopupProps) {
  const [minTotal, setMinTotal] = useState<string>('');
  const [maxTotal, setMaxTotal] = useState<string>('');
  const [localSortBy, setLocalSortBy] = useState<SortBy>(sortBy);
  const [localSortDir, setLocalSortDir] = useState<'asc' | 'desc'>(sortDir);
  const [localHold, setLocalHold] = useState<HoldMins>(holdMins);

  useEffect(() => {
    setMinTotal(value.minTotal != null ? String(value.minTotal) : '');
    setMaxTotal(value.maxTotal != null ? String(value.maxTotal) : '');
    setLocalSortBy(sortBy);
    setLocalSortDir(sortDir);
    setLocalHold(holdMins);
  }, [value.minTotal, value.maxTotal, isOpen, sortBy, sortDir, holdMins]);

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-md p-0 bg-white">
        <DialogHeader className="p-4 pb-2">
          <div className="flex items-center justify-between">
            <DialogTitle className="text-lg font-medium">Filters</DialogTitle>
            <Button
              variant="ghost"
              size="sm"
              onClick={onClose}
              className="h-8 w-8 p-0 hover:bg-gray-100"
            >
              <X className="h-5 w-5 text-gray-500" />
            </Button>
          </div>
        </DialogHeader>

        <div className="px-4 pb-4">
          {/* Visibility by Total */}
          <div className="space-y-3 mb-4">
            <div className="text-sm text-gray-600 font-medium">Visibility by Total</div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-xs text-gray-500">Min Total</label>
                <Input
                  type="number"
                  value={minTotal}
                  onChange={(e) => setMinTotal(e.target.value)}
                  placeholder="0"
                  className="mt-1"
                />
              </div>
              <div>
                <label className="text-xs text-gray-500">Max Total</label>
                <Input
                  type="number"
                  value={maxTotal}
                  onChange={(e) => setMaxTotal(e.target.value)}
                  placeholder=""
                  className="mt-1"
                />
              </div>
            </div>
          </div>

          {/* Divider */}
          <div className="h-px bg-gray-200 my-4" />

          {/* Min Hold by Type (minutes) */}
          <div className="space-y-3 mb-4">
            <div className="text-sm text-gray-600 font-medium">Min Hold (min) por tipo</div>
            <div className="grid grid-cols-1 gap-3">
              {/* Default only, per screenshot */}
              <div>
                <label className="text-xs text-gray-500">Default</label>
                <Input
                  type="number"
                  value={String((localHold as any).Default ?? 0)}
                  onChange={(e) => setLocalHold({ ...localHold, Default: Number(e.target.value || 0) } as HoldMins)}
                  placeholder="1"
                  className="mt-1"
                />
              </div>
            </div>
          </div>

          {/* Divider */}
          <div className="h-px bg-gray-200 my-4" />

          {/* Filter Settings grid for specific types */}
          <div className="space-y-3 mb-4">
            <div className="text-sm text-gray-600 font-medium">Filter Settings</div>
            <div className="grid grid-cols-2 gap-3">
              {(['Dormant','SNS','Fresh','AboveAVG','AG'] as const).map((key) => (
                <div key={key}>
                  <label className="text-xs text-gray-500">{key}</label>
                  <Input
                    type="number"
                    value={String((localHold as any)[key] ?? 0)}
                    onChange={(e) => setLocalHold({ ...localHold, [key]: Number(e.target.value || 0) } as HoldMins)}
                    placeholder={key === 'SNS' ? '5' : '1'}
                    className="mt-1"
                  />
                </div>
              ))}
            </div>
          </div>

          {/* Actions */}
          <div className="flex justify-between pt-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                setMinTotal('');
                setMaxTotal('');
                onClear?.();
                setLocalSortBy(null);
                setLocalSortDir('desc');
                onSort?.(null, 'desc');
                const cleared: HoldMins = { Global: (holdMins as any).Global ?? 0, AG: 0, Dormant: 0, SNS: 0, Fresh: 0, AboveAVG: 0, Default: 0 };
                setLocalHold(cleared);
                onHoldMinsChange(cleared);
              }}
            >
              Clear
            </Button>
            <Button
              size="sm"
              className="bg-black text-white hover:bg-black/90"
              onClick={() => {
                const min = minTotal === '' ? null : Number(minTotal);
                const max = maxTotal === '' ? null : Number(maxTotal);
                onApply({ minTotal: Number.isFinite(min as number) ? (min as number) : null, maxTotal: Number.isFinite(max as number) ? (max as number) : null });
                onSort?.(localSortBy, localSortDir);
                onHoldMinsChange(localHold);
                onClose();
              }}
            >
              Apply
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
