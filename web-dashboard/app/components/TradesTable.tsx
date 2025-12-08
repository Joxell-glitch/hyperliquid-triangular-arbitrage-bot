'use client';

import { useMemo, useState } from 'react';
import { Trade } from '@/lib/data';

interface Props {
  trades: Trade[];
}

function formatPair(trade: Trade) {
  return trade.pairPath || 'N/D';
}

export function TradesTable({ trades }: Props) {
  const [pairFilter, setPairFilter] = useState('');
  const [pnlFilter, setPnlFilter] = useState('all');

  const filtered = useMemo(() => {
    return trades.filter((trade) => {
      const matchesPair = pairFilter
        ? formatPair(trade).toLowerCase().includes(pairFilter.toLowerCase())
        : true;
      const matchesPnl =
        pnlFilter === 'positive'
          ? (trade.pnl || 0) > 0
          : pnlFilter === 'negative'
          ? (trade.pnl || 0) < 0
          : true;
      return matchesPair && matchesPnl;
    });
  }, [pairFilter, pnlFilter, trades]);

  return (
    <div className="panel">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h3 className="section-title">Trade dettagliati</h3>
        <div className="controls">
          <input placeholder="Filtra per coppia" value={pairFilter} onChange={(e) => setPairFilter(e.target.value)} />
          <select value={pnlFilter} onChange={(e) => setPnlFilter(e.target.value)}>
            <option value="all">Tutti</option>
            <option value="positive">PnL positivo</option>
            <option value="negative">PnL negativo</option>
          </select>
        </div>
      </div>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Coppia</th>
              <th>Size</th>
              <th>Prezzo ingresso</th>
              <th>Prezzo uscita</th>
              <th>PnL</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((trade) => (
              <tr key={trade.id}>
                <td>{trade.timestamp ? new Date(trade.timestamp * 1000).toLocaleString() : '—'}</td>
                <td>{formatPair(trade)}</td>
                <td>{trade.size !== null && trade.size !== undefined ? trade.size.toFixed(4) : '—'}</td>
                <td>{trade.entryPrice !== null && trade.entryPrice !== undefined ? trade.entryPrice.toFixed(6) : '—'}</td>
                <td>{trade.exitPrice !== null && trade.exitPrice !== undefined ? trade.exitPrice.toFixed(6) : '—'}</td>
                <td className={(trade.pnl || 0) >= 0 ? 'positive' : 'negative'}>
                  {trade.pnl !== null && trade.pnl !== undefined ? trade.pnl.toFixed(5) : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
