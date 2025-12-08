import Link from 'next/link';
import { fetchRuns, fetchTrades } from '@/lib/data';
import { MetricCard } from '../../components/MetricCard';
import { EquityChart } from '../../components/EquityChart';
import { PnlBarChart } from '../../components/PnlBarChart';
import { TradesTable } from '../../components/TradesTable';
import { LogsViewer } from '../../components/LogsViewer';

interface Params {
  params: { runId: string };
}

function formatDate(timestamp: number | null | undefined) {
  if (!timestamp) return '—';
  return new Date(timestamp * 1000).toLocaleString();
}

function formatSeconds(seconds: number | null | undefined) {
  if (seconds === null || seconds === undefined) return '—';
  const hours = seconds / 3600;
  return `${hours.toFixed(2)} h`;
}

export default async function RunPage({ params }: Params) {
  const [runs, trades] = await Promise.all([fetchRuns(), fetchTrades(params.runId)]);
  const run = runs.find((r) => r.runId === params.runId);
  const orderedTrades = trades.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));

  if (!run) {
    return (
      <div className="panel">
        <p>Run non trovata.</p>
        <Link href="/">Torna alla lista</Link>
      </div>
    );
  }

  let cumulative = 0;
  const equityCurve = orderedTrades.map((trade) => {
    cumulative += trade.pnl || 0;
    return { timestamp: trade.timestamp || 0, equity: cumulative };
  });
  const pnlSeries = orderedTrades.map((trade) => ({ timestamp: trade.timestamp || 0, pnl: trade.pnl || 0 }));

  const durationSeconds =
    run.startTimestamp && run.endTimestamp ? run.endTimestamp - run.startTimestamp : null;
  const winRate = orderedTrades.length
    ? orderedTrades.filter((t) => (t.pnl || 0) > 0).length / orderedTrades.length
    : 0;

  return (
    <div className="grid" style={{ gridTemplateColumns: '2fr 1fr', gap: 12 }}>
      <div style={{ gridColumn: '1 / span 2' }} className="panel">
        <Link href="/" style={{ color: 'var(--accent)' }}>
          ← Torna alle run
        </Link>
        <h2 style={{ marginTop: 12 }}>{run.runId}</h2>
        <div style={{ display: 'flex', gap: 8, marginTop: 8 }}>
          <span className={`badge ${run.status === 'running' ? 'active' : 'completed'}`}>{run.status}</span>
          <span className="badge">{orderedTrades.length} trade</span>
        </div>
      </div>

      <div className="grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gridColumn: '1 / span 2' }}>
        <MetricCard label="Inizio" value={formatDate(run.startTimestamp)} />
        <MetricCard label="Fine" value={formatDate(run.endTimestamp)} />
        <MetricCard label="Durata" value={formatSeconds(durationSeconds)} />
        <MetricCard label="Totale PnL" value={run.totalPnl.toFixed(5)} emphasis={run.totalPnl >= 0 ? 'positive' : 'negative'} />
        <MetricCard label="PnL medio" value={(run.totalPnl / Math.max(1, run.totalTrades)).toFixed(5)} />
        <MetricCard label="Win rate" value={`${(winRate * 100).toFixed(1)}%`} />
        <MetricCard
          label="Max drawdown"
          value={(() => {
            let peak = 0;
            let maxDrawdown = 0;
            for (const point of equityCurve) {
              peak = Math.max(peak, point.equity);
              maxDrawdown = Math.max(maxDrawdown, peak - point.equity);
            }
            return maxDrawdown.toFixed(5);
          })()}
          emphasis="negative"
        />
      </div>

      <div className="grid" style={{ gridTemplateColumns: '1fr 1fr', gridColumn: '1 / span 2', gap: 12 }}>
        <EquityChart data={equityCurve} />
        <PnlBarChart data={pnlSeries} />
      </div>

      <div style={{ gridColumn: '1 / span 2' }}>
        <TradesTable trades={orderedTrades} />
      </div>

      <div style={{ gridColumn: '1 / span 2' }}>
        <LogsViewer />
      </div>
    </div>
  );
}
