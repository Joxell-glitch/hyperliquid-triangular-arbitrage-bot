'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';

import { fetchStatus, startBot, stopBot, StatusSummary } from '@/lib/data';
import { IndicatorStatus, StatusIndicator } from './StatusIndicator';

export function SystemStatus() {
  const [status, setStatus] = useState<StatusSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionPending, setActionPending] = useState(false);

  const loadStatus = useCallback(async () => {
    try {
      const data = await fetchStatus();
      setStatus(data);
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Errore di connessione');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadStatus();
    const interval = setInterval(loadStatus, 5000);
    return () => clearInterval(interval);
  }, [loadStatus]);

  const statusIndicators = useMemo(() => {
    const botStatus: IndicatorStatus = loading ? 'unknown' : status?.botRunning ? 'ok' : 'error';
    const wsStatus: IndicatorStatus = loading ? 'unknown' : status?.websocketConnected ? 'ok' : 'error';
    const dashboardStatus: IndicatorStatus = loading ? 'unknown' : status?.dashboardConnected ? 'ok' : 'error';
    return { botStatus, wsStatus, dashboardStatus };
  }, [loading, status]);

  const handleStart = async () => {
    setActionPending(true);
    try {
      const data = await startBot();
      setStatus(data);
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Impossibile avviare il bot');
    } finally {
      setActionPending(false);
    }
  };

  const handleStop = async () => {
    setActionPending(true);
    try {
      const data = await stopBot();
      setStatus(data);
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Impossibile fermare il bot');
    } finally {
      setActionPending(false);
    }
  };

  return (
    <div className="panel" style={{ gridColumn: '1 / span 2' }}>
      <h2>Stato sistema</h2>
      <div className="status-section">
        <StatusIndicator
          label="Bot paper trading"
          status={statusIndicators.botStatus}
          description={loading ? 'Caricamento...' : status?.botRunning ? 'Running' : 'Fermo'}
        />
        <StatusIndicator
          label="WebSocket Hyperliquid"
          status={statusIndicators.wsStatus}
          description={loading ? 'Caricamento...' : status?.websocketConnected ? 'Connesso' : 'Disconnesso'}
        />
        <StatusIndicator
          label="Dashboard ↔ API"
          status={statusIndicators.dashboardStatus}
          description={loading ? 'Caricamento...' : status?.dashboardConnected ? 'Online' : 'Scollegata'}
        />
      </div>
      <div className="status-actions" style={{ gap: 8 }}>
        <button
          onClick={handleStart}
          disabled={actionPending}
          style={{
            background: 'var(--positive)',
            color: '#0a0f1f',
            border: 'none',
            borderRadius: 8,
            padding: '8px 12px',
            cursor: 'pointer',
            fontWeight: 700
          }}
        >
          {actionPending ? 'Attendere…' : 'Start Bot'}
        </button>
        <button
          onClick={handleStop}
          disabled={actionPending}
          style={{
            background: 'var(--negative)',
            color: '#0a0f1f',
            border: 'none',
            borderRadius: 8,
            padding: '8px 12px',
            cursor: 'pointer',
            fontWeight: 700
          }}
        >
          {actionPending ? 'Attendere…' : 'Stop Bot'}
        </button>
        {status?.lastHeartbeat && (
          <span style={{ color: 'var(--muted)', fontSize: 13 }}>
            Ultimo heartbeat: {new Date(status.lastHeartbeat * 1000).toLocaleString()}
          </span>
        )}
      </div>
      {error && <div className="error-text">{error}</div>}
    </div>
  );
}
