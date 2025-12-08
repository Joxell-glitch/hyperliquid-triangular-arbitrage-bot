# Hyperliquid Paper Trading Dashboard

Dashboard Next.js per analizzare le run di paper trading del bot di arbitraggio triangolare. Tutti i dati arrivano da un backend FastAPI remoto (nessun accesso diretto a file o SQLite).

## Requisiti
- Node.js 18+
- Endpoint FastAPI raggiungibile (default `http://localhost:8000`)

## Avvio locale
```bash
cd web-dashboard
npm install
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000 npm run dev
```
La dashboard si connette sempre alle API remota indicate da `NEXT_PUBLIC_API_BASE_URL`.

## Deploy su Vercel
- Seleziona la cartella `web-dashboard` come root del progetto.
- Usa la build command predefinita (`npm run build`).
- Imposta la variabile d'ambiente `NEXT_PUBLIC_API_BASE_URL` al tuo endpoint FastAPI pubblico.

## API consumate
- `GET /api/status` – stato bot, WebSocket, dashboard e heartbeat.
- `POST /api/start` / `POST /api/stop` – abilita/disabilita il bot.
- `GET /api/runs` – elenco run con metriche aggregate.
- `GET /api/trades?run_id=...` – trade filtrati per run.
- `GET /api/logs` – ultime 500 righe di log.
