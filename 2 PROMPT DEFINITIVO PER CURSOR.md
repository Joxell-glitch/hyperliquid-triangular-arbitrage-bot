OBIETTIVO
Implementare un Universe Raw Collector adattivo per Hyperliquid che:
- acquisisce dati L1 + contesto (OI, funding, volumi)
- mantiene SOLO le ultime 24 ore (rolling FIFO)
- usa sampling ibrido per controllare costi e I/O
- classifica automaticamente i mercati e sposta il sampling dove serve

CONTESTO
VM: Oracle E5 Flex
- 1 OCPU
- 12 GB RAM
- DB locale
- batching obbligatorio

DEFINIZIONI
- “Mercato” = variante specifica (symbol_raw), es: BTC/USDC spot, BTC-USDE perp
- I livelli si applicano ai MERCATI, non agli asset base

====================================
1) DATABASE
====================================
Creare tabella MarketSample con campi:

- ts_ms (int, indexed)
- base (str)
- quote (str)
- market_type (SPOT | PERP)
- variant (str)
- symbol_raw (str, indexed)

L1:
- bid (float)
- ask (float)
- mid (float)
- spread_bps (float)

PERP ctx (nullable):
- mark_price (float)
- funding_rate (float)
- open_interest_usd (float)
- volume_24h_usd (float)

SPOT ctx (nullable):
- volume_24h_usd (float)

Meta:
- level (A|B|C|D)
- score (float)
- stale_flag (bool)

Cleanup FIFO:
- ogni 300s cancellare ts_ms < now - 24h

====================================
2) DATA SOURCES
====================================
Usare:
- /info type "spotMetaAndAssetCtxs"
- /info type "metaAndAssetCtxs" (OBBLIGATORIO per OI/funding)

Universe dinamico:
- integrare nuovi mercati automaticamente
- mantenere symbol_raw come ID primario

====================================
3) SAMPLING LEVELS
====================================
Livelli:
- Level A: top 100 mercati → 250 ms
- Level B: rank 101–200    → 2 s
- Level C: rank 201–300    → 2 s
- Level D: rank 301–400    → 2 s

====================================
4) RANKING SCORE
====================================
Ogni 60s ricalcolare ranking.

Metriche:
- volume_24h_usd
- open_interest_usd
- spread_bps

Normalizzazione:
- percentile_rank su ciascuna metrica

Score:
score = 0.6 * volume_norm
      + 0.3 * oi_norm
      - 0.1 * spread_norm

====================================
5) SPREAD FILTER (ANTI EDGE FAKE)
====================================
- spread > 30 bps → mercato ignorato
- spread > 15 bps → non eleggibile per Level A

====================================
6) PROMOTION / DEMOTION
====================================
Promotion a Level A:
- rank <= 100
- per 3 refresh consecutivi
- spread <= 15 bps

Demotion da Level A:
- rank > 120 per 3 refresh
  OR spread > 20 bps

Usare hysteresis per evitare churn.

====================================
7) FALLBACK SAFETY
====================================
Se ranking incompleto o dati mancanti:
- includere SEMPRE tutte le varianti di BTC, ETH, SOL
- minimo Level B (2s)
- MAI forzare Level A

====================================
8) BATCHING & PERFORMANCE
====================================
- Buffer in RAM
- Insert in batch ≥ 1 transazione / secondo
- MAI insert per singola riga

====================================
9) OUTPUT DI STATO
====================================
Scrivere file:
data/universe_status.json
data/universe_levels.json

Contenuto:
- timestamp
- mercati totali
- mercati per livello
- promotion/demotion avvenute
- insert/sec medi
- db_rows_24h

====================================
10) CLI
====================================
Aggiungere comando:
run-universe-collector

Argomenti:
- --sample-levels-config
- --ranking-refresh-sec (default 60)
- --cleanup-sec (default 300)
- --duration-sec (opzionale)

====================================
VINCOLO FINALE
====================================
NON modificare scanner esistenti.
Questo collector è una base dati universale per:
- analisi offline
- backtest ciclico
- allocazione dinamica del capitale
