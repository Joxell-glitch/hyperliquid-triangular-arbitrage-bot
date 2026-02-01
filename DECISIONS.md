# DECISIONS — Registro Decisionale

Questo file contiene:
- Decisioni prese
- Decisioni scartate
- Errori già evitati

Serve a **evitare di ripercorrere strade già escluse**.

---

## Decisioni prese
- Continuità cognitiva gestita tramite file canonici (README, PROTOCOL, ROADMAP, DECISIONS)
- Cursor scelto come esecutore primario
- VM come ambiente operativo principale

### Nuove decisioni (Data Layer / Universe)
- Unità di scansione = mercato/variante (symbol_raw), non asset base
- Universe Raw Collector = fonte dati canonica (rolling 24h), da cui derivano strategie e metriche
- Sampling ibrido: Level A 250ms; resto 2s; promotion/demotion automatiche con hysteresis
- Anti-edge-fake: filtri su spread L1 per eleggibilità Level A

## Decisioni scartate
- Uso esclusivo di ChatGPT per sviluppo continuativo
- Affidarsi solo alla memoria conversazionale
- Salvare L2/trades raw per TUTTI i mercati in modo continuativo (troppo pesante): raw solo on-demand

## Errori già evitati
- Ripetizione di fix già applicati
- Refactor prematuri
- Ottimizzazione PnL prima di validazione logica
- Confondere “asset base” con “mercato/variante” (porta a carichi imprevedibili e ranking inutile)
