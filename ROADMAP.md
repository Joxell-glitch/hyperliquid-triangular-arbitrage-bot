# ROADMAP — Stato Progetto

## Fasi

A) Runtime Stability & Baseline — COMPLETATA  
B) Feed Integrity & Observability — COMPLETATA  
C) Strategy Logic Validation — PAUSA / NON ATTIVA 
D) Dataset & Offline Analysis — **OPEN** 
-  D.0 — Implement Universe Raw Collector (rolling 24h + sampling ibrido)
-  D.1 — Verifica performance (batching, insert/sec, crescita DB 24h) 
-  D.2 — Produce first 24h dataset export (SQLite/CSV) + report Dataset
-  D.3 — Innescare loop: analisi offline → metriche → implementazione strategia → re-validazione
E) Hardening & Risk Controls — BLOCCATA  
F) Future Extensions — BLOCCATA  

## Fase attiva
**C) Strategy Logic Validation**

## Micro-step attuale
D.0 — Implementare Universe Raw Collector (rolling 24h + sampling ibrido)

⚠️ È consentita **una sola fase OPEN** alla volta.
