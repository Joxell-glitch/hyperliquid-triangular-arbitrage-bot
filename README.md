# Hyperliquid Edge Research Bot

Il sistema è **orientato alla ricerca** e **solo su carta**.
È progettato per misurare un **vantaggio reale e sostenibile** sotto vincoli realistici di microstruttura del mercato.

⚠️ **Nota di contesto (aggiornamento)**  
Il progetto nasce come “triangolare”, ma l’evoluzione attuale è focalizzata sulla **ricerca edge e sulla dislocazione Spot ↔ Perp** su Hyperliquid.  
Le componenti triangolari non sono rimosse, ma **non rappresentano il core operativo attuale**.

## Focus sul Profitto Netto (Definizione)

- **Vantaggio:** discrepanza di prezzo grezza prima dei costi
- **Commissioni:** commissioni maker/taker per ogni gamba (config + fallback tier)
- **Probabilità di esecuzione:** tasso di completamento atteso per ogni gamba
- **Slippage:** impatto di esecuzione aggiustato per profondità e latenza
- **Frequenza:** opportunità al giorno per mercato
- **KPI:**  
  **netto giornaliero** = (vantaggio − commissioni − slippage) × probabilità di esecuzione × frequenza

➡️ Questo principio resta valido ed è ora applicato **a livello di singolo mercato/variante**, non di “asset base”.

## Come Vengono Selezionati i Candidati

- Classificazione dei **mercati** (non asset) per:
  - edge potenziale
  - probabilità di esecuzione
  - frequenza
- Penalizzazione per spread, slippage e latenza
- Selezione dei candidati migliori per valutazione su carta
- Rivalutazione periodica tramite **Auto-Scan** per intercettare:
  - decadimento dell’edge
  - nuove opportunità emergenti

## Caratteristiche

- Architettura asincrona utilizzando `httpx` (REST) e `websockets` (Order Book)
- Configurazione basata su YAML (`config/config.yaml`) + variabili d'ambiente
- Supporto per Hyperliquid mainnet e testnet
- Persistenza SQLite (predefinita)
- CLI basata su Typer
- CI GitHub Actions che esegue pytest
- Valutazione Spot ↔ Perp (paper only)

## Data Layer (AGGIUNTO – stato attuale del progetto)

È in fase di implementazione un **Universe Raw Collector**, che diventa la **fonte dati canonica** del progetto.

Caratteristiche:
- Unità di scansione: **mercato / variante** (es. BTC/USDC spot, BTC-USDE perp)
- Copertura: **tutti i mercati Hyperliquid**, inclusi nuovi listaggi
- Rolling window: **ultime 24 ore** (cleanup FIFO)
- Dati salvati:
  - L1: bid / ask / mid
  - Contesto: volume 24h, funding, open interest (se disponibili)
- Sampling adattivo:
  - mercati prioritari → alta frequenza
  - resto dell’universo → bassa frequenza
  - promotion / demotion automatiche

Questo layer è progettato per:
- analisi offline
- validazione strategie
- rotazione dinamica dei mercati monitorati
- evitare di appesantire inutilmente il database

## Roadmap e Stato del Progetto

### Stato Attuale della Roadmap

- **A) Stabilità Runtime e Baseline** — COMPLETATO
- **B) Integrità Feed e Osservabilità** — COMPLETATO
- **C) Validazione Logica Strategia** — PAUSA
- **D) Dataset e Analisi Offline** — **FASE ATTIVA**
- **E) Rafforzamento e Controlli di Rischio** — BLOCCATO
- **F) Estensioni Future** — BLOCCATO

### Snapshot di Handoff (Stato Attuale)

- Fonte di verità: **VM**
- Branch `main` allineato con VM
- Test pytest passano
- Traccia Decisionale normalizzata (**READY / SKIP / HALT**)
- Nessuna ottimizzazione PnL live
- Focus attuale: **costruzione dataset robusto e leggero**

## Continuità e Contratto di Esecuzione

Questo README è il **documento canonico di contesto high-level**.

Cursor deve:
- Leggere questo file completamente prima di agire
- Trattarlo come un **contratto vincolante**
- Continuare il lavoro senza assunzioni esterne

La continuità operativa è garantita da:
- `README.md` — stato e visione del progetto
- `PROTOCOL.md` — regole di esecuzione e vincoli
- `ROADMAP.md` — fase attiva e micro-step
- `DECISIONS.md` — decisioni prese / evitate
- `SESSION_*.json` — log tecnici per sessione

Ruoli:
- **ChatGPT** → brainstorming, validazione, definizione prompt
- **Cursor** → esecutore sul filesystem VM
- **Umano** → autorità decisionale
- **GitHub** → persistenza (può essere in ritardo rispetto alla VM)

Principi:
- Preferire **soluzioni deterministiche**
- Evitare modifiche esplorative o speculative a meno che non siano esplicitamente richieste
- Se le informazioni sono insufficienti, **fermarsi e chiedere**

## Requisiti

- Python 3.8
- Accesso di rete alle API Hyperliquid (test offline tramite mock)

## Ambiente e Percorsi

- Repository GitHub: https://github.com/Joxell-glitch/hyperliquid-triangular-arbitrage-bot
- Percorso VM: `/home/ubuntu/hyperliquid-triangular-arbitrage-bot`