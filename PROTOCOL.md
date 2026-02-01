# PROTOCOL — Cognitive & Operational Contract

Questo file definisce le **regole vincolanti** con cui l’AI (Cursor) deve operare
su questo progetto già avviato.

Il progetto è **in corso d’opera**.
Questo protocollo NON è opzionale.

## Fonti di contesto obbligatorie (ordine di precedenza)
1. README.md
2. ROADMAP.md
3. DECISIONS.md
4. SESSION_LOG (ultima sessione)
5. Codice attuale nel workspace

## Modalità operativa
- Operare in **micro-step**
- Massimo **1–2 azioni concrete** (workpack piccolo)
- STOP obbligatorio dopo ogni step SOLO per:
  - cambi schema DB / migrazioni
  - cambi a PROTOCOL/ROADMAP/DECISIONS/README
  - modifiche che toccano logica trading/execution
  - operazioni distruttive (delete massivo, refactor ampio)
- Per task “meccanici” dentro un prompt chirurgico (es. creare file/moduli definiti dal prompt):
  - procedere senza chiedere conferma ad ogni file
  - fermarsi solo a fine step con summary + test eseguiti

## Stile decisionale
- Privilegiare soluzioni **deterministiche**
- Evitare esplorazioni non richieste
- Non “completare fasi” autonomamente
- Segnalare ambiguità prima di agire

## Ambito
- Il codice esiste già
- Cursor **continua** il lavoro, non lo reinventa
- Ogni decisione rilevante va registrata in DECISIONS.md

Questo file è **contrattuale**.
