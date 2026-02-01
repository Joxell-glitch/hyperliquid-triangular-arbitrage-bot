# OPS_COMMANDS – Hyperliquid Edge Research Bot

Runbook operativo.
Elenco dei comandi da ricordare durante sviluppo, run e analisi.
Aggiornare questo file man mano che il progetto evolve.

---


# 1) Allineamento VM → GitHub (checkpoint manuale)
# Salva lo stato corrente della repo (git add + commit con timestamp + push).
# Usare dopo modifiche di codice o file di progetto.
./checkpoint_git.sh
checkpoint

# 2) Export dataset dopo almeno 24 ore di run
# Esporta snapshot delle ultime 24h effettive dal DB FIFO.
# Se <24h → NOT READY (non esporta). Se ≥24h → crea CSV + report.
PYTHONPATH=. python3 src/cli.py export-universe-snapshot