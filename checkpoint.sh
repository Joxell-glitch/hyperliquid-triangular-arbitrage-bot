#!/usr/bin/env bash
set -e

# Verifica di essere in una repo Git
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "❌ Non sei in una repo Git"
  exit 1
fi

echo "📌 Stato attuale:"
git status --short

# Se non ci sono cambiamenti, esci
if [ -z "$(git status --porcelain)" ]; then
  echo "✅ Nessuna modifica da salvare"
  exit 0
fi

TS=$(date '+%Y-%m-%d %H:%M')

git add .
git commit -m "Checkpoint ${TS}"
git push

echo "✅ Checkpoint completato (${TS})"
