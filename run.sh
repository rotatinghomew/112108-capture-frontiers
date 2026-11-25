#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
SHIFT="${SHIFT:-g9}"
python3 pipeline.py --shift "$SHIFT" ingest data/exchanges.jsonl
python3 pipeline.py --shift "$SHIFT" status
