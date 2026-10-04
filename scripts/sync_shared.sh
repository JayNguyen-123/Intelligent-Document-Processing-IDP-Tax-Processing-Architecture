#!/usr/bin/env bash
# Copies the single shared module into each Cloud Function source directory
# (gcloud deploys a directory, so it must be physically present). The copies are
# git-ignored; CI runs this before every deploy. The Dockerfile copies it itself.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cp "$ROOT/shared/tax_record.py" "$ROOT/cloud_functions/ingestion/tax_record.py"
echo "synced shared/tax_record.py -> cloud_functions/ingestion/"
