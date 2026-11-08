#!/usr/bin/env bash
# Downloads DuckDB-WASM from npm and bundles it (with its Apache Arrow dependency) into web/vendor, so the SQL page
# serves DuckDB itself instead of depending on a third-party CDN.
set -euo pipefail
VERSION="1.32.0"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
case "${1:-web/vendor}" in
  /*) DEST="$1" ;;
  *) DEST="$ROOT/${1:-web/vendor}" ;;
esac
mkdir -p "$DEST"
TMP="$(mktemp -d)"
cd "$TMP"
npm init -y > /dev/null
npm install --ignore-scripts --no-audit --no-fund "@duckdb/duckdb-wasm@${VERSION}" esbuild@0.25.12 > /dev/null
echo 'export * from "@duckdb/duckdb-wasm";' > entry.mjs
npx esbuild entry.mjs --bundle --format=esm --minify --log-level=warning --outfile="$DEST/duckdb.bundle.mjs"
for f in duckdb-eh.wasm duckdb-browser-eh.worker.js duckdb-mvp.wasm duckdb-browser-mvp.worker.js; do
  cp "node_modules/@duckdb/duckdb-wasm/dist/$f" "$DEST/"
done
ls -la "$DEST"
