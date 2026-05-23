#!/usr/bin/env bash

set -euo pipefail

BASE_URL="${BASE_URL:-https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DIR="${1:-$ROOT/data/raw/pubmed/updatefiles}"
PARALLEL="${PARALLEL:-4}"
CURL_RETRY="${CURL_RETRY:-5}"

mkdir -p "$DEST_DIR"
DEST_DIR="$(cd "$DEST_DIR" && pwd)"

INDEX_HTML="$DEST_DIR/index.html"
README_TXT="$DEST_DIR/README.txt"
FILES_TXT="$DEST_DIR/files.txt"
MANIFEST_TXT="$DEST_DIR/local_manifest.txt"

curl_common_args=(
  --fail
  --location
  --retry "$CURL_RETRY"
  --retry-delay 2
  --connect-timeout 30
)

echo "[1/5] Downloading updatefiles index and README into $DEST_DIR"
curl "${curl_common_args[@]}" "$BASE_URL/" -o "$INDEX_HTML"
curl "${curl_common_args[@]}" "$BASE_URL/README.txt" -o "$README_TXT"

echo "[2/5] Extracting file list from index.html"
grep -Eo 'pubmed[0-9]{2}n[0-9]{4}\.xml\.gz(\.md5)?' "$INDEX_HTML" | \
  awk '!seen[$0]++' > "$FILES_TXT"

if [[ ! -s "$FILES_TXT" ]]; then
  echo "No PubMed update files were found in $INDEX_HTML" >&2
  exit 1
fi

xml_count="$(grep -Ec '\.xml\.gz$' "$FILES_TXT" || true)"
md5_count="$(grep -Ec '\.xml\.gz\.md5$' "$FILES_TXT" || true)"

echo "[3/5] Downloading $xml_count xml.gz files and $md5_count md5 files with PARALLEL=$PARALLEL"
xargs -P "$PARALLEL" -I{} bash -lc '
  set -euo pipefail
  file="$1"
  base_url="$2"
  dest_dir="$3"
  curl --fail --location --retry 5 --retry-delay 2 --connect-timeout 30 -C - \
    "$base_url/$file" -o "$dest_dir/$file"
' _ {} "$BASE_URL" "$DEST_DIR" < "$FILES_TXT"

echo "[4/5] Writing local manifest"
{
  echo "Source: $BASE_URL"
  echo "Downloaded at (UTC): $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "Destination: $DEST_DIR"
  echo "XML files: $(find "$DEST_DIR" -maxdepth 1 -name 'pubmed*.xml.gz' | wc -l | tr -d " ")"
  echo "MD5 files: $(find "$DEST_DIR" -maxdepth 1 -name 'pubmed*.xml.gz.md5' | wc -l | tr -d " ")"
  echo "NCBI README last-updated line:"
  grep -m1 '^Last Updated ' "$README_TXT" || true
  echo "Update family range:"
  find "$DEST_DIR" -maxdepth 1 -name 'pubmed*.xml.gz' -exec basename {} \; | sort | \
    awk 'NR==1{first=$0} {last=$0} END{print first " .. " last}'
} > "$MANIFEST_TXT"

echo "[5/5] Done"
echo "Saved files under: $DEST_DIR"
echo "Manifest: $MANIFEST_TXT"
echo "Tip: if you need a proxy, run the script from a shell where proxy env vars are already exported."
