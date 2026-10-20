#!/usr/bin/env bash
# Download FAERS quarterly ASCII extracts and lay them out for Spark:
#   data/raw/{demo,drug,reac,deleted}/<YYYYQn>.txt
#
# Usage: scripts/download.sh 2024Q1 2026Q2
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <start YYYYQn> <end YYYYQn>   e.g. $0 2024Q1 2026Q2" >&2
  exit 1
fi

start=$(echo "$1" | tr '[:lower:]' '[:upper:]')
end=$(echo "$2" | tr '[:lower:]' '[:upper:]')
for q in "$start" "$end"; do
  [[ "$q" =~ ^[0-9]{4}Q[1-4]$ ]] || { echo "bad quarter: $q" >&2; exit 1; }
done
# Column layout is stable from 2014Q3 onward; earlier files have different headers.
if [[ "${start%Q*}${start#*Q}" < "20143" ]]; then
  echo "start must be 2014Q3 or later (file layout changed)" >&2
  exit 1
fi

root="$(cd "$(dirname "$0")/.." && pwd)/data"
mkdir -p "$root/zips" "$root/raw"/{demo,drug,reac,deleted}

year=${start%Q*}; qtr=${start#*Q}
while [[ "$year$qtr" < "${end%Q*}${end#*Q}" || "$year$qtr" == "${end%Q*}${end#*Q}" ]]; do
  label="${year}Q${qtr}"
  zip="$root/zips/faers_ascii_${year}q${qtr}.zip"
  if [[ ! -s "$zip" ]]; then
    echo "downloading $label"
    curl -fsSL --retry 3 -o "$zip.part" "https://fis.fda.gov/content/Exports/faers_ascii_${year}q${qtr}.zip"
    mv "$zip.part" "$zip"
  fi

  # Member names vary in case across years (ASCII/DEMO24Q1.txt vs ascii/demo24q1.txt).
  members=$(unzip -Z1 "$zip")
  for table in demo drug reac; do
    member=$(echo "$members" | grep -iE "(^|/)${table}[0-9]{2}q[1-4]\.txt$" | head -1 || true)
    [[ -n "$member" ]] || { echo "missing $table in $zip" >&2; exit 1; }
    unzip -p "$zip" "$member" > "$root/raw/$table/$label.txt"
  done
  deleted=$(echo "$members" | grep -iE "deleted?/.*\.txt$" | head -1 || true)
  if [[ -n "$deleted" ]]; then
    unzip -p "$zip" "$deleted" > "$root/raw/deleted/$label.txt"
  fi
  echo "extracted $label"

  if [[ $qtr -eq 4 ]]; then year=$((year + 1)); qtr=1; else qtr=$((qtr + 1)); fi
done
