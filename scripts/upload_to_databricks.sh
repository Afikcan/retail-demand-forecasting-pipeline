#!/usr/bin/env bash
# Uploads the raw M5 CSVs from data/ to the Unity Catalog Volume that
# ingest/01_ingest.py reads from. Requires the Databricks CLI configured
# with a workspace profile (`databricks configure`).
set -euo pipefail

VOLUME_PATH="dbfs:/Volumes/workspace/retail_demand/raw"
DATA_DIR="$(dirname "$0")/../data"

for f in calendar.csv sell_prices.csv sales_train_evaluation.csv; do
  echo "uploading $f..."
  databricks fs cp "$DATA_DIR/$f" "$VOLUME_PATH/$f" --overwrite
done

echo "done — raw files are at $VOLUME_PATH"
