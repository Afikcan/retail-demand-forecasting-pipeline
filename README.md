# Retail Demand Forecasting Pipeline

An end-to-end demand forecasting pipeline for retail sales data, built on
PySpark and Delta Lake, orchestrated and run on Databricks.

## Problem

Forecast store/department-level demand from historical retail sales, using
calendar/holiday and pricing context, and evaluate against a naive baseline.

## Data

[M5 Forecasting Accuracy](https://www.kaggle.com/competitions/m5-forecasting-accuracy) —
Walmart store × item × date sales (~30k series, 2011–2016), with pricing and
calendar/holiday event data. Published by the Makridakis Open Forecasting
Center as part of the M5 competition.

> Makridakis, S., Spiliotis, E., & Assimakopoulos, V. (2020).
> *The M5 competition: Background, organization, and implementation.*
> International Journal of Forecasting.

Raw files are not committed (see `.gitignore`) — download from Kaggle and
place under `data/`:

- `calendar.csv`
- `sell_prices.csv`
- `sales_train_evaluation.csv`

## Pipeline

```
data/ (raw CSVs)
  -> upload to Databricks (scripts/upload_to_databricks.sh)
  -> ingest/01_ingest.py                  load raw CSVs into Databricks (bronze)
  -> ingest/02_clean_join.py              clean, join, write Delta (silver)
  -> features/03_feature_engineering.py   rolling/lag/holiday/price features (gold)
  -> model/04_baseline.py                 seasonal-naive baseline
  -> model/05_train.py                    LightGBM per store/department
  -> model/06_evaluate.py                 forecast vs. actual, WMAPE, chart
```

Runs on **Databricks**, orchestrated as a single [Databricks Asset Bundle](https://docs.databricks.com/en/dev-tools/bundles/index.html)
job ([`databricks.yml`](databricks.yml), [`resources/retail_demand_job.yml`](resources/retail_demand_job.yml))
with explicit task dependencies — `baseline` and `train` run in parallel once
`feature_engineering` finishes, `evaluate` waits on both. Scheduled daily
(paused by default — flip `pause_status` to `UNPAUSED` to activate), with
email alerts and an automatic retry on task failure. All paths point at a
Unity Catalog Volume:
`/Volumes/workspace/retail_demand/{raw,bronze,silver,gold,predictions}`.

## How to run

1. Download the M5 dataset from Kaggle into `data/`.
2. Configure the Databricks CLI (`databricks configure`) and run
   `scripts/upload_to_databricks.sh` to push the raw CSVs into the
   `raw` Volume.
3. Deploy the bundle: `databricks bundle deploy --target dev`.
4. Run the full pipeline: `databricks bundle run retail_demand_pipeline --target dev`.

## Testing

Unit tests cover the pure-logic pieces of the pipeline — sales unpivoting,
lag/rolling feature construction, and WMAPE scoring — against a local
Spark session, independent of Databricks or the M5 data itself.

```
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests/
```

## Results

Evaluated on a 28-day holdout, aggregated to (store, department):

| Model                    | WMAPE  |
|--------------------------|--------|
| Seasonal-naive baseline  | 0.1398 |
| LightGBM                 | 0.1033 |

LightGBM reduces forecast error by ~26% relative to the naive baseline.

![Forecast vs. actual](assets/forecast_vs_actual.png)

`scripts/export_predictions.py` also writes a flat, joined CSV of both
models' predictions (`exports/predictions.csv`) for downstream BI tooling
that can't read Delta tables directly.

## Project structure

```
ingest/       raw -> bronze -> silver
features/     silver -> gold
model/        baseline, training, evaluation
scripts/      data upload / export helpers
resources/    Databricks Asset Bundle job definition
tests/        unit tests (pytest + local Spark)
exports/      flat prediction exports for BI tools
assets/       generated charts
```
