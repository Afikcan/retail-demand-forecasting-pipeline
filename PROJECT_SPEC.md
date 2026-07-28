# Retail Demand Forecasting Pipeline

A hands-on project to build real (not coursework-only) experience with PySpark,
Delta Lake, and Databricks, using retail time series data — built to close the
gap between your background and the invent.ai Data Scientist JD.

## Goal

Build an end-to-end pipeline: raw retail sales data → cleaned/joined with
PySpark → stored in Delta Lake → feature engineering → a demand forecasting
model → forecast vs. actual comparison.

Scope it to finish, not to impress. A working, explainable pipeline beats an
ambitious one you can't defend in an interview.

## Dataset (pick one)

- **M5 Forecasting (Walmart)** — Kaggle. Store × item × date sales, ~30k
  series, includes prices and calendar/holiday events. Larger, more realistic,
  better story for a retail-focused interview.
- **Rossmann Store Sales** — Kaggle. Smaller, simpler, faster to iterate on.
  Better if time is tight.

Start with Rossmann if you're unsure — smaller feedback loop while you learn
the Databricks environment, then move to M5 if you have time.

## Environment

- **Databricks Community Edition** (free) — do all processing here so you can
  honestly say the project was built in Databricks, not just locally with the
  `pyspark` package.
- Python 3.x, PySpark, Delta Lake (built into Databricks runtime), pandas for
  final plotting, one forecasting library (see below).

## Pipeline steps

1. **Ingest** — load raw CSVs into Databricks (DBFS or a Databricks volume).
2. **Clean & join (PySpark)** — merge sales, store metadata, and
   calendar/holiday tables; handle nulls, dedupe, cast types.
3. **Write to Delta Lake** — persist the cleaned table as a Delta table.
   This is the concrete Delta Lake line for your resume/interview — versioned,
   ACID-compliant storage, not just a CSV dump.
4. **Feature engineering (PySpark)** — rolling averages, lag features
   (7-day, 28-day), day-of-week, holiday flags, price change flags.
5. **Model** — keep it simple and defensible:
   - Baseline: naive/seasonal-naive forecast (for comparison)
   - Main model: LightGBM or Prophet per store/item-group (don't need
     per-SKU-per-store granularity — aggregate if needed to keep it tractable)
6. **Schedule (optional but a nice touch)** — set up a Databricks Job to
   re-run the pipeline on a schedule, so you have a real answer if asked
   "have you scheduled a pipeline before?"
7. **Evaluate & visualize** — forecast vs. actual, a basic error metric
   (MAPE or WMAPE), one or two charts.

## Deliverables for GitHub

- `README.md` — problem statement, data source, pipeline diagram/description,
  how to run it, results (one chart + one metric), honest scope note
  ("personal project built to gain hands-on Databricks/PySpark experience").
- Notebooks or `.py` files for each pipeline stage, organized in folders
  (`ingest/`, `features/`, `model/`).
- Requirements/environment file.
- Optional: exported Databricks notebook (`.dbc` or `.html`) as proof of
  where it ran.

## On timing / GitHub history

Don't backdate commits. Git *can* be told to fake a commit date, but:

- It's trivially checkable (commit vs. author timestamp mismatches, all
  commits pushed in one batch, etc.) — if anyone looks closely it reads as
  worse than "built this recently," it reads as "tried to hide something."
- A recently-built project isn't a red flag. Interviewers care whether you
  can explain your choices and defend the code, not the exact commit dates.
  Plenty of candidates build a project specifically for an application.

More useful than hiding dates: build it over several real, separate sessions
(a few days, not one 3am sprint) so the commit history looks like normal
iterative work — because it will be. Then just be upfront if asked:
"I built this recently to get real hands-on time with the stack you use."
That's a stronger answer than a manufactured timeline.

## Resume line (draft — fill in once built)

> **Retail Demand Forecasting Pipeline** — built an end-to-end PySpark +
> Delta Lake pipeline on Databricks to forecast [store/item]-level demand
> from [M5/Rossmann] retail sales data; achieved [X]% WMAPE with [model].

Don't add this to the CV until it's actually built and pushed — come back
and we'll wire it in then.
