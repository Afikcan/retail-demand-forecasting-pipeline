"""
Step 7: Evaluate & visualize

Compares the seasonal-naive baseline against the LightGBM model over the
validation window: WMAPE for each, plus a forecast-vs-actual chart for one
store/dept series.
"""

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

PRED_PATH = "/Volumes/workspace/retail_demand/predictions"
GROUP_COLS = ["store_id", "dept_id"]

spark = SparkSession.builder.getOrCreate()


def wmape(pdf, forecast_col):
    return (pdf["actual"] - pdf[forecast_col]).abs().sum() / pdf["actual"].abs().sum()


def main():
    baseline = spark.read.format("delta").load(f"{PRED_PATH}/baseline").toPandas()
    main_model = (
        spark.read.format("delta").load(f"{PRED_PATH}/main_model").toPandas()
    )

    combined = main_model.merge(
        baseline[[*GROUP_COLS, "date", "baseline_forecast"]],
        on=[*GROUP_COLS, "date"],
        how="left",
    )

    print(f"baseline WMAPE:  {wmape(combined, 'baseline_forecast'):.4f}")
    print(f"LightGBM WMAPE:  {wmape(combined, 'model_forecast'):.4f}")

    sample_key = combined[GROUP_COLS].drop_duplicates().iloc[0]
    sample = combined[
        (combined["store_id"] == sample_key["store_id"])
        & (combined["dept_id"] == sample_key["dept_id"])
    ].sort_values("date")

    plt.figure(figsize=(10, 5))
    plt.plot(sample["date"], sample["actual"], label="actual")
    plt.plot(sample["date"], sample["baseline_forecast"], label="baseline")
    plt.plot(sample["date"], sample["model_forecast"], label="lightgbm")
    plt.title(f"{sample_key['store_id']} / {sample_key['dept_id']}")
    plt.xlabel("date")
    plt.ylabel("units sold")
    plt.legend()
    plt.tight_layout()
    plt.savefig("/Volumes/workspace/retail_demand/predictions/forecast_vs_actual.png")


if __name__ == "__main__":
    main()
