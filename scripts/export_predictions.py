"""
Exports the baseline + LightGBM predictions as a single joined CSV for use
in BI tools (Tableau, Power BI) that can't read Delta tables directly.
"""

from pyspark.sql import SparkSession

PRED_PATH = "/Volumes/workspace/retail_demand/predictions"
GROUP_COLS = ["store_id", "dept_id"]

spark = SparkSession.builder.getOrCreate()


def main():
    baseline = spark.read.format("delta").load(f"{PRED_PATH}/baseline")
    main_model = spark.read.format("delta").load(f"{PRED_PATH}/main_model")

    combined = main_model.join(
        baseline.select(*GROUP_COLS, "date", "baseline_forecast"),
        on=[*GROUP_COLS, "date"],
        how="left",
    )

    (
        combined.coalesce(1)
        .write.format("csv")
        .mode("overwrite")
        .option("header", "true")
        .save(f"{PRED_PATH}/export_csv")
    )
    print(f"wrote predictions.export_csv: {combined.count()} rows")


if __name__ == "__main__":
    main()
