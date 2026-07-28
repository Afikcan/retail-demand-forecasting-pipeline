"""
Step 5a: Baseline model

Aggregates sales to (store_id, dept_id, date) and forecasts each day as the
actual value from 7 days prior (seasonal-naive). Writes predictions for
comparison against the main model in 06_evaluate.py.
"""

from pyspark.sql import SparkSession, Window, functions as F

GOLD_PATH = "/Volumes/workspace/retail_demand/gold"
PRED_PATH = "/Volumes/workspace/retail_demand/predictions"

GROUP_COLS = ["store_id", "dept_id"]

spark = SparkSession.builder.getOrCreate()


def aggregate_to_group(df):
    return df.groupBy(*GROUP_COLS, "date").agg(F.sum("sales").alias("sales"))


def main():
    df = spark.read.format("delta").load(f"{GOLD_PATH}/sales_features")

    grouped = aggregate_to_group(df)

    group_window = Window.partitionBy(*GROUP_COLS).orderBy("date")
    preds = grouped.withColumn(
        "baseline_forecast", F.lag("sales", 7).over(group_window)
    )

    preds = preds.select(
        *GROUP_COLS, "date", F.col("sales").alias("actual"), "baseline_forecast"
    )

    preds.write.format("delta").mode("overwrite").save(f"{PRED_PATH}/baseline")
    print(f"wrote predictions.baseline: {preds.count()} rows")


if __name__ == "__main__":
    main()
