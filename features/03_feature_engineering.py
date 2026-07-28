"""
Step 4: Feature engineering (PySpark)

Reads the silver Delta table and adds lag/rolling sales features, holiday
flags, and price change flags. Writes the result as the gold feature table.
"""

from pyspark.sql import SparkSession, Window, functions as F

SILVER_PATH = "/Volumes/workspace/retail_demand/silver"
GOLD_PATH = "/Volumes/workspace/retail_demand/gold"

LAGS = [7, 28]
ROLLING_WINDOWS = [7, 28]

spark = SparkSession.builder.getOrCreate()


def add_lag_and_rolling_features(df):
    id_window = Window.partitionBy("id").orderBy("date")

    for lag in LAGS:
        df = df.withColumn(f"sales_lag_{lag}", F.lag("sales", lag).over(id_window))

    for window_size in ROLLING_WINDOWS:
        rolling_window = id_window.rowsBetween(-window_size, -1)
        df = df.withColumn(
            f"sales_roll_mean_{window_size}",
            F.avg("sales").over(rolling_window),
        )

    return df


def add_calendar_features(df):
    df = df.withColumn("day_of_week", F.dayofweek("date"))
    df = df.withColumn(
        "is_holiday",
        (F.col("event_name_1").isNotNull() | F.col("event_name_2").isNotNull()).cast(
            "int"
        ),
    )
    return df


def add_price_features(df):
    id_window = Window.partitionBy("id").orderBy("date")
    prev_price = F.lag("sell_price").over(id_window)
    df = df.withColumn(
        "price_change_flag",
        (F.col("sell_price") != prev_price).cast("int"),
    )
    return df


def main():
    silver_df = spark.read.format("delta").load(f"{SILVER_PATH}/sales_clean")

    df = add_lag_and_rolling_features(silver_df)
    df = add_calendar_features(df)
    df = add_price_features(df)

    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("state_id")
        .save(f"{GOLD_PATH}/sales_features")
    )
    print(f"wrote gold.sales_features: {df.count()} rows")


if __name__ == "__main__":
    main()
