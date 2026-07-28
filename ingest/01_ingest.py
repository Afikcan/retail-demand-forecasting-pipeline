"""
Step 1: Ingest

Load raw M5 CSVs into Spark and persist them as bronze Delta tables,
unmodified except for schema typing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

RAW_PATH = "/Volumes/workspace/retail_demand/raw"
BRONZE_PATH = "/Volumes/workspace/retail_demand/bronze"

spark = SparkSession.builder.getOrCreate()


def load_calendar():
    schema = StructType(
        [
            StructField("date", StringType()),
            StructField("wm_yr_wk", IntegerType()),
            StructField("weekday", StringType()),
            StructField("wday", IntegerType()),
            StructField("month", IntegerType()),
            StructField("year", IntegerType()),
            StructField("d", StringType()),
            StructField("event_name_1", StringType()),
            StructField("event_type_1", StringType()),
            StructField("event_name_2", StringType()),
            StructField("event_type_2", StringType()),
            StructField("snap_CA", IntegerType()),
            StructField("snap_TX", IntegerType()),
            StructField("snap_WI", IntegerType()),
        ]
    )
    return spark.read.csv(f"{RAW_PATH}/calendar.csv", header=True, schema=schema)


def load_sell_prices():
    schema = StructType(
        [
            StructField("store_id", StringType()),
            StructField("item_id", StringType()),
            StructField("wm_yr_wk", IntegerType()),
            StructField("sell_price", DoubleType()),
        ]
    )
    return spark.read.csv(f"{RAW_PATH}/sell_prices.csv", header=True, schema=schema)


def load_sales():
    # sales_train_evaluation.csv is wide (one column per day, d_1..d_1941);
    # read with inferred schema, unpivoted downstream in 02_clean_join.py.
    return spark.read.csv(
        f"{RAW_PATH}/sales_train_evaluation.csv", header=True, inferSchema=True
    )


def main():
    tables = {
        "calendar": load_calendar(),
        "sell_prices": load_sell_prices(),
        "sales": load_sales(),
    }
    for name, df in tables.items():
        df.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/{name}")
        print(f"wrote bronze.{name}: {df.count()} rows")


if __name__ == "__main__":
    main()
