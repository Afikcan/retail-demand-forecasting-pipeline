"""
Step 2: Clean & join (PySpark)

Unpivot the wide sales table into one row per (id, day), join calendar and
sell_prices, and write the result as the silver Delta table.
"""

from pyspark.sql import SparkSession, functions as F

BRONZE_PATH = "/Volumes/workspace/retail_demand/bronze"
SILVER_PATH = "/Volumes/workspace/retail_demand/silver"

ID_COLS = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]

spark = SparkSession.builder.getOrCreate()


def unpivot_sales(sales_df):
    day_cols = [c for c in sales_df.columns if c.startswith("d_")]
    stack_expr = "stack({}, {}) as (d, sales)".format(
        len(day_cols), ", ".join(f"'{c}', `{c}`" for c in day_cols)
    )
    return sales_df.select(*ID_COLS, F.expr(stack_expr))


def build_silver(sales_df, calendar_df, prices_df):
    long_sales = unpivot_sales(sales_df)

    df = long_sales.join(calendar_df, on="d", how="left")

    df = df.join(prices_df, on=["store_id", "item_id", "wm_yr_wk"], how="left")

    df = df.dropDuplicates(["id", "d"])
    df = df.withColumn("date", F.to_date("date"))
    df = df.withColumn("sales", F.col("sales").cast("int"))
    df = df.na.fill({"sales": 0})

    return df.select(
        *ID_COLS,
        "date",
        "d",
        "wday",
        "month",
        "year",
        "event_name_1",
        "event_type_1",
        "event_name_2",
        "event_type_2",
        "snap_CA",
        "snap_TX",
        "snap_WI",
        "sell_price",
        "sales",
    )


def main():
    sales_df = spark.read.format("delta").load(f"{BRONZE_PATH}/sales")
    calendar_df = spark.read.format("delta").load(f"{BRONZE_PATH}/calendar")
    prices_df = spark.read.format("delta").load(f"{BRONZE_PATH}/sell_prices")

    silver_df = build_silver(sales_df, calendar_df, prices_df)

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("state_id")
        .save(f"{SILVER_PATH}/sales_clean")
    )
    print(f"wrote silver.sales_clean: {silver_df.count()} rows")


if __name__ == "__main__":
    main()
