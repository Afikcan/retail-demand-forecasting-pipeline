"""
Step 5b: Main model

Aggregates sales to (store_id, dept_id, date), builds lag/rolling/calendar
features at that grain, and trains a LightGBM regressor. The last 28 days
are held out as validation. Predictions are written alongside the baseline
for comparison in 06_evaluate.py.
"""

import shutil
import tempfile

import lightgbm as lgb
import pandas as pd
from pyspark.sql import SparkSession, functions as F

GOLD_PATH = "/Volumes/workspace/retail_demand/gold"
PRED_PATH = "/Volumes/workspace/retail_demand/predictions"
MODEL_PATH = "/Volumes/workspace/retail_demand/models/lightgbm_model.txt"

GROUP_COLS = ["store_id", "dept_id"]
LAGS = [7, 28]
ROLLING_WINDOWS = [7, 28]
VALIDATION_DAYS = 28
FEATURE_COLS = [
    "day_of_week",
    "is_holiday",
    "sell_price",
    *[f"sales_lag_{lag}" for lag in LAGS],
    *[f"sales_roll_mean_{w}" for w in ROLLING_WINDOWS],
]

spark = SparkSession.builder.getOrCreate()


def load_grouped_pandas():
    df = spark.read.format("delta").load(f"{GOLD_PATH}/sales_features")
    grouped = df.groupBy(*GROUP_COLS, "date").agg(
        F.sum("sales").alias("sales"),
        F.avg("sell_price").alias("sell_price"),
        F.max("is_holiday").alias("is_holiday"),
        F.first("day_of_week").alias("day_of_week"),
    )
    return grouped.orderBy(*GROUP_COLS, "date").toPandas()


def add_features(pdf):
    pdf = pdf.sort_values([*GROUP_COLS, "date"])
    grp = pdf.groupby(GROUP_COLS)["sales"]

    for lag in LAGS:
        pdf[f"sales_lag_{lag}"] = grp.shift(lag)

    for window in ROLLING_WINDOWS:
        pdf[f"sales_roll_mean_{window}"] = grp.transform(
            lambda s, w=window: s.shift(1).rolling(w).mean()
        )

    return pdf.dropna(subset=[f"sales_lag_{max(LAGS)}"])


def train_val_split(pdf):
    cutoff = pdf["date"].max() - pd.Timedelta(days=VALIDATION_DAYS)
    train = pdf[pdf["date"] <= cutoff]
    val = pdf[pdf["date"] > cutoff]
    return train, val


def main():
    pdf = load_grouped_pandas()
    pdf = add_features(pdf)
    train, val = train_val_split(pdf)

    train_set = lgb.Dataset(train[FEATURE_COLS], label=train["sales"])
    val_set = lgb.Dataset(val[FEATURE_COLS], label=val["sales"], reference=train_set)

    params = {
        "objective": "regression",
        "metric": "mae",
        "learning_rate": 0.05,
        "num_leaves": 63,
        "verbose": -1,
    }
    model = lgb.train(
        params,
        train_set,
        num_boost_round=500,
        valid_sets=[val_set],
        callbacks=[lgb.early_stopping(20), lgb.log_evaluation(50)],
    )
    # LightGBM reopens the file in append mode to write metadata, which
    # Unity Catalog Volumes' FUSE mount doesn't support — save locally first.
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = f"{tmp_dir}/lightgbm_model.txt"
        model.save_model(local_path)
        shutil.copy(local_path, MODEL_PATH)

    val = val.copy()
    val["model_forecast"] = model.predict(val[FEATURE_COLS])
    preds = val[[*GROUP_COLS, "date"]].assign(
        actual=val["sales"], model_forecast=val["model_forecast"]
    )

    spark.createDataFrame(preds).write.format("delta").mode("overwrite").save(
        f"{PRED_PATH}/main_model"
    )
    print(f"wrote predictions.main_model: {len(preds)} rows")


if __name__ == "__main__":
    main()
