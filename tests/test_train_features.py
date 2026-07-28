import pandas as pd

from conftest import load_module

train = load_module("model/05_train.py", "train_module")


def _make_series(store_id, dept_id, n_days, start_sales=1):
    dates = pd.date_range("2024-01-01", periods=n_days, freq="D")
    return pd.DataFrame(
        {
            "store_id": store_id,
            "dept_id": dept_id,
            "date": dates,
            "sales": [start_sales + i for i in range(n_days)],
            "sell_price": 5.0,
            "is_holiday": 0,
            "day_of_week": dates.dayofweek,
        }
    )


def test_add_features_lag_matches_shifted_sales():
    pdf = _make_series("CA_1", "FOODS", n_days=40)
    result = train.add_features(pdf)

    # sales_lag_7 for a given row should equal sales 7 rows earlier
    row = result.iloc[0]
    original_idx = pdf.index[pdf["date"] == row["date"]][0]
    expected_lag_7 = pdf.loc[original_idx - 7, "sales"]
    assert row["sales_lag_7"] == expected_lag_7


def test_add_features_drops_rows_without_full_lag_history():
    pdf = _make_series("CA_1", "FOODS", n_days=40)
    result = train.add_features(pdf)

    # rows before day 28 (the longest lag) can't have a full lag_28 value
    assert result["date"].min() >= pdf["date"].iloc[28]


def test_add_features_keeps_groups_independent():
    store_a = _make_series("CA_1", "FOODS", n_days=40, start_sales=1)
    store_b = _make_series("CA_2", "FOODS", n_days=40, start_sales=1000)
    pdf = pd.concat([store_a, store_b], ignore_index=True)

    result = train.add_features(pdf)

    # a CA_2 row's lag should come from CA_2's own history, not CA_1's
    ca2_rows = result[result["store_id"] == "CA_2"]
    assert (ca2_rows["sales_lag_7"] >= 1000).all()
