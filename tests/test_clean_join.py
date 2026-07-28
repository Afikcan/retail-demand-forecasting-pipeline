from conftest import load_module

clean_join = load_module("ingest/02_clean_join.py", "clean_join_module")


def test_unpivot_sales_produces_one_row_per_day(spark):
    sales_df = spark.createDataFrame(
        [("CA_1_FOODS_1", "FOODS_1", "FOODS", "FOOD", "CA_1", "CA", 3, 5, 7)],
        ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id", "d_1", "d_2", "d_3"],
    )

    result = clean_join.unpivot_sales(sales_df).orderBy("d")

    rows = result.collect()
    assert [r["d"] for r in rows] == ["d_1", "d_2", "d_3"]
    assert [r["sales"] for r in rows] == [3, 5, 7]
    assert all(r["item_id"] == "FOODS_1" for r in rows)
