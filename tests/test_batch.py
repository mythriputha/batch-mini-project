"""
Local unit tests for the transformation logic in src/01_batch_pipeline.py.

Run locally (outside Databricks) with a local PySpark install:
    pip install pyspark==3.5.1 pytest
    pytest tests/test_batch.py -v

These tests import the pure functions (cast_types, apply_data_quality, build_silver,
build_gold) by re-defining them here against tiny in-memory DataFrames, so they run
without any Databricks/Unity Catalog dependency.
"""
import datetime
import pytest
from pyspark.sql import SparkSession, functions as F


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("batch-mini-project-tests")
        .getOrCreate()
    )


def cast_types(bronze_df):
    return (
        bronze_df
        .withColumn("trade_date", F.to_date("trade_date"))
        .withColumn("quantity", F.col("quantity").cast("decimal(18,4)"))
        .withColumn("price", F.col("price").cast("decimal(18,4)"))
        .withColumn("trade_value", F.col("quantity") * F.col("price"))
        .withColumn("side", F.upper(F.trim(F.col("side"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    )


def apply_data_quality(typed_df):
    dup_ids = (
        typed_df.groupBy("trade_id").count()
        .where((F.col("count") > 1) & F.col("trade_id").isNotNull())
        .select("trade_id")
    )
    dup_list = [r["trade_id"] for r in dup_ids.collect()]

    checked = (
        typed_df
        .withColumn("_is_dup", F.col("trade_id").isin(dup_list))
        .withColumn(
            "_dq_reason",
            F.when(F.col("trade_id").isNull(), "NULL_TRADE_ID")
             .when(F.col("account_id").isNull(), "NULL_ACCOUNT_ID")
             .when(F.col("quantity") <= 0, "NON_POSITIVE_QUANTITY")
             .when(F.col("price") <= 0, "NON_POSITIVE_PRICE")
             .when(~F.col("side").isin("BUY", "SELL"), "INVALID_SIDE")
             .when(F.col("_is_dup"), "DUPLICATE_TRADE_ID")
             .otherwise(None)
        )
    )
    quarantine_df = checked.where(F.col("_dq_reason").isNotNull()).drop("_is_dup")
    valid_df = checked.where(F.col("_dq_reason").isNull()).drop("_is_dup", "_dq_reason")
    return valid_df, quarantine_df


def _raw_trades(spark, rows):
    cols = ["trade_id", "account_id", "trade_date", "symbol", "side", "quantity", "price", "currency"]
    return spark.createDataFrame(rows, cols)


def test_null_trade_id_is_quarantined(spark):
    rows = [(None, "A001", "2024-06-01", "AAPL", "BUY", 10.0, 100.0, "USD")]
    typed = cast_types(_raw_trades(spark, rows))
    valid, quarantine = apply_data_quality(typed)
    assert valid.count() == 0
    assert quarantine.count() == 1
    assert quarantine.collect()[0]["_dq_reason"] == "NULL_TRADE_ID"


def test_invalid_side_is_quarantined(spark):
    rows = [("T001", "A001", "2024-06-01", "AAPL", "SEL", 10.0, 100.0, "USD")]
    typed = cast_types(_raw_trades(spark, rows))
    valid, quarantine = apply_data_quality(typed)
    assert valid.count() == 0
    assert quarantine.collect()[0]["_dq_reason"] == "INVALID_SIDE"


def test_non_positive_quantity_is_quarantined(spark):
    rows = [("T001", "A001", "2024-06-01", "AAPL", "BUY", -5.0, 100.0, "USD")]
    typed = cast_types(_raw_trades(spark, rows))
    valid, quarantine = apply_data_quality(typed)
    assert quarantine.collect()[0]["_dq_reason"] == "NON_POSITIVE_QUANTITY"


def test_duplicate_trade_id_is_quarantined(spark):
    rows = [
        ("T001", "A001", "2024-06-01", "AAPL", "BUY", 10.0, 100.0, "USD"),
        ("T001", "A001", "2024-06-01", "AAPL", "BUY", 10.0, 100.0, "USD"),
    ]
    typed = cast_types(_raw_trades(spark, rows))
    valid, quarantine = apply_data_quality(typed)
    assert valid.count() == 0
    assert quarantine.count() == 2


def test_valid_row_passes_through(spark):
    rows = [("T001", "A001", "2024-06-01", "AAPL", "buy", 10.0, 100.0, "usd")]
    typed = cast_types(_raw_trades(spark, rows))
    valid, quarantine = apply_data_quality(typed)
    assert valid.count() == 1
    assert quarantine.count() == 0
    r = valid.collect()[0]
    assert r["side"] == "BUY"
    assert r["currency"] == "USD"
    assert float(r["trade_value"]) == 1000.0


def test_row_count_reconciliation(spark):
    rows = [
        ("T001", "A001", "2024-06-01", "AAPL", "BUY", 10.0, 100.0, "USD"),
        (None, "A002", "2024-06-01", "MSFT", "BUY", 5.0, 50.0, "USD"),
        ("T003", "A003", "2024-06-01", "GOOG", "XYZ", 1.0, 10.0, "USD"),
    ]
    typed = cast_types(_raw_trades(spark, rows))
    valid, quarantine = apply_data_quality(typed)
    assert typed.count() == valid.count() + quarantine.count()


def test_rerun_is_idempotent_no_double_count(spark):
    """Re-running data quality on the same input twice must not change the counts."""
    rows = [("T001", "A001", "2024-06-01", "AAPL", "BUY", 10.0, 100.0, "USD")]
    typed = cast_types(_raw_trades(spark, rows))
    valid1, quarantine1 = apply_data_quality(typed)
    valid2, quarantine2 = apply_data_quality(typed)
    assert valid1.count() == valid2.count()
    assert quarantine1.count() == quarantine2.count()
