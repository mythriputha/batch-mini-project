# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Batch Lakehouse Pipeline — Investment & Trade Data
# MAGIC Bronze -> Silver (+ Quarantine) -> Gold
# MAGIC
# MAGIC Run the cells in order. Set the widgets below before running.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbx_batch_mini_ws", "Catalog")
dbutils.widgets.text("schema", "batch_mini_project", "Schema")
dbutils.widgets.text("volume", "landing", "Volume")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")

SOURCE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_trades"
SILVER = f"{CATALOG}.{SCHEMA}.silver_trades"
QUARANTINE = f"{CATALOG}.{SCHEMA}.quarantine_trades"
GOLD = f"{CATALOG}.{SCHEMA}.gold_daily_position"
ACCOUNTS_TBL = f"{CATALOG}.{SCHEMA}.accounts"
CUSTOMERS_TBL = f"{CATALOG}.{SCHEMA}.customers"

print("SOURCE:", SOURCE)
print("BRONZE:", BRONZE, "| SILVER:", SILVER, "| QUARANTINE:", QUARANTINE, "| GOLD:", GOLD)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: catalog / schema / volume
# MAGIC Run once. Requires CREATE CATALOG / CREATE SCHEMA privileges (or ask a workspace admin to run this cell).

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC Upload customers.csv, accounts.csv, transactions.csv into the volume path shown above
# MAGIC (Catalog Explorer -> volume -> Upload, or `%fs cp` / Databricks CLI) before continuing.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read source files with explicit schema
# MAGIC Explicit schemas avoid inference drift and make the pipeline safe to rerun on unstable sources.

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
])

accounts_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("base_currency", StringType(), True),
    StructField("status", StringType(), True),
])

trades_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("trade_date", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("side", StringType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency", StringType(), True),
])

customers = spark.read.option("header", True).schema(customers_schema).csv(f"{SOURCE}/customers.csv")
accounts = spark.read.option("header", True).schema(accounts_schema).csv(f"{SOURCE}/accounts.csv")
trades_raw = spark.read.option("header", True).schema(trades_schema).csv(f"{SOURCE}/transactions.csv")

display(trades_raw)

# COMMAND ----------

# Persist dimension tables too (needed for the conformed join later, and useful for governance/lineage demos)
customers.write.mode("overwrite").format("delta").saveAsTable(CUSTOMERS_TBL)
accounts.write.mode("overwrite").format("delta").saveAsTable(ACCOUNTS_TBL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Bronze layer
# MAGIC Raw fidelity + ingestion metadata. No business logic here.

# COMMAND ----------

import uuid

BATCH_ID = str(uuid.uuid4())

def to_bronze(trades_raw: DataFrame) -> DataFrame:
    return (
        trades_raw
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )

bronze = to_bronze(trades_raw)

(bronze.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE))

display(spark.table(BRONZE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Silver: type casting + conformance

# COMMAND ----------

def cast_types(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df
        .withColumn("trade_date", F.to_date("trade_date"))
        .withColumn("quantity", F.col("quantity").cast("decimal(18,4)"))
        .withColumn("price", F.col("price").cast("decimal(18,4)"))
        .withColumn("trade_value", F.col("quantity") * F.col("price"))
        .withColumn("side", F.upper(F.trim(F.col("side"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    )

typed = cast_types(spark.table(BRONZE))
display(typed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality rules
# MAGIC | Rule | Action | Reason |
# MAGIC |---|---|---|
# MAGIC | trade_id not null | Quarantine | Primary business identifier required |
# MAGIC | account_id not null | Quarantine | Cannot associate trade |
# MAGIC | quantity > 0 | Quarantine | Prevents invalid position math |
# MAGIC | price > 0 | Quarantine | Prevents invalid trade value |
# MAGIC | side in BUY/SELL | Quarantine | Domain conformance |
# MAGIC | duplicate trade_id | Quarantine | Prevents double counting |

# COMMAND ----------

def apply_data_quality(typed_df: DataFrame):
    """Returns (valid_df, quarantine_df). quarantine_df carries a _dq_reason column."""

    dup_ids = (
        typed_df.groupBy("trade_id")
        .count()
        .where((F.col("count") > 1) & F.col("trade_id").isNotNull())
        .select("trade_id")
    )

    checked = (
        typed_df
        .withColumn("_is_dup", F.col("trade_id").isin([r["trade_id"] for r in dup_ids.collect()]))
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

valid, quarantine = apply_data_quality(typed)

(quarantine.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(QUARANTINE))

print("Valid rows:", valid.count())
print("Quarantined rows:", quarantine.count())
display(quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Conformed joins (dimension enrichment)

# COMMAND ----------

def build_silver(valid_df: DataFrame, accounts_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    return (
        valid_df.join(accounts_df, "account_id", "left")
        .join(customers_df, "customer_id", "left")
        .withColumn("_processed_at", F.current_timestamp())
    )

silver = build_silver(valid, spark.table(ACCOUNTS_TBL), spark.table(CUSTOMERS_TBL))

orphan_accounts = silver.where(F.col("account_type").isNull()).select("trade_id", "account_id")
print("Orphan account keys found:", orphan_accounts.count())
display(orphan_accounts)

(silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(SILVER))
display(spark.table(SILVER))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Gold business metrics

# COMMAND ----------

def build_gold(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.groupBy("trade_date", "symbol", "currency")
        .agg(
            F.sum(F.when(F.col("side") == "BUY", F.col("trade_value")).otherwise(0)).alias("buy_value"),
            F.sum(F.when(F.col("side") == "SELL", F.col("trade_value")).otherwise(0)).alias("sell_value"),
            F.sum("trade_value").alias("gross_value"),
            F.countDistinct("trade_id").alias("trade_count"),
        )
        .withColumn("net_flow", F.col("buy_value") - F.col("sell_value"))
    )

gold = build_gold(spark.table(SILVER))

(gold.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(GOLD))
display(spark.table(GOLD).orderBy("trade_date", "symbol"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation checks (run every batch)

# COMMAND ----------

src_count = trades_raw.count()
valid_count = spark.table(SILVER).count()
quarantine_count = spark.table(QUARANTINE).count()

print(f"Source rows: {src_count}")
print(f"Valid (silver) rows: {valid_count}")
print(f"Quarantined rows: {quarantine_count}")
print(f"Row-count reconciliation OK: {src_count == valid_count + quarantine_count}")

# Financial reconciliation: gross source trade value (valid rows only) == silver gross value
source_gross = valid.select((F.col("quantity") * F.col("price")).alias("v")).agg(F.sum("v")).collect()[0][0]
silver_gross = spark.table(GOLD).agg(F.sum("gross_value")).collect()[0][0]
print(f"Source-valid gross value: {source_gross} | Gold gross value: {silver_gross}")
print(f"Financial reconciliation OK: {source_gross == silver_gross}")