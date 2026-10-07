"""Pipeline stages. Each stage is a function (spark, settings) -> None, run as one job task."""
from __future__ import annotations

from typing import Callable, Dict

from pyspark.sql import DataFrame, SparkSession

from . import bronze, dq, generator, gold, silver
from .config import Settings


def _write(df: DataFrame, s: Settings, table: str) -> None:
    (df.write.format(s.table_format)
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(s.table(table)))
    print(f"[stage] wrote {s.table(table)}")


def generate(spark: SparkSession, s: Settings) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s.schema_fqn}")
    if s.uses_volume:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {s.schema_fqn}.{s.volume}")
    counts = generator.generate_all(
        s.raw_root, s.n_customers, s.n_products, s.n_orders, s.seed, s.days, s.dirty_ratio)
    print(f"[stage] generated raw files in {s.raw_root}: {counts}")


# ── Bronze ────────────────────────────────────────────────────────────────────
def bronze_orders(spark, s):    _write(bronze.read_orders(spark, s.raw_root), s, "bronze_orders")
def bronze_customers(spark, s): _write(bronze.read_customers(spark, s.raw_root), s, "bronze_customers")
def bronze_products(spark, s):  _write(bronze.read_products(spark, s.raw_root), s, "bronze_products")


# ── Silver ────────────────────────────────────────────────────────────────────
def silver_orders(spark, s):
    valid, bad = silver.clean_orders(spark.table(s.table("bronze_orders")))
    _write(valid, s, "silver_orders")
    _write(bad, s, "quarantine_orders")


def silver_customers(spark, s):
    valid, bad = silver.clean_customers(spark.table(s.table("bronze_customers")))
    _write(valid, s, "silver_customers")
    _write(bad, s, "quarantine_customers")


def silver_products(spark, s):
    valid, bad = silver.clean_products(spark.table(s.table("bronze_products")))
    _write(valid, s, "silver_products")
    _write(bad, s, "quarantine_products")


# ── Gold ──────────────────────────────────────────────────────────────────────
def gold_daily_sales(spark, s):
    _write(gold.daily_sales(spark.table(s.table("silver_orders"))), s, "gold_daily_sales")


def gold_customer_segments(spark, s):
    df = gold.customer_segments(spark.table(s.table("silver_orders")), spark.table(s.table("silver_customers")))
    _write(df, s, "gold_customer_segments")


def gold_product_performance(spark, s):
    df = gold.product_performance(spark.table(s.table("silver_orders")), spark.table(s.table("silver_products")))
    _write(df, s, "gold_product_performance")


# ── Quality gate ──────────────────────────────────────────────────────────────
def dq_gate(spark, s):
    dq.persist_and_enforce(spark, s, dq.run_checks(spark, s))


STAGES: Dict[str, Callable[[SparkSession, Settings], None]] = {
    "generate": generate,
    "bronze_orders": bronze_orders,
    "bronze_customers": bronze_customers,
    "bronze_products": bronze_products,
    "silver_orders": silver_orders,
    "silver_customers": silver_customers,
    "silver_products": silver_products,
    "gold_daily_sales": gold_daily_sales,
    "gold_customer_segments": gold_customer_segments,
    "gold_product_performance": gold_product_performance,
    "dq": dq_gate,
}

# Order used for local end-to-end runs (the Databricks job expresses the same as a DAG)
PIPELINE_ORDER = list(STAGES)
