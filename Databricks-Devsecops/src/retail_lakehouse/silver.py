"""Silver layer: standardise, validate (quarantine bad rows), deduplicate, enrich."""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .quality import split_by_rules

VALID_STATUSES = ("PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED")
EMAIL_REGEX = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"


def _latest_per_key(df: DataFrame, key: str, order_cols: list) -> DataFrame:
    w = Window.partitionBy(key).orderBy(*[F.col(c).desc() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# ── Orders ────────────────────────────────────────────────────────────────────

def standardize_orders(df: DataFrame) -> DataFrame:
    """Normalise types and text. try_* functions keep this safe under ANSI mode."""
    return (
        df.withColumn("order_ts", F.try_to_timestamp(F.col("order_ts")))
          .withColumn("status", F.upper(F.trim("status")))
          .withColumn("ship_country", F.upper(F.trim(F.col("shipping.country"))))
          .withColumn("ship_method", F.lower(F.trim(F.col("shipping.method"))))
          .drop("shipping")
    )


def order_rules():
    return {
        "order_line_id_present": F.col("order_line_id").isNotNull(),
        "order_id_present":      F.col("order_id").isNotNull(),
        "customer_id_present":   F.col("customer_id").isNotNull(),
        "product_id_present":    F.col("product_id").isNotNull(),
        "quantity_positive":     F.col("quantity") > 0,
        "unit_price_positive":   F.col("unit_price") > 0,
        "discount_in_range":     F.col("discount_pct").between(0, 90),
        "status_valid":          F.col("status").isin(*VALID_STATUSES),
        "order_ts_parsable":     F.col("order_ts").isNotNull(),
    }


def enrich_orders(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "line_total",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2)
             .cast("decimal(14,2)"),
        )
        .withColumn("order_date", F.to_date("order_ts"))
    )


def clean_orders(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Return (silver_orders, quarantine). Duplicate deliveries keep the latest ingest."""
    valid, invalid = split_by_rules(standardize_orders(bronze), order_rules())
    deduped = _latest_per_key(valid, "order_line_id", ["_ingested_at", "_source_file"])
    return enrich_orders(deduped), invalid


# ── Customers ─────────────────────────────────────────────────────────────────

def standardize_customers(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("email", F.lower(F.trim("email")))
          .withColumn("country", F.upper(F.trim("country")))
          .withColumn("first_name", F.initcap(F.trim("first_name")))
          .withColumn("last_name", F.initcap(F.trim("last_name")))
          .withColumn("signup_date", F.to_date(F.try_to_timestamp(F.col("signup_date"), F.lit("yyyy-MM-dd"))))
          .withColumn("updated_at", F.try_to_timestamp(F.col("updated_at")))
    )


def customer_rules():
    return {
        "customer_id_present": F.col("customer_id").isNotNull(),
        "email_valid":         F.col("email").rlike(EMAIL_REGEX),
        "country_present":     F.col("country").isNotNull() & (F.length("country") == 2),
        "updated_at_parsable": F.col("updated_at").isNotNull(),
    }


def clean_customers(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Return (silver_customers, quarantine). Latest ``updated_at`` wins per customer."""
    valid, invalid = split_by_rules(standardize_customers(bronze), customer_rules())
    return _latest_per_key(valid, "customer_id", ["updated_at", "_ingested_at"]), invalid


# ── Products ──────────────────────────────────────────────────────────────────

def product_rules():
    return {
        "product_id_present":  F.col("product_id").isNotNull(),
        "category_present":    F.col("category").isNotNull(),
        "list_price_positive": F.col("list_price") > 0,
        "cost_not_above_price": F.col("unit_cost").between(0, F.col("list_price")),
    }


def clean_products(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    standardized = bronze.withColumn("name", F.trim("name")).withColumn("category", F.trim("category"))
    valid, invalid = split_by_rules(standardized, product_rules())
    return _latest_per_key(valid, "product_id", ["_ingested_at", "_source_file"]), invalid
