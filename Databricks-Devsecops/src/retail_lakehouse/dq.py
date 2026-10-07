"""Post-load data-quality gate. Fails the job if any check fails."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .gold import SEGMENTS, revenue_orders
from .quality import (
    CheckResult,
    check_not_empty,
    check_ratio_below,
    check_reconciles,
    check_unique,
    check_values_in,
)

TABLES = [
    "bronze_orders", "bronze_customers", "bronze_products",
    "silver_orders", "silver_customers", "silver_products",
    "gold_daily_sales", "gold_customer_segments", "gold_product_performance",
]


def run_checks(spark: SparkSession, s: Settings) -> List[CheckResult]:
    t = {name: spark.table(s.table(name)) for name in TABLES}
    results: List[CheckResult] = [check_not_empty(df, name) for name, df in t.items()]

    silver_orders = t["silver_orders"]
    results.append(check_unique(silver_orders, ["order_line_id"], "silver_orders"))
    results.append(check_unique(t["silver_customers"], ["customer_id"], "silver_customers"))
    results.append(check_unique(t["silver_products"], ["product_id"], "silver_products"))

    # Quarantine ratio: share of bronze order rows rejected by validation rules
    quarantined = spark.table(s.table("quarantine_orders")).count()
    results.append(check_ratio_below(
        quarantined, t["bronze_orders"].count(), s.max_quarantine_ratio, "orders:quarantine_ratio"))

    # Referential integrity: order lines whose product is missing from silver_products
    orphans = silver_orders.join(t["silver_products"], "product_id", "left_anti").count()
    results.append(check_ratio_below(
        orphans, silver_orders.count(), s.max_orphan_ratio, "orders:orphan_product_ratio"))

    # Reconciliation: gold daily revenue must equal silver realised revenue
    silver_rev = revenue_orders(silver_orders).agg(F.sum("line_total")).collect()[0][0]
    gold_rev = t["gold_daily_sales"].agg(F.sum("revenue")).collect()[0][0]
    results.append(check_reconciles(float(gold_rev or 0), float(silver_rev or 0),
                                    "gold_daily_sales:reconciles_with_silver"))

    results.append(check_values_in(t["gold_customer_segments"], "segment", SEGMENTS,
                                   "gold_customer_segments:valid_segments"))
    return results


def persist_and_enforce(spark: SparkSession, s: Settings, results: List[CheckResult]) -> None:
    run_ts = datetime.now(timezone.utc)
    rows = [(run_ts, r.name, r.passed, r.detail) for r in results]
    spark.createDataFrame(rows, "run_ts timestamp, check_name string, passed boolean, detail string") \
         .write.format(s.table_format).mode("append").saveAsTable(s.table("dq_results"))

    for r in results:
        print(f"[dq] {'PASS' if r.passed else 'FAIL'}  {r.name}  — {r.detail}")
    failed = [r for r in results if not r.passed]
    if failed:
        raise RuntimeError(f"{len(failed)} data-quality check(s) failed: "
                           + ", ".join(r.name for r in failed))
