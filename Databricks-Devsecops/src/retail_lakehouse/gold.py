"""Gold layer: business-ready aggregates built from clean silver tables."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Orders that count as realised revenue (excludes PLACED / CANCELLED / RETURNED)
REVENUE_STATUSES = ("PAID", "SHIPPED", "DELIVERED")
SEGMENTS = {"Champion", "Promising", "New", "At Risk", "Hibernating", "Needs Attention"}


def revenue_orders(orders: DataFrame) -> DataFrame:
    return orders.where(F.col("status").isin(*REVENUE_STATUSES))


def daily_sales(orders: DataFrame) -> DataFrame:
    """Aggregate realised revenue per day and shipping country with a 7-day moving average."""
    daily = (
        revenue_orders(orders)
        .groupBy("order_date", "ship_country")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.sum("line_total").alias("revenue"),
        )
        .withColumn("avg_order_value", F.round(F.col("revenue") / F.col("orders"), 2))
    )
    w = Window.partitionBy("ship_country").orderBy("order_date").rowsBetween(-6, 0)
    return daily.withColumn("revenue_7d_avg", F.round(F.avg("revenue").over(w), 2))


def customer_segments(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """RFM scoring (quartiles) mapped to named segments.

    Recency is measured against the latest order date in the data, so results
    are reproducible regardless of when the pipeline runs.
    """
    rev = revenue_orders(orders)
    as_of = rev.agg(F.max("order_date").alias("d")).collect()[0]["d"]

    rfm = (
        rev.groupBy("customer_id")
        .agg(
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_id").alias("frequency"),
            F.sum("line_total").alias("monetary"),
        )
        .withColumn("recency_days", F.datediff(F.lit(as_of), F.col("last_order_date")))
    )
    # Global windows are fine at demo scale; partition by a cohort key at real scale.
    r_score = F.ntile(4).over(Window.orderBy(F.col("recency_days").desc()))
    f_score = F.ntile(4).over(Window.orderBy(F.col("frequency").asc()))
    m_score = F.ntile(4).over(Window.orderBy(F.col("monetary").asc()))
    scored = (
        rfm.withColumn("r_score", r_score)
           .withColumn("f_score", f_score)
           .withColumn("m_score", m_score)
    )
    segment = (
        F.when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "Champion")
         .when((F.col("r_score") >= 3) & (F.col("frequency") == 1), "New")
         .when(F.col("r_score") >= 3, "Promising")
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), "At Risk")
         .when((F.col("r_score") == 1) & (F.col("f_score") <= 2), "Hibernating")
         .otherwise("Needs Attention")
    )
    return (
        scored.withColumn("segment", segment)
        .join(customers.select("customer_id", "first_name", "last_name", "country", "signup_date"),
              "customer_id", "left")
    )


def product_performance(orders: DataFrame, products: DataFrame) -> DataFrame:
    """Revenue, margin and in-category rank per product (orphan lines are excluded)."""
    agg = (
        revenue_orders(orders)
        .groupBy("product_id")
        .agg(F.sum("quantity").alias("units"), F.sum("line_total").alias("revenue"))
    )
    joined = agg.join(
        products.select("product_id", "name", "category", "unit_cost"), "product_id", "inner"
    )
    joined = (
        joined.withColumn("cost", F.round(F.col("units") * F.col("unit_cost"), 2).cast("decimal(14,2)"))
              .withColumn("gross_margin", F.col("revenue") - F.col("cost"))
              .withColumn(
                  "margin_pct",
                  F.when(F.col("revenue") > 0, F.round(F.col("gross_margin") / F.col("revenue") * 100, 2)),
              )
              .drop("unit_cost")
    )
    w = Window.partitionBy("category").orderBy(F.col("revenue").desc())
    return joined.withColumn("category_rank", F.dense_rank().over(w))
