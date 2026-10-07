"""Explicit schemas for the raw landing files (never rely on inference in bronze)."""
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

ORDERS_SCHEMA = StructType([
    StructField("order_line_id", StringType()),
    StructField("order_id", StringType()),
    StructField("order_ts", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", LongType()),
    StructField("unit_price", DoubleType()),
    StructField("discount_pct", DoubleType()),
    StructField("status", StringType()),
    StructField("shipping", StructType([
        StructField("country", StringType()),
        StructField("method", StringType()),
    ])),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("country", StringType()),
    StructField("signup_date", StringType()),
    StructField("updated_at", StringType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("unit_cost", DoubleType()),
    StructField("list_price", DoubleType()),
])
