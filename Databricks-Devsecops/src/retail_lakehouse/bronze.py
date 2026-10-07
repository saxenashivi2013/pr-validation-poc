"""Bronze layer: land raw files as-is, plus lineage metadata."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import CUSTOMERS_SCHEMA, ORDERS_SCHEMA, PRODUCTS_SCHEMA


def add_ingest_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def read_orders(spark: SparkSession, raw_root: str) -> DataFrame:
    df = spark.read.schema(ORDERS_SCHEMA).json(f"{raw_root}/orders/")
    return add_ingest_metadata(df)


def read_customers(spark: SparkSession, raw_root: str) -> DataFrame:
    df = spark.read.schema(CUSTOMERS_SCHEMA).option("header", True).csv(f"{raw_root}/customers/")
    return add_ingest_metadata(df)


def read_products(spark: SparkSession, raw_root: str) -> DataFrame:
    df = spark.read.schema(PRODUCTS_SCHEMA).option("header", True).csv(f"{raw_root}/products/")
    return add_ingest_metadata(df)
