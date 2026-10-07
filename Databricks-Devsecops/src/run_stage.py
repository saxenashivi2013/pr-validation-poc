"""Entrypoint for Databricks `spark_python_task`s: one invocation runs one stage."""
import argparse
import os
import sys

# Bundle syncs src/ as plain workspace files, so make the package importable.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession  # noqa: E402

from retail_lakehouse.config import Settings  # noqa: E402
from retail_lakehouse.stages import STAGES  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--stage", required=True, choices=sorted(STAGES))
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--n-orders", type=int, default=5000)
    args = p.parse_args()

    settings = Settings(catalog=args.catalog, schema=args.schema, n_orders=args.n_orders)
    spark = SparkSession.builder.getOrCreate()
    print(f"[run_stage] stage={args.stage} target={settings.schema_fqn}")
    STAGES[args.stage](spark, settings)


if __name__ == "__main__":
    main()
