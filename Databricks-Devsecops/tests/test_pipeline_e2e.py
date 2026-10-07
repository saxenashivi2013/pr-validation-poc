"""End-to-end run of every stage on local Spark (parquet instead of Delta, tmp dir instead of a Volume)."""
import pytest

from retail_lakehouse.config import Settings
from retail_lakehouse.stages import PIPELINE_ORDER, STAGES


@pytest.fixture(scope="module")
def settings(tmp_path_factory):
    return Settings(
        catalog="spark_catalog", schema="retail_test",
        raw_root_override=str(tmp_path_factory.mktemp("raw")), table_format="parquet",
        n_customers=80, n_products=50, n_orders=400,
    )


def test_full_pipeline_passes_quality_gate(spark, settings):
    for stage in PIPELINE_ORDER:
        STAGES[stage](spark, settings)

    silver = spark.table(settings.table("silver_orders"))
    bronze = spark.table(settings.table("bronze_orders"))
    quarantine = spark.table(settings.table("quarantine_orders"))
    assert 0 < silver.count() < bronze.count()
    assert quarantine.count() > 0
    assert spark.table(settings.table("gold_daily_sales")).count() > 0

    results = spark.table(settings.table("dq_results")).collect()
    assert results and all(r.passed for r in results)


def test_dq_gate_fails_when_a_check_fails(spark, settings):
    from retail_lakehouse import dq
    from retail_lakehouse.quality import CheckResult

    with pytest.raises(RuntimeError, match="1 data-quality check"):
        dq.persist_and_enforce(spark, settings, [CheckResult("x", True, ""), CheckResult("y", False, "boom")])
