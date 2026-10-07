from datetime import date, datetime

from pyspark.sql import functions as F

from retail_lakehouse import gold, silver
from retail_lakehouse.quality import (
    check_not_empty, check_ratio_below, check_reconciles, check_unique, check_values_in,
    split_by_rules,
)

ORDER_COLS = ("order_line_id order_id order_ts customer_id product_id quantity unit_price "
              "discount_pct status shipping _ingested_at _source_file").split()


def bronze_orders(spark, rows):
    schema = ("order_line_id string, order_id string, order_ts string, customer_id string, "
              "product_id string, quantity long, unit_price double, discount_pct double, "
              "status string, shipping struct<country:string,method:string>, "
              "_ingested_at timestamp, _source_file string")
    return spark.createDataFrame(rows, schema)


def line(i, **kw):
    base = dict(order_line_id=f"L{i}", order_id=f"O{i}", order_ts="2025-01-10T10:00:00",
                customer_id="C1", product_id="P1", quantity=2, unit_price=10.0, discount_pct=10.0,
                status="PAID", shipping=("us", "Express"),
                _ingested_at=datetime(2025, 1, 11), _source_file="f1")
    base.update(kw)
    return tuple(base[c] for c in ORDER_COLS)


# ── quality primitives ────────────────────────────────────────────────────────

def test_split_by_rules_flags_all_failures_and_nulls(spark):
    df = spark.createDataFrame([(1,), (-1,), (None,)], "x int")
    valid, invalid = split_by_rules(df, {"positive": F.col("x") > 0, "small": F.col("x") < 100})
    assert [r.x for r in valid.collect()] == [1]
    failed = {r.x: r.dq_failed_rules for r in invalid.collect()}
    assert failed[-1] == ["positive"]
    assert set(failed[None]) == {"positive", "small"}


def test_dataset_checks(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",)], "k string")
    assert not check_unique(df, ["k"], "t").passed
    assert check_not_empty(df, "t").passed
    assert not check_not_empty(df.where("1=0"), "t").passed
    assert check_ratio_below(1, 10, 0.2, "r").passed
    assert not check_ratio_below(5, 10, 0.2, "r").passed
    assert check_ratio_below(0, 0, 0.2, "r").passed
    assert check_reconciles(10.0, 10.005, "rec").passed
    assert not check_reconciles(10.0, 11.0, "rec").passed
    assert check_values_in(df, "k", {"a", "b"}, "v").passed
    assert not check_values_in(df, "k", {"a"}, "v").passed


# ── silver orders ─────────────────────────────────────────────────────────────

def test_clean_orders_standardises_and_computes_total(spark):
    df = bronze_orders(spark, [line(1, status=" paid ")])
    valid, invalid = silver.clean_orders(df)
    row = valid.collect()[0]
    assert invalid.count() == 0
    assert row.status == "PAID" and row.ship_country == "US" and row.ship_method == "express"
    assert float(row.line_total) == 18.0                      # 2 * 10 * 0.9
    assert row.order_date == date(2025, 1, 10)


def test_clean_orders_quarantines_bad_rows_with_reasons(spark):
    df = bronze_orders(spark, [
        line(1),
        line(2, customer_id=None),
        line(3, quantity=-1),
        line(4, order_ts="not-a-date"),
        line(5, status="???"),
        line(6, discount_pct=150.0),
        line(7, unit_price=0.0),
    ])
    valid, invalid = silver.clean_orders(df)
    assert valid.count() == 1
    reasons = {r.order_line_id: r.dq_failed_rules for r in invalid.collect()}
    assert reasons["L2"] == ["customer_id_present"]
    assert reasons["L3"] == ["quantity_positive"]
    assert reasons["L4"] == ["order_ts_parsable"]
    assert reasons["L5"] == ["status_valid"]
    assert reasons["L6"] == ["discount_in_range"]
    assert reasons["L7"] == ["unit_price_positive"]


def test_clean_orders_dedupes_keeping_latest_ingest(spark):
    df = bronze_orders(spark, [
        line(1, quantity=1, _ingested_at=datetime(2025, 1, 11)),
        line(1, quantity=3, _ingested_at=datetime(2025, 1, 12)),
    ])
    valid, _ = silver.clean_orders(df)
    rows = valid.collect()
    assert len(rows) == 1 and rows[0].quantity == 3


# ── silver customers / products ───────────────────────────────────────────────

def test_clean_customers_dedupes_latest_and_rejects_bad_email(spark):
    schema = ("customer_id string, first_name string, last_name string, email string, country string, "
              "signup_date string, updated_at string, _ingested_at timestamp, _source_file string")
    ing = datetime(2025, 1, 1)
    df = spark.createDataFrame([
        ("C1", "ava", "smith", "OLD@x.com", "us", "2024-01-01", "2024-02-01T00:00:00", ing, "f"),
        ("C1", "ava", "smith", "New@X.com", "gb", "2024-01-01", "2024-03-01T00:00:00", ing, "f"),
        ("C2", "liam", "jones", "not-an-email", "us", "2024-01-01", "2024-02-01T00:00:00", ing, "f"),
    ], schema)
    valid, invalid = silver.clean_customers(df)
    rows = {r.customer_id: r for r in valid.collect()}
    assert set(rows) == {"C1"}
    assert rows["C1"].email == "new@x.com" and rows["C1"].country == "GB"
    assert rows["C1"].first_name == "Ava"
    assert invalid.collect()[0].dq_failed_rules == ["email_valid"]


def test_clean_products_rejects_cost_above_price_and_dedupes(spark):
    schema = ("product_id string, name string, category string, unit_cost double, list_price double, "
              "_ingested_at timestamp, _source_file string")
    ing = datetime(2025, 1, 1)
    df = spark.createDataFrame([
        ("P1", " Lamp ", "Home", 5.0, 10.0, ing, "f"),
        ("P1", "Lamp", "Home", 5.0, 10.0, ing, "f"),
        ("P2", "Bad", "Home", 20.0, 10.0, ing, "f"),
    ], schema)
    valid, invalid = silver.clean_products(df)
    assert [r.product_id for r in valid.collect()] == ["P1"]
    assert valid.collect()[0].name == "Lamp"
    assert invalid.collect()[0].dq_failed_rules == ["cost_not_above_price"]


# ── gold ──────────────────────────────────────────────────────────────────────

def silver_orders(spark, rows):
    valid, _ = silver.clean_orders(bronze_orders(spark, rows))
    return valid


def test_daily_sales_excludes_non_revenue_statuses(spark):
    orders = silver_orders(spark, [
        line(1, order_id="O1"),
        line(2, order_id="O1", order_line_id="L1b"),
        line(3, order_id="O3", status="CANCELLED"),
        line(4, order_id="O4", status="RETURNED"),
    ])
    row = gold.daily_sales(orders).collect()[0]
    assert row.orders == 1 and row.units == 4
    assert float(row.revenue) == 36.0
    assert float(row.avg_order_value) == 36.0


def test_daily_sales_moving_average(spark):
    rows = [line(i, order_id=f"O{i}", order_ts=f"2025-01-{10 + i:02d}T10:00:00") for i in range(1, 4)]
    out = gold.daily_sales(silver_orders(spark, rows)).orderBy("order_date").collect()
    assert [float(r.revenue_7d_avg) for r in out] == [18.0, 18.0, 18.0]


def test_customer_segments_assigns_valid_named_segments(spark):
    rows = []
    n = 0
    for c in range(1, 21):                       # customer c places c orders on spread-out days
        for k in range(c):
            n += 1
            rows.append(line(n, customer_id=f"C{c}", order_id=f"O{n}",
                             order_ts=f"2025-01-{1 + (c + k) % 28:02d}T09:00:00",
                             quantity=1 + c % 5))
    customers = spark.createDataFrame(
        [(f"C{c}", "A", "B", "US", date(2024, 1, 1)) for c in range(1, 21)],
        "customer_id string, first_name string, last_name string, country string, signup_date date")
    out = gold.customer_segments(silver_orders(spark, rows), customers)
    assert out.count() == 20
    assert {r.segment for r in out.collect()} <= gold.SEGMENTS
    assert out.where("segment is null").count() == 0
    assert out.where("country is null").count() == 0


def test_product_performance_margin_and_rank(spark):
    orders = silver_orders(spark, [
        line(1, product_id="P1", quantity=2, discount_pct=0.0, unit_price=10.0),
        line(2, product_id="P2", quantity=1, discount_pct=0.0, unit_price=50.0),
        line(3, product_id="P9", quantity=1, discount_pct=0.0, unit_price=5.0),   # orphan
    ])
    products = spark.createDataFrame(
        [("P1", "Lamp", "Home", 4.0), ("P2", "Kettle", "Home", 30.0)],
        "product_id string, name string, category string, unit_cost double")
    out = {r.product_id: r for r in gold.product_performance(orders, products).collect()}
    assert set(out) == {"P1", "P2"}                           # orphan excluded
    assert float(out["P1"].gross_margin) == 12.0              # 20 - 2*4
    assert float(out["P1"].margin_pct) == 60.0
    assert out["P2"].category_rank == 1 and out["P1"].category_rank == 2
