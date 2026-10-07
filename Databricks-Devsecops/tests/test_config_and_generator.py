import json

import pytest

from retail_lakehouse import generator
from retail_lakehouse.config import Settings


def test_settings_paths_and_tables():
    s = Settings(catalog="workspace", schema="retail_dev")
    assert s.raw_root == "/Volumes/workspace/retail_dev/raw"
    assert s.table("silver_orders") == "workspace.retail_dev.silver_orders"
    assert s.uses_volume


def test_settings_override_disables_volume():
    s = Settings(catalog="spark_catalog", schema="t", raw_root_override="/tmp/x", table_format="parquet")
    assert s.raw_root == "/tmp/x"
    assert not s.uses_volume


@pytest.mark.parametrize("bad", ["a b", "x;drop", "1abc", "", "a.b"])
def test_settings_reject_unsafe_identifiers(bad):
    with pytest.raises(ValueError):
        Settings(catalog=bad, schema="ok")
    with pytest.raises(ValueError):
        Settings(catalog="ok", schema="ok").table(bad)


def test_generator_is_deterministic():
    a = generator.generate_products(20, seed=1)
    b = generator.generate_products(20, seed=1)
    assert a == b


def test_products_contain_planted_dirty_rows():
    rows = generator.generate_products(20, seed=1)
    assert len(rows) == 21                                   # one planted duplicate
    assert any(r["unit_cost"] > r["list_price"] for r in rows)


def test_customers_have_late_updates_and_bad_emails():
    rows = generator.generate_customers(400, seed=3, dirty_ratio=0.2)
    ids = [r["customer_id"] for r in rows]
    assert len(ids) > len(set(ids))                          # duplicate customer ids
    assert any("@" not in r["email"] for r in rows)


def test_orders_have_dirty_lines():
    customers = generator.generate_customers(50, seed=5)
    products = generator.generate_products(20, seed=5)
    lines = generator.generate_orders(400, customers, products, seed=5, dirty_ratio=0.2)
    assert any(l["customer_id"] is None for l in lines) or any(l["quantity"] < 0 for l in lines)
    ids = [l["order_line_id"] for l in lines]
    assert len(ids) > len(set(ids))                          # duplicate deliveries


def test_generate_all_writes_files(tmp_path):
    counts = generator.generate_all(str(tmp_path), 20, 12, 50, seed=9)
    assert counts["orders"] >= 50
    first = (tmp_path / "orders" / "orders.jsonl").read_text().splitlines()[0]
    assert "order_line_id" in json.loads(first)
    assert (tmp_path / "customers" / "customers.csv").exists()
    assert (tmp_path / "products" / "products.csv").exists()
