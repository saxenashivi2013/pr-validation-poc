"""Deterministic synthetic data generator (pure Python, no Spark).

Produces orders / customers / products with a controlled amount of dirty data
(nulls, negatives, bad enums, unparsable timestamps, duplicates, malformed
emails) so the silver layer's validation and quarantine logic has real work to do.
"""
from __future__ import annotations

import csv
import json
import os
import random
from datetime import datetime, timedelta
from typing import Dict, List

COUNTRIES = ["US", "GB", "DE", "IN", "AU", "CA", "FR", "BR"]
SHIP_METHODS = ["standard", "express", "pickup"]
FIRST_NAMES = ["Ava", "Liam", "Noah", "Mia", "Zoe", "Ethan", "Isla", "Arjun", "Priya", "Lena", "Omar", "Chen"]
LAST_NAMES = ["Smith", "Patel", "Garcia", "Muller", "Brown", "Nguyen", "Silva", "Khan", "Dubois", "Jones"]
ADJECTIVES = ["Classic", "Pro", "Eco", "Ultra", "Compact", "Premium", "Smart", "Rugged"]
CATEGORIES: Dict[str, List[str]] = {
    "Electronics": ["Headphones", "Charger", "Speaker", "Webcam"],
    "Home": ["Lamp", "Kettle", "Blender", "Cushion"],
    "Sports": ["Yoga Mat", "Bottle", "Backpack", "Dumbbell"],
    "Books": ["Notebook", "Planner", "Cookbook", "Atlas"],
    "Beauty": ["Moisturiser", "Shampoo", "Perfume", "Brush"],
}
STATUS_WEIGHTS = {
    "PAID": 25, "SHIPPED": 25, "DELIVERED": 30,
    "PLACED": 8, "CANCELLED": 7, "RETURNED": 5,
}

TS_FMT = "%Y-%m-%dT%H:%M:%S"
START = datetime(2025, 1, 1)


def generate_products(n: int, seed: int = 42) -> List[dict]:
    rng = random.Random(seed)
    cats = list(CATEGORIES)
    rows = []
    for i in range(1, n + 1):
        cat = cats[(i - 1) % len(cats)]
        price = round(rng.uniform(5, 400), 2)
        rows.append({
            "product_id": f"P{i:04d}",
            "name": f"{rng.choice(ADJECTIVES)} {rng.choice(CATEGORIES[cat])}",
            "category": cat,
            "unit_cost": round(price * rng.uniform(0.35, 0.75), 2),
            "list_price": price,
        })
    if n >= 10:
        # Dirty: cost above list price -> quarantined in silver (creates orphan order lines)
        rows[3]["unit_cost"] = round(rows[3]["list_price"] * 1.5, 2)
        # Dirty: duplicate with stray whitespace -> deduplicated in silver
        dup = dict(rows[5])
        dup["name"] = f"  {dup['name']}  "
        rows.append(dup)
    return rows


def generate_customers(n: int, seed: int = 42, dirty_ratio: float = 0.06) -> List[dict]:
    rng = random.Random(seed + 1)
    rows = []
    for i in range(1, n + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        signup = START - timedelta(days=rng.randint(1, 700))
        row = {
            "customer_id": f"C{i:05d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}{i}@example.com".lower(),
            "country": rng.choice(COUNTRIES),
            "signup_date": signup.strftime("%Y-%m-%d"),
            "updated_at": signup.strftime(TS_FMT),
        }
        if rng.random() < dirty_ratio / 2:
            row["email"] = "not-an-email"                  # fails email rule
        rows.append(row)

    # Late-arriving updates: same customer_id, newer updated_at, shouty email
    for row in rng.sample(rows, k=max(1, int(n * dirty_ratio / 2))):
        upd = dict(row)
        upd["email"] = row["email"].upper() if "@" in row["email"] else row["email"]
        upd["country"] = rng.choice(COUNTRIES)
        upd["updated_at"] = (START + timedelta(days=rng.randint(1, 30))).strftime(TS_FMT)
        rows.append(upd)
    return rows


def generate_orders(
    n_orders: int,
    customers: List[dict],
    products: List[dict],
    seed: int = 42,
    days: int = 90,
    dirty_ratio: float = 0.06,
) -> List[dict]:
    rng = random.Random(seed + 2)
    cust_ids = sorted({c["customer_id"] for c in customers})
    prices = {p["product_id"]: p["list_price"] for p in products}
    prod_ids = sorted(prices)
    statuses, weights = zip(*STATUS_WEIGHTS.items())

    lines: List[dict] = []
    for i in range(1, n_orders + 1):
        order_id = f"O{i:06d}"
        ts = START + timedelta(days=rng.randrange(days), seconds=rng.randrange(86400))
        customer = rng.choice(cust_ids)
        status = rng.choices(statuses, weights)[0]
        shipping = {"country": rng.choice(COUNTRIES), "method": rng.choice(SHIP_METHODS)}
        for k, pid in enumerate(rng.sample(prod_ids, rng.randint(1, 4)), start=1):
            line = {
                "order_line_id": f"{order_id}-{k}",
                "order_id": order_id,
                "order_ts": ts.strftime(TS_FMT),
                "customer_id": customer,
                "product_id": pid,
                "quantity": rng.randint(1, 5),
                "unit_price": prices[pid],
                "discount_pct": rng.choice([0, 0, 0, 5, 10, 15, 25]),
                "status": status,
                "shipping": dict(shipping),
            }
            if rng.random() < 0.05:
                line["status"] = rng.choice([status.lower(), f" {status.title()} "])  # cosmetic mess
            if rng.random() < dirty_ratio:
                _corrupt(line, rng)
            lines.append(line)
            if rng.random() < dirty_ratio / 2:
                lines.append(dict(line))                   # exact duplicate delivery
    return lines


def _corrupt(line: dict, rng: random.Random) -> None:
    kind = rng.choice(["null_customer", "neg_qty", "zero_price", "bad_status", "bad_ts", "big_discount"])
    if kind == "null_customer":
        line["customer_id"] = None
    elif kind == "neg_qty":
        line["quantity"] = -abs(line["quantity"])
    elif kind == "zero_price":
        line["unit_price"] = 0.0
    elif kind == "bad_status":
        line["status"] = "???"
    elif kind == "bad_ts":
        line["order_ts"] = "not-a-date"
    else:
        line["discount_pct"] = 150


def write_jsonl(path: str, rows: List[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def write_csv(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_all(
    raw_root: str,
    n_customers: int,
    n_products: int,
    n_orders: int,
    seed: int = 42,
    days: int = 90,
    dirty_ratio: float = 0.06,
) -> Dict[str, int]:
    """Write all three raw datasets under *raw_root* and return row counts."""
    customers = generate_customers(n_customers, seed, dirty_ratio)
    products = generate_products(n_products, seed)
    orders = generate_orders(n_orders, customers, products, seed, days, dirty_ratio)

    write_csv(f"{raw_root}/customers/customers.csv", customers,
              ["customer_id", "first_name", "last_name", "email", "country", "signup_date", "updated_at"])
    write_csv(f"{raw_root}/products/products.csv", products,
              ["product_id", "name", "category", "unit_cost", "list_price"])
    write_jsonl(f"{raw_root}/orders/orders.jsonl", orders)
    return {"customers": len(customers), "products": len(products), "orders": len(orders)}
