"""Reusable data-quality primitives: row-level rules and dataset-level checks."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


# ── Row-level rules ───────────────────────────────────────────────────────────

def split_by_rules(df: DataFrame, rules: Dict[str, Column]) -> Tuple[DataFrame, DataFrame]:
    """Split *df* into (valid, invalid).

    Each rule is a boolean Column that must be true. NULL counts as a failure.
    The invalid frame gets a ``dq_failed_rules`` array listing every broken rule.
    """
    failures = F.array(*[
        F.when(F.coalesce(cond, F.lit(False)), F.lit(None)).otherwise(F.lit(name))
        for name, cond in rules.items()
    ])
    flagged = df.withColumn("dq_failed_rules", F.filter(failures, lambda x: x.isNotNull()))
    valid = flagged.where(F.size("dq_failed_rules") == 0).drop("dq_failed_rules")
    invalid = flagged.where(F.size("dq_failed_rules") > 0)
    return valid, invalid


# ── Dataset-level checks ──────────────────────────────────────────────────────

def check_not_empty(df: DataFrame, name: str) -> CheckResult:
    n = df.count()
    return CheckResult(f"{name}:not_empty", n > 0, f"{n} rows")


def check_unique(df: DataFrame, cols: list, name: str) -> CheckResult:
    dupes = df.groupBy(*cols).count().where("count > 1").count()
    return CheckResult(f"{name}:unique({','.join(cols)})", dupes == 0, f"{dupes} duplicated keys")


def check_ratio_below(numerator: int, denominator: int, limit: float, name: str) -> CheckResult:
    ratio = (numerator / denominator) if denominator else 0.0
    return CheckResult(name, ratio <= limit, f"{ratio:.2%} (limit {limit:.2%})")


def check_reconciles(left: float, right: float, name: str, tolerance: float = 0.01) -> CheckResult:
    diff = abs((left or 0.0) - (right or 0.0))
    return CheckResult(name, diff <= tolerance, f"left={left} right={right} diff={diff:.4f}")


def check_values_in(df: DataFrame, col: str, allowed: set, name: str) -> CheckResult:
    bad = df.where(~F.col(col).isin(*allowed) | F.col(col).isNull()).count()
    return CheckResult(name, bad == 0, f"{bad} rows outside {sorted(allowed)}")
