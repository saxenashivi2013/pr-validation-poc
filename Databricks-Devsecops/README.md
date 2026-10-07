# Databricks DevSecOps Demo — Retail Lakehouse + PR Guard

A medallion-architecture pipeline (bronze → silver → gold) deployed with a **Databricks Asset Bundle**,
guarded by a **GitHub Actions "PR Guard"** workflow that posts an AI summary, test + coverage results and a
dependency-vulnerability scan on every pull request.

Runs on the **Databricks Free Edition** (serverless compute, Unity Catalog, no cluster config needed).

## What the pipeline does

```
generate ─┬─ bronze_orders ────── silver_orders ──┬─ gold_daily_sales ────────┐
          ├─ bronze_customers ─── silver_customers ┼─ gold_customer_segments ──┼─ dq (gate)
          └─ bronze_products ──── silver_products ─┴─ gold_product_performance ┘
```

| Layer | Tables | Logic |
|-------|--------|-------|
| raw | UC volume `raw/` | Deterministic synthetic orders (JSONL, nested shipping struct), customers + products (CSV) with ~6% planted bad data |
| bronze | `bronze_*` | Explicit-schema ingest + lineage columns (`_ingested_at`, `_source_file`) |
| silver | `silver_*`, `quarantine_*` | Standardise text/types (ANSI-safe `try_*`), rule-based validation, **bad rows quarantined with the list of broken rules**, dedupe latest-wins, line totals |
| gold | `gold_daily_sales`, `gold_customer_segments`, `gold_product_performance` | Revenue with 7-day moving average; RFM quartile segmentation (Champion / At Risk / …); product margin + in-category rank |
| gate | `dq_results` | Non-empty, key uniqueness, quarantine ratio, orphan-product ratio, gold↔silver revenue reconciliation, valid segments — **fails the job if any check fails** |

## Repository layout

```
.
├── .github/
│   ├── workflows/
│   │   ├── pr-guard.yml        # PR Guard template (configured for this repo)
│   │   ├── ci.yml              # ruff lint + `databricks bundle validate`
│   │   └── deploy.yml          # manual: deploy (+ run) the bundle
│   └── pr-guard/               # PR Guard scripts + its own README
├── databricks.yml              # bundle definition, dev/prod targets
├── resources/jobs.yml          # the serverless job DAG
├── src/
│   ├── run_stage.py            # job entrypoint: one stage per task
│   └── retail_lakehouse/       # config, generator, bronze, silver, gold, quality, dq, stages
├── tests/                      # unit + local end-to-end tests (pytest, PySpark local mode)
├── pyproject.toml              # pytest / coverage / ruff config
└── requirements.txt            # pinned, scanned by PR Guard's OSV check
```

## 1 · Set up Databricks Free Edition

1. Sign up at <https://www.databricks.com/learn/free-edition> and open your workspace.
2. Copy the workspace URL, e.g. `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com` → this is `DATABRICKS_HOST`.
3. **Settings → Developer → Access tokens → Generate new token** → this is `DATABRICKS_TOKEN`.
4. Your workspace already has a catalog named `workspace`; the pipeline creates the schema and volume itself.

## 2 · Try the bundle from your machine (optional)

```bash
# install the CLI: https://docs.databricks.com/dev-tools/cli/install
export DATABRICKS_HOST=https://dbc-xxxx.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run retail_pipeline -t dev
```

Then in the workspace: **Catalog → workspace → retail_dev** to browse the tables, or run in a SQL editor:

```sql
SELECT segment, COUNT(*) FROM workspace.retail_dev.gold_customer_segments GROUP BY segment;
SELECT dq_failed_rules, COUNT(*) FROM workspace.retail_dev.quarantine_orders GROUP BY dq_failed_rules;
SELECT * FROM workspace.retail_dev.dq_results ORDER BY run_ts DESC;
```

> If deploy complains about `environment_version`, change `"2"` in `resources/jobs.yml` to a version your
> serverless workspace lists (Job → Environment & libraries).

## 3 · Push to GitHub and enable PR Guard

1. Create a repo and push this folder to `main`.
2. **Settings → Secrets and variables → Actions → Secrets**, add:

   | Secret | Value |
   |--------|-------|
   | `DATABRICKS_HOST` | workspace URL (no trailing slash) |
   | `DATABRICKS_TOKEN` | the token from step 1 |

   These serve two purposes: PR Guard's LLM summary uses the free Databricks-hosted
   `databricks-meta-llama-3-3-70b-instruct` endpoint, and `ci.yml`/`deploy.yml` use them for the bundle.
   *(Prefer OpenAI/Anthropic? Change `LLM_PROVIDER`/`LLM_MODEL` in `pr-guard.yml` and add that key instead.
   If the Databricks endpoint is unavailable on your plan, the AI section is simply skipped — tests and
   dependency scan still post.)*
3. **Settings → Actions → General → Workflow permissions**: allow read & write (PR Guard comments on PRs).
4. Optional: run **Actions → Deploy bundle → Run workflow** to deploy and execute the job from CI.

## 4 · Test PR Guard

Create a branch, make a change, and open a **non-draft** PR into `main`. Each scenario below exercises a
different section of the PR comment:

| Scenario | Change on the branch | Expect in the comment |
|----------|----------------------|-----------------------|
| Happy path | Tweak a comment in `src/retail_lakehouse/gold.py` | AI summary; all tests green; coverage table for the changed file |
| Failing test | In `gold.py`, add `"CANCELLED"` to `REVENUE_STATUSES` | ❌ `test_daily_sales_excludes_non_revenue_statuses` fails and is listed in the comment |
| Coverage drop | Add an untested function to `silver.py` | 🟡/🔴 coverage badge, missing line numbers for `silver.py` |
| Vulnerable dependency | Add `requests==2.19.1` to `requirements.txt` | 📦 table with CVEs, severities and fix versions |
| Comment edits in place | Push another commit to the same PR | Same comment is updated, not duplicated |

Re-run locally (needs Python 3.10+ and **Java 17** for PySpark):

```bash
pip install -r requirements.txt pytest pytest-cov
pytest tests --cov=src
```

## Notes & limits

- Free Edition is serverless-only with daily compute quotas; the default 5,000 orders (~13k lines) is small
  enough for a few runs per day. Lower it with `databricks bundle run retail_pipeline --params n_orders=1000`.
- `mode: development` prefixes the job name with `[dev <you>]` and keeps the schedule paused.
- Tables are rewritten each run (idempotent, seeded generator) to keep the demo simple; swap `_write` in
  `stages.py` for `MERGE` / Auto Loader when you want incremental loads.
