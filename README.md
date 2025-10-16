# Devin Zero-ETL Demo: Salesforce → Oracle Transformations

This repository demonstrates robust, realistic business logic for transforming **Salesforce Opportunity & Account data** (arriving via AWS Zero ETL into your lake) for loading into **Oracle** (or your warehouse of choice).

## Features

- **Schema enforcement & validation** for core Salesforce fields
- **Deduplication** by `Id` using latest `LastModifiedDate`
- **Stage normalization** via mapping table
- **Currency normalization to USD** using daily FX rates (closest prior or same date lookup)
- **Expected revenue** and other metrics (`expected_revenue_usd`, `sales_cycle_days`, `is_won`, `is_lost`)
- **PII minimization**: email hashed (SHA-256) and phone normalized (E.164-ish light normalization)
- **Data quality checks**: invalid probabilities, negative amounts, close dates in future, missing mappings
- **Deterministic outputs** (sorted) for reproducibility
- **CLI**: single entry point with explicit inputs/outputs
- **Unit tests** with sample data

> Note: This demo uses **pandas** for convenience. In production, place this in Glue/Spark or dbt, and/or emit SQL for your Oracle landing tables.

## Layout

```
src/devin_etl/
  pipeline.py          # CLI entrypoint
  transformations.py   # core business transforms
  quality_checks.py    # rule-based validations
  utils.py             # helpers (FX lookups, parsing, normalization)
  __init__.py
tests/
  test_transformations.py
  sample_data/
    opportunities.csv
    accounts.csv
    fx_rates.csv
    stage_mapping.csv
out/                   # outputs written here by default
requirements.txt
pyproject.toml
LICENSE
README.md
```

## Getting Started

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m devin_etl.pipeline   --opportunities tests/sample_data/opportunities.csv   --accounts tests/sample_data/accounts.csv   --fx tests/sample_data/fx_rates.csv   --stage-map tests/sample_data/stage_mapping.csv   --outdir out
```

Outputs:
- `out/opportunities_transformed.csv` – canonical, modelled facts
- `out/opportunities_anomalies.csv` – rows + issue codes
- `out/run_summary.json` – counts, timing, rule hits

## Configuration

For simplicity, configuration is handled via CLI flags. Extend `utils.py` with environment/secret managers (e.g., AWS Parameter Store) for credentials and production configs.

## Oracle Load

The output CSV can be `SQL*Loader`-ed or external-table loaded. Consider schema:

```sql
CREATE TABLE SALES_OPPORTUNITY_FACT (
  OPPORTUNITY_ID         VARCHAR2(36) PRIMARY KEY,
  ACCOUNT_ID             VARCHAR2(36),
  ACCOUNT_NAME           VARCHAR2(255),
  STAGE                  VARCHAR2(64),
  STAGE_STD              VARCHAR2(64),
  AMOUNT_ORIG            NUMBER(18,2),
  CURRENCY_ORIG          CHAR(3),
  AMOUNT_USD             NUMBER(18,2),
  EXPECTED_REVENUE_USD   NUMBER(18,2),
  PROBABILITY_PCT        NUMBER(5,2),
  CLOSE_DATE             DATE,
  CREATED_DATE           DATE,
  LAST_MODIFIED_DATE     DATE,
  SALES_CYCLE_DAYS       NUMBER,
  OWNER_EMAIL_HASH       CHAR(64),
  PHONE_NORMALIZED       VARCHAR2(32),
  IS_WON                 NUMBER(1),
  IS_LOST                NUMBER(1),
  DATA_SOURCE            VARCHAR2(32),
  RECORD_VERSION_TS      TIMESTAMP
);
```

## Security & Compliance

- PII is minimized/hardened by hashing emails and normalizing phones.
- Add role-based encryption at rest, column-level masking in Oracle, and access policies in IAM/Okta.

## Limitations

- FX matching picks the **closest prior or same day** rate. If none exists, it uses the latest available and flags an anomaly.
- Stage mapping is demo-grade; extend to your enterprise taxonomy.
- Does not implement SCD2 in this demo.

---

**Author:** Generated for demo purposes.
**License:** MIT
