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

## Automated Salesforce to Supabase ETL Pipeline

This repository now includes an automated ETL pipeline that extracts data directly from Salesforce and loads it into Supabase, bypassing the need for manual CSV exports.

### Setup

The pipeline requires the following environment variables to be set:

- `SALESFORCE_URL` - Your Salesforce instance URL (e.g., `https://your-instance.salesforce.com`)
- `SALESFORCE_USERNAME` - Your Salesforce username
- `SALESFORCE_PASSWORD` - Your Salesforce password  
- `SALESFORCE_SECURITY_TOKEN` - Your Salesforce security token

### Running the Pipeline

```bash
cd ~/repos/business-logic
source .venv/bin/activate
python -m src.salesforce_supabase_etl
```

The pipeline will:
1. Authenticate with Salesforce using the provided credentials
2. Extract Opportunity and Account data from your Salesforce org
3. Apply the same transformation logic as the CSV-based pipeline
4. Write results to two Supabase tables:
   - `opportunities_transformed` - Canonical, transformed opportunity facts
   - `opportunities_anomalies` - Data quality issues detected during transformation

### Key Features

- **Dynamic field detection**: Automatically adapts to your Salesforce org's available fields
- **Resilient extraction**: Handles missing optional fields (e.g., CurrencyIsoCode, Phone, Industry)
- **Batch processing**: Efficiently loads data into Supabase in batches
- **Full audit trail**: All records timestamped with `_created_at` in Supabase
- **Uses existing business logic**: Leverages the same transformation pipeline and quality checks as the CSV-based approach

### Architecture

```
src/salesforce_supabase_etl.py    # Main ETL orchestration script
  ├── authenticate_salesforce()    # Salesforce OAuth authentication
  ├── extract_opportunities()      # Query Salesforce Opportunity data
  ├── extract_accounts()           # Query Salesforce Account data  
  ├── run_pipeline()               # Apply transformations (from transformations.py)
  ├── anomaly_rules()              # Apply quality checks (from quality_checks.py)
  └── write_to_supabase()          # Write to Supabase via MCP
```

The pipeline integrates with:
- **simple-salesforce** library for Salesforce API authentication
- **Supabase MCP** for table creation and data insertion
- Existing transformation logic in `src/transformations.py`
- Existing quality checks in `src/quality_checks.py`

---

**Author:** Generated for demo purposes.
**License:** MIT
