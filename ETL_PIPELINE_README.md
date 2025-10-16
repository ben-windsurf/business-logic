# Salesforce to Supabase ETL Pipeline

This automated ETL pipeline extracts Opportunity data from Salesforce, applies business transformation logic, and loads the results to Supabase.

## Overview

The pipeline performs the following steps:

1. **Extract**: Pulls Opportunity and Account data from Salesforce via SOQL queries using the Salesforce API
2. **Transform**: Applies business logic from `src/transformations.py` including:
   - Deduplication (latest record wins)
   - Stage name standardization
   - Multi-currency normalization to USD
   - PII protection (email hashing, phone normalization)
   - Business metric calculations
3. **Quality Check**: Validates data using rules from `src/quality_checks.py`
4. **Load**: Writes transformed and anomaly data to Supabase tables

## Prerequisites

1. Python 3.12+ with virtual environment
2. Salesforce credentials (username, password, security token, instance URL)
3. Supabase project access
4. Required Python packages (automatically installed in venv)

## Setup

1. Activate the virtual environment:
   ```bash
   source .venv/bin/activate
   ```

2. Ensure required packages are installed:
   ```bash
   pip install -r requirements.txt
   pip install simple-salesforce requests
   ```

3. Set environment variables with your Salesforce credentials:
   ```bash
   export SALESFORCE_USERNAME="your_username"
   export SALESFORCE_PASSWORD="your_password"
   export SALESFORCE_SECURITY_TOKEN="your_security_token"
   export SALESFORCE_URL="https://your-instance.salesforce.com"
   ```

   Note: These are automatically available if running in Devin's environment via Secrets.

## Running the Pipeline

Execute the ETL pipeline:

```bash
python etl_pipeline.py
```

The pipeline will:
1. Authenticate with Salesforce using your credentials
2. Extract Opportunity and Account data
3. Apply transformations and quality checks
4. Create/update Supabase tables
5. Load data to Supabase

## Output

The pipeline creates two tables in your Supabase project:

### `opportunities_transformed`
Contains canonical opportunity data with 20 columns:
- `id`, `account_id`, `account_name`, `account_industry`
- `name`, `stage_name`, `stage_std`
- `amount`, `currency_iso_code`, `amount_usd`, `expected_revenue_usd`, `probability`
- `close_date`, `created_date`, `last_modified_date`, `sales_cycle_days`
- `owner_email_hash`, `phone_normalized`, `is_won`, `is_lost`
- `etl_loaded_at` (timestamp of when data was loaded)

### `opportunities_anomalies`
Contains data quality issues detected:
- `id` (serial primary key)
- `opportunity_id` (reference to opportunity)
- `code` (anomaly type: NEG_AMOUNT, PROB_OOB, FUTURE_CLOSE, etc.)
- `detail` (description of the issue)
- `detected_at` (timestamp)

## Reference Data

The pipeline uses reference data files for FX rates and stage mappings:
- `tests/sample_data/fx_rates.csv` - Currency exchange rates
- `tests/sample_data/stage_mapping.csv` - Stage name standardization rules

In production, these could be extracted from Salesforce or other data sources.

## Error Handling

The pipeline includes error handling for:
- Salesforce authentication failures
- Missing required fields in extracted data
- Transformation errors
- Supabase connection issues

If the pipeline fails, check:
1. Salesforce credentials are correct
2. Network connectivity to Salesforce and Supabase
3. Required permissions in Salesforce (read access to Opportunity and Account objects)
4. Supabase project exists and is accessible

## Scheduling

To run this pipeline on a schedule, you can use:
- **Cron**: Add to crontab for periodic execution
- **Airflow**: Create a DAG that calls `etl_pipeline.py`
- **GitHub Actions**: Set up a workflow with scheduled triggers
- **Cloud Functions**: Deploy as a serverless function with timer trigger

Example cron entry (daily at 2 AM):
```
0 2 * * * cd /path/to/business-logic && source .venv/bin/activate && python etl_pipeline.py >> etl.log 2>&1
```

## Troubleshooting

**Authentication Error**: Verify your Salesforce credentials and security token. The security token is required when authenticating from outside Salesforce's trusted IP ranges.

**Transformation Error**: Check that all required fields are present in the Salesforce data. Review error messages for missing columns.

**Supabase Error**: Verify your Supabase project is active and you have permission to create tables and insert data.

**No Data Extracted**: Verify SOQL queries match your Salesforce schema. Some orgs may have custom field names or restricted access.
