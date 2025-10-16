# Salesforce to Supabase ETL Pipeline

This automated ETL pipeline extracts Opportunity and Account data from Salesforce, runs the transformation logic, and loads the results into Supabase.

## Prerequisites

1. **Environment Variables**: Set the following environment variables:
   ```bash
   export SALESFORCE_USERNAME="your-salesforce-username"
   export SALESFORCE_PASSWORD="your-salesforce-password"
   export SALESFORCE_URL="https://your-instance.salesforce.com"
   ```

2. **Python Dependencies**: Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. **Reference Data**: The pipeline uses two reference data files located in the repository root:
   - `fx_rates.csv` - Foreign exchange rates for currency conversion
   - `stage_mapping.csv` - Mapping from Salesforce stages to standardized stages

## Usage

Run the ETL pipeline:

```bash
cd ~/repos/business-logic
source .venv/bin/activate
python -m src.salesforce_to_supabase_etl
```

## What It Does

1. **Authenticate to Salesforce**: Uses username/password authentication via the simple-salesforce library
2. **Extract Data**: Pulls Opportunity and Account records using SOQL queries
3. **Transform Data**: 
   - Deduplicates records by ID using latest LastModifiedDate
   - Normalizes stages via mapping table
   - Enriches opportunities with account information
   - Converts amounts to USD using FX rates
   - Calculates metrics (expected revenue, sales cycle days, win/loss flags)
   - Sanitizes PII (hashes emails, normalizes phone numbers)
4. **Detect Anomalies**: Runs data quality checks for:
   - Negative amounts
   - Probabilities outside 0-100 range
   - Future close dates
   - Missing stage mappings
   - Missing FX rates
5. **Load to Supabase**: Creates and populates two tables:
   - `opportunities_transformed` - Clean, transformed opportunity data
   - `opportunities_anomalies` - Data quality issues detected

## Output Tables

### opportunities_transformed
Contains the canonical, transformed opportunity data with columns:
- `id`, `account_id`, `account_name`, `account_industry`, `name`
- `stage_name`, `stage_std`
- `amount`, `currency_iso_code`, `amount_usd`, `expected_revenue_usd`, `probability`
- `close_date`, `created_date`, `last_modified_date`, `sales_cycle_days`
- `owner_email_hash`, `phone_normalized`, `is_won`, `is_lost`
- `loaded_at` (timestamp of when data was loaded)

### opportunities_anomalies
Contains data quality issues with columns:
- `id` (auto-incrementing primary key)
- `opportunity_id` (references the opportunity with the issue)
- `code` (anomaly type: NEG_AMOUNT, PROB_OOB, FUTURE_CLOSE, MISSING_STAGE_MAP, MISSING_FX)
- `detail` (human-readable description)
- `detected_at` (timestamp of when anomaly was detected)

## Error Handling

The pipeline includes error handling for:
- Salesforce authentication failures
- SOQL query errors
- Data validation errors from transformation logic
- Supabase connection and loading issues

Any errors will be displayed with clear messages and the pipeline will exit with a non-zero status code.

## Supabase Project

Data is loaded to the Supabase project:
- Organization: Ben Lehrburger's Org
- Project: Firefly
- Project ID: yylrfxpjbxefavcmjvho

## Customization

To modify the reference data:
- Edit `fx_rates.csv` to update currency exchange rates
- Edit `stage_mapping.csv` to update stage name mappings

To modify the transformation logic:
- See `src/transformations.py` for data transformation functions
- See `src/quality_checks.py` for anomaly detection rules
