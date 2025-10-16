#!/usr/bin/env python3
"""
Automated ETL Pipeline: Salesforce -> Transformation -> Supabase

This script orchestrates the complete ETL workflow:
1. Extract Opportunity and Account data from Salesforce via SOQL
2. Apply business transformation logic from src/transformations.py
3. Run data quality checks from src/quality_checks.py
4. Load transformed and anomaly data to Supabase via MCP
"""

import os
import sys
import json
import subprocess
import tempfile
from pathlib import Path
from simple_salesforce import Salesforce
import pandas as pd

from src.transformations import run_pipeline
from src.quality_checks import anomaly_rules


def authenticate_salesforce():
    """Authenticate with Salesforce using credentials from environment variables."""
    print("Authenticating with Salesforce...")
    
    username = os.environ.get('SALESFORCE_USERNAME')
    password = os.environ.get('SALESFORCE_PASSWORD')
    security_token = os.environ.get('SALESFORCE_SECURITY_TOKEN')
    instance_url = os.environ.get('SALESFORCE_URL')
    
    if not all([username, password, security_token, instance_url]):
        raise ValueError("Missing required Salesforce credentials in environment variables")
    
    sf = Salesforce(
        username=username,
        password=password,
        security_token=security_token,
        instance_url=instance_url
    )
    
    print(f"✓ Successfully authenticated to Salesforce: {instance_url}")
    return sf


def get_available_fields(sf, object_name, desired_fields):
    """Check which fields are available on a Salesforce object."""
    describe = sf.__getattr__(object_name).describe()
    available_fields = {field['name'] for field in describe['fields']}
    
    fields_to_query = []
    missing_fields = []
    
    for field in desired_fields:
        if field in available_fields:
            fields_to_query.append(field)
        else:
            missing_fields.append(field)
    
    return fields_to_query, missing_fields



def extract_opportunities(sf, output_path):
    """Extract Opportunity data from Salesforce and save to CSV."""
    print("Extracting Opportunity data from Salesforce...")
    
    desired_fields = [
        'Id', 'AccountId', 'Name', 'StageName', 'Amount',
        'CurrencyIsoCode', 'Probability', 'CloseDate', 'CreatedDate',
        'LastModifiedDate', 'Phone', 'IsWon', 'IsClosed'
    ]
    
    available_fields, missing_fields = get_available_fields(sf, 'Opportunity', desired_fields)
    
    if missing_fields:
        print(f"  Note: Fields not available in this org: {', '.join(missing_fields)}")
    
    fields_str = ', '.join(available_fields)
    query = f"""
    SELECT 
        {fields_str},
        Owner.Email
    FROM Opportunity
    WHERE IsDeleted = false
    ORDER BY CreatedDate DESC
    """
    
    results = sf.query_all(query)
    records = results['records']
    
    for record in records:
        if 'Owner' in record and record['Owner']:
            record['OwnerEmail'] = record['Owner'].get('Email', '')
        else:
            record['OwnerEmail'] = ''
        
        if 'CurrencyIsoCode' not in record:
            record['CurrencyIsoCode'] = 'USD'
        
        record.pop('Owner', None)
        record.pop('attributes', None)
    
    df = pd.DataFrame(records)
    
    required_columns = [
        'Id', 'AccountId', 'Name', 'StageName', 'Amount', 'CurrencyIsoCode',
        'Probability', 'CloseDate', 'CreatedDate', 'LastModifiedDate',
        'OwnerEmail', 'Phone', 'IsWon', 'IsClosed'
    ]
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = '' if col in ['OwnerEmail', 'Phone', 'CurrencyIsoCode'] else None
    
    datetime_cols = ['CloseDate', 'CreatedDate', 'LastModifiedDate']
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)
    
    df.to_csv(output_path, index=False)
    
    print(f"✓ Extracted {len(df)} opportunities to {output_path}")
    return df


def extract_accounts(sf, output_path):
    """Extract Account data from Salesforce and save to CSV."""
    print("Extracting Account data from Salesforce...")
    
    desired_fields = ['Id', 'Name', 'Industry', 'OwnerId']
    
    available_fields, missing_fields = get_available_fields(sf, 'Account', desired_fields)
    
    if missing_fields:
        print(f"  Note: Fields not available in this org: {', '.join(missing_fields)}")
    
    fields_str = ', '.join(available_fields)
    query = f"""
    SELECT 
        {fields_str}
    FROM Account
    WHERE IsDeleted = false
    """
    
    results = sf.query_all(query)
    records = results['records']
    
    for record in records:
        record.pop('attributes', None)
        
        for field in desired_fields:
            if field not in record:
                record[field] = ''
    
    df = pd.DataFrame(records)
    
    for col in desired_fields:
        if col not in df.columns:
            df[col] = ''
    
    df.to_csv(output_path, index=False)
    
    print(f"✓ Extracted {len(df)} accounts to {output_path}")
    return df


def run_transformation(opportunities_csv, accounts_csv, fx_csv, stage_map_csv):
    """Run the existing transformation pipeline."""
    print("\nRunning transformation pipeline...")
    
    try:
        canon, full = run_pipeline(
            opportunities_csv=opportunities_csv,
            accounts_csv=accounts_csv,
            fx_csv=fx_csv,
            stage_map_csv=stage_map_csv
        )
        print(f"✓ Transformation complete: {len(canon)} canonical records, {len(full)} full records")
        return canon, full
    except Exception as e:
        print(f"✗ Transformation failed: {str(e)}")
        raise


def run_quality_checks(full_df):
    """Run data quality checks to detect anomalies."""
    print("\nRunning data quality checks...")
    
    try:
        anomalies = anomaly_rules(full_df)
        print(f"✓ Quality checks complete: {len(anomalies)} anomalies detected")
        return anomalies
    except Exception as e:
        print(f"✗ Quality checks failed: {str(e)}")
        raise


def mcp_execute(server, tool_name, input_args):
    """Execute an MCP tool and return the result."""
    input_json = json.dumps(input_args) if input_args else '{}'
    
    result = subprocess.run(
        ['mcp-cli', 'tool', 'call', tool_name, '--server', server, '--input', input_json],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"MCP command failed: {result.stderr}")
    
    output_lines = result.stdout.strip().split('\n')
    result_started = False
    result_lines = []
    
    for line in output_lines:
        if 'Tool result:' in line:
            result_started = True
            continue
        if result_started:
            result_lines.append(line)
    
    if result_lines:
        result_text = '\n'.join(result_lines).strip()
        try:
            return json.loads(result_text)
        except json.JSONDecodeError:
            return result_text
    
    return None


def get_supabase_project_id():
    """Get the Supabase project ID for 'Firefly'."""
    print("\nFinding Supabase project 'Firefly'...")
    
    projects = mcp_execute('supabase', 'list_projects', {})
    
    if not projects:
        raise ValueError("No Supabase projects found")
    
    for project in projects:
        if project.get('name') == 'Firefly':
            project_id = project.get('id')
            print(f"✓ Found Supabase project 'Firefly': {project_id}")
            return project_id
    
    raise ValueError("Supabase project 'Firefly' not found")


def create_supabase_tables(project_id):
    """Create tables in Supabase for transformed opportunities and anomalies."""
    print("\nCreating Supabase tables...")
    
    create_opportunities_sql = """
    CREATE TABLE IF NOT EXISTS opportunities_transformed (
        id TEXT PRIMARY KEY,
        account_id TEXT,
        account_name TEXT,
        account_industry TEXT,
        name TEXT,
        stage_name TEXT,
        stage_std TEXT,
        amount NUMERIC,
        currency_iso_code TEXT,
        amount_usd NUMERIC,
        expected_revenue_usd NUMERIC,
        probability NUMERIC,
        close_date DATE,
        created_date TIMESTAMP,
        last_modified_date TIMESTAMP,
        sales_cycle_days INTEGER,
        owner_email_hash TEXT,
        phone_normalized TEXT,
        is_won INTEGER,
        is_lost INTEGER,
        etl_loaded_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    mcp_execute('supabase', 'execute_sql', {
        'project_id': project_id,
        'query': create_opportunities_sql
    })
    print("✓ Created/verified opportunities_transformed table")
    
    create_anomalies_sql = """
    CREATE TABLE IF NOT EXISTS opportunities_anomalies (
        id SERIAL PRIMARY KEY,
        opportunity_id TEXT,
        code TEXT,
        detail TEXT,
        detected_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    mcp_execute('supabase', 'execute_sql', {
        'project_id': project_id,
        'query': create_anomalies_sql
    })
    print("✓ Created/verified opportunities_anomalies table")


def load_to_supabase(project_id, canon_df, anomalies_df):
    """Load transformed and anomaly data to Supabase."""
    print("\nLoading data to Supabase...")
    
    mcp_execute('supabase', 'execute_sql', {
        'project_id': project_id,
        'query': 'DELETE FROM opportunities_transformed; DELETE FROM opportunities_anomalies;'
    })
    print("✓ Cleared existing data")
    
    if len(canon_df) > 0:
        canon_df_copy = canon_df.copy()
        canon_df_copy.columns = [col.lower().replace('id', '_id') if col != 'Id' else 'id' 
                                   for col in canon_df_copy.columns]
        
        column_mapping = {
            'id': 'id',
            'account_id': 'account_id',
            'accountname': 'account_name',
            'accountindustry': 'account_industry',
            'name': 'name',
            'stagename': 'stage_name',
            'stagestd': 'stage_std',
            'amount': 'amount',
            'currencyisocode': 'currency_iso_code',
            'amountusd': 'amount_usd',
            'expected_revenue_usd': 'expected_revenue_usd',
            'probability': 'probability',
            'closedate': 'close_date',
            'createddate': 'created_date',
            'lastmodifieddate': 'last_modified_date',
            'sales_cycle_days': 'sales_cycle_days',
            'owner_email_hash': 'owner_email_hash',
            'phone_normalized': 'phone_normalized',
            'is_won': 'is_won',
            'is_lost': 'is_lost'
        }
        canon_df_copy.rename(columns=column_mapping, inplace=True)
        
        records = []
        for _, row in canon_df_copy.iterrows():
            record = {}
            for col in canon_df_copy.columns:
                val = row[col]
                if pd.isna(val):
                    record[col] = None
                elif isinstance(val, pd.Timestamp):
                    record[col] = val.isoformat()
                else:
                    record[col] = val
            records.append(record)
        
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            
            columns = list(batch[0].keys())
            values_list = []
            
            for record in batch:
                values = []
                for col in columns:
                    val = record[col]
                    if val is None:
                        values.append('NULL')
                    elif isinstance(val, str):
                        escaped = val.replace("'", "''")
                        values.append(f"'{escaped}'")
                    else:
                        values.append(str(val))
                values_list.append(f"({', '.join(values)})")
            
            insert_sql = f"""
            INSERT INTO opportunities_transformed ({', '.join(columns)})
            VALUES {', '.join(values_list)};
            """
            
            mcp_execute('supabase', 'execute_sql', {
                'project_id': project_id,
                'query': insert_sql
            })
        
        print(f"✓ Loaded {len(canon_df)} records to opportunities_transformed")
    
    if len(anomalies_df) > 0:
        anomalies_copy = anomalies_df.copy()
        
        batch_size = 100
        for i in range(0, len(anomalies_copy), batch_size):
            batch = anomalies_copy.iloc[i:i+batch_size]
            
            values_list = []
            for _, row in batch.iterrows():
                opp_id = row['opportunity_id'].replace("'", "''") if pd.notna(row['opportunity_id']) else ''
                code = row['code'].replace("'", "''") if pd.notna(row['code']) else ''
                detail = row['detail'].replace("'", "''") if pd.notna(row['detail']) else ''
                values_list.append(f"('{opp_id}', '{code}', '{detail}')")
            
            insert_sql = f"""
            INSERT INTO opportunities_anomalies (opportunity_id, code, detail)
            VALUES {', '.join(values_list)};
            """
            
            mcp_execute('supabase', 'execute_sql', {
                'project_id': project_id,
                'query': insert_sql
            })
        
        print(f"✓ Loaded {len(anomalies_df)} anomalies to opportunities_anomalies")


def main():
    """Main ETL pipeline orchestrator."""
    print("=" * 80)
    print("Starting Salesforce -> Supabase ETL Pipeline")
    print("=" * 80)
    
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            
            sf = authenticate_salesforce()
            
            opportunities_csv = tmpdir / 'opportunities.csv'
            accounts_csv = tmpdir / 'accounts.csv'
            
            extract_opportunities(sf, opportunities_csv)
            extract_accounts(sf, accounts_csv)
            
            fx_csv = 'tests/sample_data/fx_rates.csv'
            stage_map_csv = 'tests/sample_data/stage_mapping.csv'
            
            print(f"\nUsing reference data:")
            print(f"  - FX rates: {fx_csv}")
            print(f"  - Stage mapping: {stage_map_csv}")
            
            canon_df, full_df = run_transformation(
                opportunities_csv=str(opportunities_csv),
                accounts_csv=str(accounts_csv),
                fx_csv=fx_csv,
                stage_map_csv=stage_map_csv
            )
            
            anomalies_df = run_quality_checks(full_df)
            
            project_id = get_supabase_project_id()
            create_supabase_tables(project_id)
            load_to_supabase(project_id, canon_df, anomalies_df)
            
            print("\n" + "=" * 80)
            print("ETL Pipeline completed successfully!")
            print("=" * 80)
            print(f"\nSummary:")
            print(f"  - Opportunities extracted: {len(full_df)}")
            print(f"  - Canonical records: {len(canon_df)}")
            print(f"  - Anomalies detected: {len(anomalies_df)}")
            print(f"\nData loaded to Supabase project: Firefly ({project_id})")
            print(f"  - Table: opportunities_transformed")
            print(f"  - Table: opportunities_anomalies")
            
            return 0
            
    except Exception as e:
        print(f"\n✗ ETL Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
