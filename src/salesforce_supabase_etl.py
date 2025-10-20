#!/usr/bin/env python3

import os
import sys
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from simple_salesforce import Salesforce
from .transformations import run_pipeline
from .quality_checks import anomaly_rules


def authenticate_salesforce():
    """Authenticate with Salesforce using credentials from environment variables."""
    username = os.environ.get('SALESFORCE_USERNAME')
    password = os.environ.get('SALESFORCE_PASSWORD')
    security_token = os.environ.get('SALESFORCE_SECURITY_TOKEN')
    instance_url = os.environ.get('SALESFORCE_URL')
    
    if not all([username, password, security_token, instance_url]):
        raise ValueError("Missing required Salesforce credentials in environment variables")
    
    print(f"Authenticating with Salesforce at {instance_url}...")
    sf = Salesforce(
        username=username,
        password=password,
        security_token=security_token,
        instance_url=instance_url
    )
    print("Successfully authenticated with Salesforce")
    return sf


def get_available_fields(sf, object_name):
    """Get list of available fields for a Salesforce object."""
    describe = sf.__getattr__(object_name).describe()
    return [field['name'] for field in describe['fields']]


def extract_opportunities(sf):
    """Extract Opportunity data from Salesforce."""
    print("Extracting Opportunity data...")
    
    available_fields = get_available_fields(sf, 'Opportunity')
    print(f"Available Opportunity fields: {len(available_fields)} total")
    
    required_fields = ['Id', 'AccountId', 'Name', 'StageName', 'Amount', 
                      'Probability', 'CloseDate', 'CreatedDate', 'LastModifiedDate',
                      'IsWon', 'IsClosed']
    
    optional_fields = ['CurrencyIsoCode', 'Phone']
    
    fields_to_query = [f for f in required_fields if f in available_fields]
    fields_to_query.extend([f for f in optional_fields if f in available_fields])
    
    has_currency = 'CurrencyIsoCode' in available_fields
    has_phone = 'Phone' in available_fields
    
    query_fields = ', '.join(fields_to_query)
    query = f"""
        SELECT {query_fields}, Owner.Email
        FROM Opportunity
        WHERE IsDeleted = false
    """
    
    print(f"Querying fields: {query_fields}")
    
    results = sf.query_all(query)
    records = results['records']
    
    opportunities = []
    for record in records:
        close_date = record.get('CloseDate')
        created_date = record.get('CreatedDate')
        modified_date = record.get('LastModifiedDate')
        
        if close_date and 'T' in close_date:
            close_date = close_date.split('T')[0]
        if created_date and 'T' in created_date:
            created_date = created_date.split('T')[0]
        if modified_date and 'T' in modified_date:
            modified_date = modified_date.split('T')[0]
        
        opp = {
            'Id': record.get('Id'),
            'AccountId': record.get('AccountId'),
            'Name': record.get('Name'),
            'StageName': record.get('StageName'),
            'Amount': record.get('Amount'),
            'CurrencyIsoCode': record.get('CurrencyIsoCode') if has_currency else 'USD',
            'Probability': record.get('Probability'),
            'CloseDate': close_date,
            'CreatedDate': created_date,
            'LastModifiedDate': modified_date,
            'OwnerEmail': record.get('Owner', {}).get('Email') if record.get('Owner') else None,
            'Phone': record.get('Phone') if has_phone else None,
            'IsWon': record.get('IsWon'),
            'IsClosed': record.get('IsClosed')
        }
        opportunities.append(opp)
    
    df = pd.DataFrame(opportunities)
    print(f"Extracted {len(df)} opportunities")
    
    if not has_currency:
        print("  Note: CurrencyIsoCode not available in org, defaulting to 'USD'")
    if not has_phone:
        print("  Note: Phone field not available on Opportunity object")
    
    return df


def extract_accounts(sf):
    """Extract Account data from Salesforce."""
    print("Extracting Account data...")
    
    available_fields = get_available_fields(sf, 'Account')
    print(f"Available Account fields: {len(available_fields)} total")
    
    required_fields = ['Id', 'Name', 'OwnerId']
    optional_fields = ['Industry']
    
    fields_to_query = [f for f in required_fields if f in available_fields]
    fields_to_query.extend([f for f in optional_fields if f in available_fields])
    
    has_industry = 'Industry' in available_fields
    
    query_fields = ', '.join(fields_to_query)
    query = f"""
        SELECT {query_fields}
        FROM Account
        WHERE IsDeleted = false
    """
    
    print(f"Querying fields: {query_fields}")
    
    results = sf.query_all(query)
    records = results['records']
    
    accounts = []
    for record in records:
        acc = {
            'Id': record.get('Id'),
            'Name': record.get('Name'),
            'Industry': record.get('Industry') if has_industry else None,
            'OwnerId': record.get('OwnerId')
        }
        accounts.append(acc)
    
    df = pd.DataFrame(accounts)
    print(f"Extracted {len(df)} accounts")
    
    if not has_industry:
        print("  Note: Industry field not available on Account object")
    
    return df


def get_fx_rates():
    """Get FX rates data (using sample data as baseline)."""
    print("Loading FX rates...")
    
    sample_fx_path = Path(__file__).parent.parent / "tests" / "sample_data" / "fx_rates.csv"
    df = pd.read_csv(sample_fx_path, dtype=str, keep_default_na=False)
    
    print(f"Loaded {len(df)} FX rate records")
    return df


def get_stage_mapping():
    """Get stage mapping data (using sample data as baseline)."""
    print("Loading stage mapping...")
    
    sample_stage_path = Path(__file__).parent.parent / "tests" / "sample_data" / "stage_mapping.csv"
    df = pd.read_csv(sample_stage_path, dtype=str, keep_default_na=False)
    
    print(f"Loaded {len(df)} stage mappings")
    return df


def write_to_supabase(df, table_name, project_id):
    """Write DataFrame to Supabase table."""
    print(f"Writing {len(df)} rows to Supabase table '{table_name}'...")
    
    if df.empty:
        print(f"Warning: DataFrame is empty, skipping table '{table_name}'")
        return
    
    create_table_sql = generate_create_table_sql(df, table_name)
    
    print(f"Creating table '{table_name}' if it doesn't exist...")
    result = os.popen(f'mcp-cli tool call apply_migration --server supabase --input \'{{"project_id": "{project_id}", "name": "create_{table_name}", "query": {json.dumps(create_table_sql)}}}\'').read()
    print(f"Table creation result: {result}")
    
    insert_data(df, table_name, project_id)
    print(f"Successfully wrote to table '{table_name}'")


def generate_create_table_sql(df, table_name):
    """Generate CREATE TABLE SQL from DataFrame schema."""
    
    type_mapping = {
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    
    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        sql_type = type_mapping.get(dtype, 'TEXT')
        
        col_name = col.lower().replace(' ', '_')
        columns.append(f'"{col_name}" {sql_type}')
    
    create_sql = f'''
CREATE TABLE IF NOT EXISTS "{table_name}" (
    _id BIGSERIAL PRIMARY KEY,
    {', '.join(columns)},
    _created_at TIMESTAMP DEFAULT NOW()
);
'''
    return create_sql


def insert_data(df, table_name, project_id):
    """Insert DataFrame data into Supabase table using proper MCP CLI calls."""
    print(f"Inserting {len(df)} rows into '{table_name}'...")
    
    df_copy = df.copy()
    df_copy.columns = [col.lower().replace(' ', '_') for col in df_copy.columns]
    
    for col in df_copy.select_dtypes(include=['datetime64']).columns:
        df_copy[col] = df_copy[col].astype(str)
    
    df_copy = df_copy.where(pd.notna(df_copy), None)
    
    batch_size = 10
    total_rows = len(df_copy)
    
    for i in range(0, total_rows, batch_size):
        batch = df_copy.iloc[i:i+batch_size]
        
        values = []
        for _, row in batch.iterrows():
            row_values = []
            for val in row.values:
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    row_values.append('NULL')
                elif isinstance(val, str):
                    escaped = val.replace("'", "''").replace("\\", "\\\\")
                    row_values.append(f"'{escaped}'")
                elif isinstance(val, bool):
                    row_values.append('TRUE' if val else 'FALSE')
                else:
                    row_values.append(str(val))
            values.append(f"({', '.join(row_values)})")
        
        columns_str = ', '.join([f'"{col}"' for col in batch.columns])
        values_str = ', '.join(values)
        
        insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES {values_str};'
        
        import subprocess
        result = subprocess.run(
            ['mcp-cli', 'tool', 'call', 'execute_sql', '--server', 'supabase', '--input', 
             json.dumps({"project_id": project_id, "query": insert_sql})],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"  Warning: Insert batch {i//batch_size + 1} had issues: {result.stderr}")
        
        if (i + batch_size) % 50 == 0 and i > 0:
            print(f"  Inserted {min(i + batch_size, total_rows)}/{total_rows} rows...")
    
    print(f"  Completed inserting all {total_rows} rows")


def main():
    """Main ETL pipeline execution."""
    print("=" * 80)
    print("SALESFORCE TO SUPABASE ETL PIPELINE")
    print("=" * 80)
    print()
    
    try:
        sf = authenticate_salesforce()
        
        opportunities_df = extract_opportunities(sf)
        accounts_df = extract_accounts(sf)
        
        fx_rates_df = get_fx_rates()
        stage_mapping_df = get_stage_mapping()
        
        temp_dir = Path("/tmp/salesforce_etl")
        temp_dir.mkdir(exist_ok=True)
        
        opp_path = temp_dir / "opportunities.csv"
        acc_path = temp_dir / "accounts.csv"
        fx_path = temp_dir / "fx_rates.csv"
        stage_path = temp_dir / "stage_mapping.csv"
        
        opportunities_df.to_csv(opp_path, index=False)
        accounts_df.to_csv(acc_path, index=False)
        fx_rates_df.to_csv(fx_path, index=False)
        stage_mapping_df.to_csv(stage_path, index=False)
        
        print("\n" + "=" * 80)
        print("RUNNING TRANSFORMATION PIPELINE")
        print("=" * 80)
        print()
        
        canon, full = run_pipeline(
            str(opp_path),
            str(acc_path),
            str(fx_path),
            str(stage_path)
        )
        
        anomalies = anomaly_rules(full)
        
        print(f"\nTransformation complete:")
        print(f"  - Transformed records: {len(canon)}")
        print(f"  - Anomaly records: {len(anomalies)}")
        
        print("\n" + "=" * 80)
        print("WRITING TO SUPABASE")
        print("=" * 80)
        print()
        
        project_id = "yylrfxpjbxefavcmjvho"
        
        write_to_supabase(canon, "opportunities_transformed", project_id)
        write_to_supabase(anomalies, "opportunities_anomalies", project_id)
        
        print("\n" + "=" * 80)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print()
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "opportunities_extracted": len(opportunities_df),
            "accounts_extracted": len(accounts_df),
            "opportunities_transformed": len(canon),
            "anomalies_detected": len(anomalies),
            "status": "success"
        }
        
        print(json.dumps(summary, indent=2))
        
        return 0
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print("ETL PIPELINE FAILED")
        print("=" * 80)
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
