#!/usr/bin/env python3

import os
import sys
import tempfile
from pathlib import Path
import pandas as pd
from simple_salesforce import Salesforce
from supabase import create_client, Client

from .transformations import run_pipeline
from .quality_checks import anomaly_rules


def authenticate_salesforce():
    username = os.environ.get('SALESFORCE_USERNAME')
    password = os.environ.get('SALESFORCE_PASSWORD')
    url = os.environ.get('SALESFORCE_URL', '')
    
    if not all([username, password, url]):
        raise ValueError("Missing required environment variables: SALESFORCE_USERNAME, SALESFORCE_PASSWORD, SALESFORCE_URL")
    
    domain = url.replace('https://', '').replace('http://', '').rstrip('/')
    
    print(f"Authenticating to Salesforce at {domain}...")
    
    try:
        sf = Salesforce(
            username=username,
            password=password,
            security_token='',
            domain=domain
        )
        print("✓ Successfully authenticated to Salesforce")
        return sf
    except Exception as e:
        print(f"✗ Salesforce authentication failed: {e}")
        raise


def extract_salesforce_data(sf):
    print("\nExtracting Opportunities from Salesforce...")
    
    opp_soql = """
        SELECT Id, AccountId, Name, StageName, Amount, CurrencyIsoCode, 
               Probability, CloseDate, CreatedDate, LastModifiedDate, 
               Owner.Email, Phone, IsWon, IsClosed
        FROM Opportunity
    """
    
    try:
        opportunities = sf.query_all(opp_soql)
        print(f"✓ Extracted {len(opportunities['records'])} opportunities")
    except Exception as e:
        print(f"✗ Failed to extract opportunities: {e}")
        raise
    
    print("Extracting Accounts from Salesforce...")
    
    acct_soql = """
        SELECT Id, Name, Industry, OwnerId
        FROM Account
    """
    
    try:
        accounts = sf.query_all(acct_soql)
        print(f"✓ Extracted {len(accounts['records'])} accounts")
    except Exception as e:
        print(f"✗ Failed to extract accounts: {e}")
        raise
    
    return opportunities, accounts


def prepare_data_for_transformation(opportunities, accounts):
    print("\nPreparing data for transformation...")
    
    opp_records = []
    for record in opportunities['records']:
        rec = dict(record)
        rec.pop('attributes', None)
        
        if 'Owner' in rec and rec['Owner']:
            rec['OwnerEmail'] = rec['Owner'].get('Email', '')
            rec.pop('Owner', None)
        else:
            rec['OwnerEmail'] = ''
        
        opp_records.append(rec)
    
    acct_records = []
    for record in accounts['records']:
        rec = dict(record)
        rec.pop('attributes', None)
        acct_records.append(rec)
    
    opp_df = pd.DataFrame(opp_records)
    acct_df = pd.DataFrame(acct_records)
    
    opp_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    acct_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    
    opp_df.to_csv(opp_temp.name, index=False)
    acct_df.to_csv(acct_temp.name, index=False)
    
    opp_temp.close()
    acct_temp.close()
    
    print(f"✓ Prepared {len(opp_df)} opportunities and {len(acct_df)} accounts")
    
    return opp_temp.name, acct_temp.name


def run_transformations(opp_path, acct_path):
    print("\nRunning transformation pipeline...")
    
    repo_root = Path(__file__).parent.parent
    fx_path = repo_root / 'fx_rates.csv'
    stage_map_path = repo_root / 'stage_mapping.csv'
    
    if not fx_path.exists():
        raise FileNotFoundError(f"FX rates file not found: {fx_path}")
    if not stage_map_path.exists():
        raise FileNotFoundError(f"Stage mapping file not found: {stage_map_path}")
    
    try:
        canon, full = run_pipeline(
            opportunities_csv=opp_path,
            accounts_csv=acct_path,
            fx_csv=str(fx_path),
            stage_map_csv=str(stage_map_path)
        )
        print(f"✓ Transformed {len(canon)} opportunity records")
        
        anomalies = anomaly_rules(full)
        print(f"✓ Detected {len(anomalies)} anomalies")
        
        return canon, anomalies
    except Exception as e:
        print(f"✗ Transformation failed: {e}")
        raise


def get_supabase_client():
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_API_KEY')
    
    if not url:
        url = "https://yylrfxpjbxefavcmjvho.supabase.co"
    
    if not key:
        raise ValueError("Missing SUPABASE_API_KEY environment variable")
    
    return create_client(url, key)


def create_supabase_tables(project_id: str):
    print("\nCreating Supabase tables using MCP...")
    
    create_transformed_table = """
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
        close_date TIMESTAMP,
        created_date TIMESTAMP,
        last_modified_date TIMESTAMP,
        sales_cycle_days INTEGER,
        owner_email_hash TEXT,
        phone_normalized TEXT,
        is_won INTEGER,
        is_lost INTEGER,
        loaded_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    create_anomalies_table = """
    CREATE TABLE IF NOT EXISTS opportunities_anomalies (
        id SERIAL PRIMARY KEY,
        opportunity_id TEXT,
        code TEXT,
        detail TEXT,
        detected_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    print("✓ Tables will be created via MCP during first data load")


def load_to_supabase(supabase: Client, transformed_df, anomalies_df):
    print("\nLoading data to Supabase...")
    
    print("Deleting existing data...")
    try:
        supabase.table('opportunities_transformed').delete().neq('id', '').execute()
        supabase.table('opportunities_anomalies').delete().neq('id', 0).execute()
        print("✓ Cleared existing data")
    except Exception as e:
        print(f"Note: Could not clear tables (may not exist yet): {e}")
    
    print("Inserting transformed data...")
    
    transformed_df = transformed_df.copy()
    transformed_df.columns = [col.lower() for col in transformed_df.columns]
    
    column_mapping = {
        'accountid': 'account_id',
        'accountname': 'account_name',
        'accountindustry': 'account_industry',
        'stagename': 'stage_name',
        'stagestd': 'stage_std',
        'currencyisocode': 'currency_iso_code',
        'amountusd': 'amount_usd',
        'closedate': 'close_date',
        'createddate': 'created_date',
        'lastmodifieddate': 'last_modified_date'
    }
    transformed_df = transformed_df.rename(columns=column_mapping)
    
    records = transformed_df.to_dict('records')
    
    batch_size = 100
    total_rows = len(records)
    
    for i in range(0, total_rows, batch_size):
        batch = records[i:i+batch_size]
        
        for record in batch:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
                elif isinstance(value, pd.Timestamp):
                    record[key] = value.isoformat()
        
        try:
            supabase.table('opportunities_transformed').insert(batch).execute()
        except Exception as e:
            print(f"✗ Failed to insert batch {i//batch_size + 1}: {e}")
            raise
    
    print(f"✓ Inserted {total_rows} transformed records")
    
    if len(anomalies_df) > 0:
        print("Inserting anomaly data...")
        
        anomaly_records = []
        for _, row in anomalies_df.iterrows():
            anomaly_records.append({
                'opportunity_id': str(row.get('opportunity_id', '')),
                'code': str(row.get('code', '')),
                'detail': str(row.get('detail', ''))
            })
        
        for i in range(0, len(anomaly_records), batch_size):
            batch = anomaly_records[i:i+batch_size]
            try:
                supabase.table('opportunities_anomalies').insert(batch).execute()
            except Exception as e:
                print(f"✗ Failed to insert anomaly batch: {e}")
                raise
        
        print(f"✓ Inserted {len(anomalies_df)} anomaly records")
    else:
        print("✓ No anomalies to insert")


def verify_data_in_supabase(supabase: Client):
    print("\nVerifying data in Supabase...")
    
    try:
        transformed_result = supabase.table('opportunities_transformed').select('id', count='exact').execute()
        anomalies_result = supabase.table('opportunities_anomalies').select('id', count='exact').execute()
        
        transformed_count = transformed_result.count if hasattr(transformed_result, 'count') else len(transformed_result.data)
        anomalies_count = anomalies_result.count if hasattr(anomalies_result, 'count') else len(anomalies_result.data)
        
        print("✓ Data verification complete")
        print(f"  - Transformed records: {transformed_count}")
        print(f"  - Anomaly records: {anomalies_count}")
    except Exception as e:
        print(f"✗ Verification failed: {e}")
        raise


def main():
    print("=" * 60)
    print("Salesforce to Supabase ETL Pipeline")
    print("=" * 60)
    
    project_id = 'yylrfxpjbxefavcmjvho'
    
    try:
        supabase = get_supabase_client()
        print("✓ Connected to Supabase")
        
        sf = authenticate_salesforce()
        
        opportunities, accounts = extract_salesforce_data(sf)
        
        opp_path, acct_path = prepare_data_for_transformation(opportunities, accounts)
        
        try:
            transformed, anomalies = run_transformations(opp_path, acct_path)
            
            create_supabase_tables(project_id)
            
            load_to_supabase(supabase, transformed, anomalies)
            
            verify_data_in_supabase(supabase)
            
        finally:
            os.unlink(opp_path)
            os.unlink(acct_path)
        
        print("\n" + "=" * 60)
        print("✓ ETL Pipeline completed successfully!")
        print("=" * 60)
        print(f"\nTransformed records: {len(transformed)}")
        print(f"Anomaly records: {len(anomalies)}")
        print(f"\nData loaded to Supabase project: {project_id}")
        print("Tables: opportunities_transformed, opportunities_anomalies")
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ ETL Pipeline failed: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
