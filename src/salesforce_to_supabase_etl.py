#!/usr/bin/env python3

import os
import sys
import tempfile
import json
import subprocess
from pathlib import Path
import pandas as pd
from simple_salesforce import Salesforce

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


def create_supabase_tables(project_id):
    print("\nCreating Supabase tables...")
    
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
    
    try:
        result = subprocess.run(
            ['mcp-cli', 'tool', 'call', 'apply_migration', '--server', 'supabase', '--input',
             json.dumps({
                 'project_id': project_id,
                 'name': 'create_opportunities_tables',
                 'query': create_transformed_table + '\n' + create_anomalies_table
             })],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"✗ Failed to create tables: {result.stderr}")
            raise Exception(f"Table creation failed: {result.stderr}")
        
        print("✓ Successfully created Supabase tables")
    except Exception as e:
        print(f"✗ Table creation failed: {e}")
        raise


def load_to_supabase(project_id, transformed_df, anomalies_df):
    print("\nLoading data to Supabase...")
    
    print("Truncating existing data...")
    truncate_sql = """
    TRUNCATE TABLE opportunities_transformed;
    TRUNCATE TABLE opportunities_anomalies;
    """
    
    try:
        subprocess.run(
            ['mcp-cli', 'tool', 'call', 'execute_sql', '--server', 'supabase', '--input',
             json.dumps({'project_id': project_id, 'query': truncate_sql})],
            capture_output=True,
            text=True,
            check=True
        )
        print("✓ Truncated existing data")
    except Exception as e:
        print(f"Warning: Could not truncate tables (may not exist yet): {e}")
    
    print("Inserting transformed data...")
    
    transformed_df = transformed_df.copy()
    transformed_df.columns = [col.lower() for col in transformed_df.columns]
    
    batch_size = 500
    total_rows = len(transformed_df)
    
    for i in range(0, total_rows, batch_size):
        batch = transformed_df.iloc[i:i+batch_size]
        
        values_list = []
        for _, row in batch.iterrows():
            values = []
            for col in ['id', 'accountid', 'accountname', 'accountindustry', 'name',
                       'stagename', 'stagestd', 'amount', 'currencyisocode', 'amountusd',
                       'expected_revenue_usd', 'probability', 'closedate', 'createddate',
                       'lastmodifieddate', 'sales_cycle_days', 'owner_email_hash',
                       'phone_normalized', 'is_won', 'is_lost']:
                val = row.get(col)
                if pd.isna(val):
                    values.append('NULL')
                elif isinstance(val, str):
                    values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                elif isinstance(val, pd.Timestamp):
                    values.append(f"'{val.isoformat()}'")
                else:
                    values.append(f"'{str(val).replace(chr(39), chr(39)+chr(39))}'")
            
            values_list.append(f"({', '.join(values)})")
        
        insert_sql = f"""
        INSERT INTO opportunities_transformed 
        (id, account_id, account_name, account_industry, name, stage_name, stage_std,
         amount, currency_iso_code, amount_usd, expected_revenue_usd, probability,
         close_date, created_date, last_modified_date, sales_cycle_days,
         owner_email_hash, phone_normalized, is_won, is_lost)
        VALUES {', '.join(values_list)};
        """
        
        try:
            subprocess.run(
                ['mcp-cli', 'tool', 'call', 'execute_sql', '--server', 'supabase', '--input',
                 json.dumps({'project_id': project_id, 'query': insert_sql})],
                capture_output=True,
                text=True,
                check=True
            )
        except Exception as e:
            print(f"✗ Failed to insert batch {i//batch_size + 1}: {e}")
            raise
    
    print(f"✓ Inserted {total_rows} transformed records")
    
    if len(anomalies_df) > 0:
        print("Inserting anomaly data...")
        
        for i in range(0, len(anomalies_df), batch_size):
            batch = anomalies_df.iloc[i:i+batch_size]
            
            values_list = []
            for _, row in batch.iterrows():
                opp_id = str(row.get('opportunity_id', '')).replace("'", "''")
                code = str(row.get('code', '')).replace("'", "''")
                detail = str(row.get('detail', '')).replace("'", "''")
                values_list.append(f"('{opp_id}', '{code}', '{detail}')")
            
            insert_sql = f"""
            INSERT INTO opportunities_anomalies (opportunity_id, code, detail)
            VALUES {', '.join(values_list)};
            """
            
            try:
                subprocess.run(
                    ['mcp-cli', 'tool', 'call', 'execute_sql', '--server', 'supabase', '--input',
                     json.dumps({'project_id': project_id, 'query': insert_sql})],
                    capture_output=True,
                    text=True,
                    check=True
                )
            except Exception as e:
                print(f"✗ Failed to insert anomaly batch: {e}")
                raise
        
        print(f"✓ Inserted {len(anomalies_df)} anomaly records")
    else:
        print("✓ No anomalies to insert")


def verify_data_in_supabase(project_id):
    print("\nVerifying data in Supabase...")
    
    verify_sql = """
    SELECT 
        (SELECT COUNT(*) FROM opportunities_transformed) as transformed_count,
        (SELECT COUNT(*) FROM opportunities_anomalies) as anomalies_count;
    """
    
    try:
        result = subprocess.run(
            ['mcp-cli', 'tool', 'call', 'execute_sql', '--server', 'supabase', '--input',
             json.dumps({'project_id': project_id, 'query': verify_sql})],
            capture_output=True,
            text=True,
            check=True
        )
        
        print("✓ Data verification complete")
        print(f"Result: {result.stdout}")
    except Exception as e:
        print(f"✗ Verification failed: {e}")
        raise


def main():
    print("=" * 60)
    print("Salesforce to Supabase ETL Pipeline")
    print("=" * 60)
    
    supabase_project_id = 'yylrfxpjbxefavcmjvho'
    
    try:
        sf = authenticate_salesforce()
        
        opportunities, accounts = extract_salesforce_data(sf)
        
        opp_path, acct_path = prepare_data_for_transformation(opportunities, accounts)
        
        try:
            transformed, anomalies = run_transformations(opp_path, acct_path)
            
            create_supabase_tables(supabase_project_id)
            
            load_to_supabase(supabase_project_id, transformed, anomalies)
            
            verify_data_in_supabase(supabase_project_id)
            
        finally:
            os.unlink(opp_path)
            os.unlink(acct_path)
        
        print("\n" + "=" * 60)
        print("✓ ETL Pipeline completed successfully!")
        print("=" * 60)
        print(f"\nTransformed records: {len(transformed)}")
        print(f"Anomaly records: {len(anomalies)}")
        print(f"\nData loaded to Supabase project: {supabase_project_id}")
        print("Tables: opportunities_transformed, opportunities_anomalies")
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ ETL Pipeline failed: {e}")
        print("=" * 60)
        sys.exit(1)


if __name__ == '__main__':
    main()
