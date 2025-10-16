#!/usr/bin/env python3
"""
Automated ETL pipeline: Salesforce → Transformation → Supabase
Extracts Opportunity data from Salesforce, applies business transformations,
detects anomalies, and loads results to Supabase.
"""

import os
import sys
import json
import tempfile
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Tuple

import pandas as pd
from simple_salesforce import Salesforce

from .transformations import run_pipeline
from .quality_checks import anomaly_rules

SUPABASE_PROJECT_ID = "yylrfxpjbxefavcmjvho"


def get_salesforce_credentials() -> Dict[str, str]:
    """Get Salesforce credentials from environment variables."""
    required_vars = ['SALESFORCE_USERNAME', 'SALESFORCE_PASSWORD', 'SALESFORCE_URL']
    credentials = {}
    
    for var in required_vars:
        value = os.environ.get(var)
        if not value:
            raise ValueError(f"Missing required environment variable: {var}")
        credentials[var.lower().replace('salesforce_', '')] = value
    
    credentials['security_token'] = os.environ.get('SALESFORCE_SECURITY_TOKEN', '')
    
    return credentials


def extract_salesforce_data(credentials: Dict[str, str]) -> Tuple[List[Dict], List[Dict]]:
    """Extract Opportunity and Account data from Salesforce."""
    print("Authenticating to Salesforce...")
    
    sf = Salesforce(
        username=credentials['username'],
        password=credentials['password'],
        security_token=credentials['security_token'],
        instance_url=credentials['url']
    )
    
    print("✓ Authenticated successfully")
    
    print("Querying Opportunities...")
    opp_query = """
        SELECT Id, AccountId, Name, StageName, Amount, CurrencyIsoCode, Probability,
               CloseDate, CreatedDate, LastModifiedDate, OwnerEmail, Phone, IsWon, IsClosed
        FROM Opportunity
    """
    opp_result = sf.query_all(opp_query)
    opp_records = opp_result['records']
    print(f"✓ Extracted {len(opp_records)} opportunities")
    
    print("Querying Accounts...")
    acct_query = "SELECT Id, Name, Industry, OwnerId FROM Account"
    acct_result = sf.query_all(acct_query)
    acct_records = acct_result['records']
    print(f"✓ Extracted {len(acct_records)} accounts")
    
    return opp_records, acct_records


def save_records_to_csv(records: List[Dict], filepath: Path) -> None:
    """Save Salesforce records to CSV file."""
    clean_records = [
        {k: v for k, v in record.items() if k != 'attributes'}
        for record in records
    ]
    
    df = pd.DataFrame(clean_records)
    df.to_csv(filepath, index=False)


def run_mcp_command(server: str, tool: str, input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Execute an MCP command and return the result."""
    cmd = [
        'mcp-cli', 'tool', 'call',
        tool,
        '--server', server,
        '--input', json.dumps(input_data)
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    
    if result.stdout:
        for line in result.stdout.split('\n'):
            if line.strip().startswith('[') or line.strip().startswith('{'):
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue
    
    return {}


def create_supabase_tables(project_id: str) -> None:
    """Create tables in Supabase if they don't exist."""
    print("Creating Supabase tables...")
    
    transformed_ddl = """
        CREATE TABLE IF NOT EXISTS opportunities_transformed (
            "Id" TEXT,
            "AccountId" TEXT,
            "AccountName" TEXT,
            "AccountIndustry" TEXT,
            "Name" TEXT,
            "StageName" TEXT,
            "StageStd" TEXT,
            "Amount" NUMERIC,
            "CurrencyIsoCode" TEXT,
            "AmountUSD" NUMERIC,
            "expected_revenue_usd" NUMERIC,
            "Probability" NUMERIC,
            "CloseDate" TIMESTAMP,
            "CreatedDate" TIMESTAMP,
            "LastModifiedDate" TIMESTAMP,
            "sales_cycle_days" INTEGER,
            "owner_email_hash" TEXT,
            "phone_normalized" TEXT,
            "is_won" INTEGER,
            "is_lost" INTEGER
        );
    """
    
    run_mcp_command('supabase', 'apply_migration', {
        'project_id': project_id,
        'name': f'create_opportunities_transformed_{int(pd.Timestamp.now().timestamp())}',
        'query': transformed_ddl
    })
    print("✓ Created opportunities_transformed table")
    
    anomalies_ddl = """
        CREATE TABLE IF NOT EXISTS opportunities_anomalies (
            "opportunity_id" TEXT,
            "code" TEXT,
            "detail" TEXT
        );
    """
    
    run_mcp_command('supabase', 'apply_migration', {
        'project_id': project_id,
        'name': f'create_opportunities_anomalies_{int(pd.Timestamp.now().timestamp())}',
        'query': anomalies_ddl
    })
    print("✓ Created opportunities_anomalies table")


def load_to_supabase(transformed_df: pd.DataFrame, anomalies_df: pd.DataFrame, project_id: str) -> None:
    """Load DataFrames to Supabase tables."""
    print("Loading data to Supabase...")
    
    print("  Truncating existing data...")
    run_mcp_command('supabase', 'execute_sql', {
        'project_id': project_id,
        'query': 'TRUNCATE TABLE opportunities_transformed; TRUNCATE TABLE opportunities_anomalies;'
    })
    
    print(f"  Inserting {len(transformed_df)} transformed records...")
    batch_size = 500
    for i in range(0, len(transformed_df), batch_size):
        batch = transformed_df.iloc[i:i+batch_size]
        
        values = []
        for _, row in batch.iterrows():
            row_values = []
            for val in row:
                if pd.isna(val):
                    row_values.append('NULL')
                elif isinstance(val, (int, float)):
                    row_values.append(str(val))
                elif isinstance(val, pd.Timestamp):
                    row_values.append(f"'{val.isoformat()}'")
                else:
                    escaped = str(val).replace("'", "''")
                    row_values.append(f"'{escaped}'")
            values.append(f"({','.join(row_values)})")
        
        columns = ','.join([f'"{col}"' for col in transformed_df.columns])
        insert_sql = f"INSERT INTO opportunities_transformed ({columns}) VALUES {','.join(values)};"
        
        run_mcp_command('supabase', 'execute_sql', {
            'project_id': project_id,
            'query': insert_sql
        })
        print(f"    Inserted batch {i//batch_size + 1}/{(len(transformed_df)-1)//batch_size + 1}")
    
    if not anomalies_df.empty:
        print(f"  Inserting {len(anomalies_df)} anomaly records...")
        values = []
        for _, row in anomalies_df.iterrows():
            opp_id = str(row['opportunity_id']).replace("'", "''")
            code = str(row['code']).replace("'", "''")
            detail = str(row['detail']).replace("'", "''")
            values.append(f"('{opp_id}', '{code}', '{detail}')")
        
        insert_sql = f"INSERT INTO opportunities_anomalies (opportunity_id, code, detail) VALUES {','.join(values)};"
        run_mcp_command('supabase', 'execute_sql', {
            'project_id': project_id,
            'query': insert_sql
        })
    else:
        print("  No anomalies to insert")
    
    print("✓ Data loaded successfully")


def main() -> int:
    """Main ETL pipeline function."""
    print("=" * 60)
    print("Salesforce → Supabase ETL Pipeline")
    print("=" * 60)
    
    try:
        print("\n[1/5] Extracting data from Salesforce...")
        credentials = get_salesforce_credentials()
        opp_records, acct_records = extract_salesforce_data(credentials)
        
        print("\n[2/5] Preparing data for transformation...")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            
            opp_csv = tmpdir_path / "opportunities.csv"
            acct_csv = tmpdir_path / "accounts.csv"
            save_records_to_csv(opp_records, opp_csv)
            save_records_to_csv(acct_records, acct_csv)
            print("✓ Saved to temporary CSV files")
            
            repo_root = Path(__file__).parent.parent
            fx_csv = repo_root / "tests/sample_data/fx_rates.csv"
            stage_csv = repo_root / "tests/sample_data/stage_mapping.csv"
            
            print("\n[3/5] Running transformation pipeline...")
            canonical_df, full_df = run_pipeline(
                str(opp_csv),
                str(acct_csv),
                str(fx_csv),
                str(stage_csv)
            )
            print(f"✓ Transformed {len(canonical_df)} records")
            
            print("\n[4/5] Running anomaly detection...")
            anomalies_df = anomaly_rules(full_df)
            anomaly_count = len(anomalies_df)
            unique_opps_with_anomalies = anomalies_df['opportunity_id'].nunique() if not anomalies_df.empty else 0
            print(f"✓ Found {anomaly_count} anomalies across {unique_opps_with_anomalies} opportunities")
            
            print("\n[5/5] Loading to Supabase...")
            create_supabase_tables(SUPABASE_PROJECT_ID)
            load_to_supabase(canonical_df, anomalies_df, SUPABASE_PROJECT_ID)
        
        print("\n" + "=" * 60)
        print("✓ ETL Pipeline Completed Successfully!")
        print("=" * 60)
        print(f"\nSummary:")
        print(f"  Opportunities extracted:    {len(opp_records)}")
        print(f"  Accounts extracted:         {len(acct_records)}")
        print(f"  Records transformed:        {len(canonical_df)}")
        print(f"  Anomalies detected:         {anomaly_count}")
        print(f"  Opportunities with issues:  {unique_opps_with_anomalies}")
        print(f"\nData loaded to Supabase project: {SUPABASE_PROJECT_ID}")
        print(f"  - opportunities_transformed table: {len(canonical_df)} rows")
        print(f"  - opportunities_anomalies table:   {anomaly_count} rows")
        
        return 0
        
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
