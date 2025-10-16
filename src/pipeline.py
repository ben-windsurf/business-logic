
import argparse, json, time
from pathlib import Path
import pandas as pd
from .transformations import run_pipeline
from .quality_checks import anomaly_rules

def main():
    ap = argparse.ArgumentParser(description="Salesforce → Oracle transformations (Zero ETL demo)")
    ap.add_argument("--opportunities", required=True, help="CSV of Salesforce Opportunities")
    ap.add_argument("--accounts", required=True, help="CSV of Salesforce Accounts")
    ap.add_argument("--fx", required=True, help="CSV of FX rates with columns: currency,rate_to_usd,rate_date")
    ap.add_argument("--stage-map", required=True, help="CSV with columns: source_stage,std_stage")
    ap.add_argument("--outdir", default="out", help="Output directory")
    args = ap.parse_args()

    t0 = time.time()
    canon, full = run_pipeline(args.opportunities, args.accounts, args.fx, args.stage_map)
    anomalies = anomaly_rules(full)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    canon.to_csv(outdir / "opportunities_transformed.csv", index=False)
    anomalies.to_csv(outdir / "opportunities_anomalies.csv", index=False)

    summary = {
        "rows_in": int(full.shape[0]),
        "rows_out": int(canon.shape[0]),
        "anomaly_rows": int(anomalies["opportunity_id"].nunique()) if not anomalies.empty else 0,
        "anomaly_count": int(anomalies.shape[0]),
        "duration_seconds": round(time.time()-t0, 3)
    }
    (outdir / "run_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
