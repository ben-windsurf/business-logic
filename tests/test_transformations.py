
import pandas as pd
from pathlib import Path
from src.devin_etl.transformations import run_pipeline

def test_basic_transform(tmp_path: Path):
    root = Path(__file__).parent
    canon, full = run_pipeline(
        root / "sample_data/opportunities.csv",
        root / "sample_data/accounts.csv",
        root / "sample_data/fx_rates.csv",
        root / "sample_data/stage_mapping.csv",
    )
    # Dedup: 0062 appears twice, keep the latest LastModifiedDate
    assert (full[full["Id"]=="0062"].shape[0]) == 1
    # Stage mapping
    row = canon[canon["Id"]=="0062"].iloc[0]
    assert row["StageStd"] == "Commit"
    # FX normalization EUR on 2025-10-01 uses 1.08
    amt_usd = canon[canon["Id"]=="0062"].iloc[0]["AmountUSD"]
    assert round(float(amt_usd),2) == round(85000*1.08,2)
    # Expected revenue
    exp_rev = canon[canon["Id"]=="0062"].iloc[0]["expected_revenue_usd"]
    assert round(float(exp_rev),2) == round(85000*1.08*0.72,2)
