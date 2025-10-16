
from __future__ import annotations
import pandas as pd
from .utils import (
    parse_dt, normalize_phone, hash_email, closest_prior_or_same_rate, safe_float,
    REQUIRED_OPP_COLS, REQUIRED_ACCT_COLS, enforce_required
)

def load_csv(path: str, parse_dates=None) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if parse_dates:
        for c in parse_dates:
            if c in df.columns:
                df[c] = df[c].apply(parse_dt)
    return df

def dedupe_latest(df: pd.DataFrame, id_col="Id", ts_col="LastModifiedDate") -> pd.DataFrame:
    df = df.copy()
    df[ts_col] = df[ts_col].apply(parse_dt)
    df = df.sort_values(ts_col).drop_duplicates(subset=[id_col], keep="last")
    return df

def normalize_stages(opp: pd.DataFrame, stage_map: pd.DataFrame) -> pd.DataFrame:
    stage_map = stage_map.rename(columns={"source_stage":"StageName","std_stage":"StageStd"})
    return opp.merge(stage_map[["StageName","StageStd"]], on="StageName", how="left")

def enrich_accounts(opp: pd.DataFrame, accts: pd.DataFrame) -> pd.DataFrame:
    accts = accts.rename(columns={"Id":"AccountId","Name":"AccountName","Industry":"AccountIndustry"})
    cols = ["AccountId","AccountName","AccountIndustry","OwnerId"]
    return opp.merge(accts[cols], on="AccountId", how="left")

def apply_fx(opp: pd.DataFrame, fx: pd.DataFrame) -> pd.DataFrame:
    fx = fx.copy()
    fx["currency"] = fx["currency"].str.upper()
    fx["rate_date"] = pd.to_datetime(fx["rate_date"])
    def _fx(row):
        return closest_prior_or_same_rate(fx, row.get("CurrencyIsoCode"), row.get("CloseDate"))
    opp["fx_rate_used"] = opp.apply(_fx, axis=1)
    def to_usd(amount, rate):
        if pd.isna(amount):
            return None
        if rate is None or pd.isna(rate):
            return None
        return float(amount) * float(rate)
    opp["AmountUSD"] = opp.apply(lambda r: to_usd(r.get("Amount"), r.get("fx_rate_used")) if r.get("CurrencyIsoCode","").upper()!="USD" else safe_float(r.get("Amount")), axis=1)
    return opp

def compute_metrics(opp: pd.DataFrame) -> pd.DataFrame:
    opp = opp.copy()
    # Types
    for col in ["Amount","Probability"]:
        opp[col] = opp[col].apply(safe_float)
    for c in ["CloseDate","CreatedDate","LastModifiedDate"]:
        if c in opp.columns:
            opp[c] = opp[c].apply(parse_dt)

    opp["expected_revenue_usd"] = opp.apply(lambda r: (r.get("AmountUSD") or 0.0) * ((r.get("Probability") or 0.0)/100.0), axis=1)
    opp["sales_cycle_days"] = (opp["CloseDate"] - opp["CreatedDate"]).dt.days
    # flags
    opp["is_won"] = opp["IsWon"].astype(str).str.lower().isin(["true","1","t","y"]).astype(int)
    opp["is_lost"] = opp.apply(lambda r: 1 if (str(r.get("IsClosed")).lower() in ["true","1","t","y"]) and (r.get("is_won")==0) else 0, axis=1)
    return opp

def sanitize_pii(opp: pd.DataFrame) -> pd.DataFrame:
    opp = opp.copy()
    opp["owner_email_hash"] = opp["OwnerEmail"].apply(hash_email)
    opp["phone_normalized"] = opp["Phone"].apply(normalize_phone)
    return opp

def canonical_select(opp: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "Id","AccountId","AccountName","AccountIndustry","Name",
        "StageName","StageStd",
        "Amount","CurrencyIsoCode","AmountUSD","expected_revenue_usd","Probability",
        "CloseDate","CreatedDate","LastModifiedDate","sales_cycle_days",
        "owner_email_hash","phone_normalized","is_won","is_lost"
    ]
    existing = [c for c in cols if c in opp.columns]
    out = opp[existing].copy()
    out = out.sort_values(["CloseDate","Id"], na_position="last").reset_index(drop=True)
    return out

def run_pipeline(opportunities_csv, accounts_csv, fx_csv, stage_map_csv):
    # Load
    opp = load_csv(opportunities_csv, parse_dates=["CloseDate","CreatedDate","LastModifiedDate"])
    accts = load_csv(accounts_csv)
    fx = load_csv(fx_csv, parse_dates=["rate_date"])
    stage_map = load_csv(stage_map_csv)

    # Validate columns
    missing_opp = enforce_required(opp, REQUIRED_OPP_COLS)
    missing_acct = enforce_required(accts, REQUIRED_ACCT_COLS)
    if missing_opp or missing_acct:
        raise ValueError(f"Missing columns – Opp: {missing_opp}, Accounts: {missing_acct}")

    # Dedupe
    opp = dedupe_latest(opp)

    # Stage normalization
    opp = normalize_stages(opp, stage_map)

    # Enrich with account fields
    opp = enrich_accounts(opp, accts)

    # FX and metrics
    opp = apply_fx(opp, fx)
    opp = compute_metrics(opp)

    # PII & canonical
    opp = sanitize_pii(opp)
    canon = canonical_select(opp)

    return canon, opp  # canon for facts, opp retains helper cols for anomaly checks
