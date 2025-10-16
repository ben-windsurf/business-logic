
import pandas as pd

def anomaly_rules(df: pd.DataFrame) -> pd.DataFrame:
    issues = []

    def add_issue(row, code, detail):
        issues.append({
            "opportunity_id": row.get("Id"),
            "code": code,
            "detail": detail
        })

    for idx, row in df.iterrows():
        amt = row.get("Amount")
        prob = row.get("Probability")
        closed = row.get("CloseDate")
        stage_std = row.get("StageStd")
        fx = row.get("fx_rate_used")

        if pd.notna(amt) and amt < 0:
            add_issue(row, "NEG_AMOUNT", "Amount is negative")
        if pd.notna(prob) and (prob < 0 or prob > 100):
            add_issue(row, "PROB_OOB", "Probability outside 0-100")
        if pd.notna(closed) and closed > pd.Timestamp.now(tz=None).normalize() + pd.Timedelta(days=1):
            add_issue(row, "FUTURE_CLOSE", "CloseDate in the future")
        if pd.isna(stage_std):
            add_issue(row, "MISSING_STAGE_MAP", "Stage could not be mapped to standard taxonomy")
        if row.get("CurrencyIsoCode") and pd.isna(fx):
            add_issue(row, "MISSING_FX", "FX rate missing for currency/date")

    return pd.DataFrame(issues)
