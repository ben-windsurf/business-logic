
from __future__ import annotations
from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
import re
import hashlib

def parse_dt(x):
    if pd.isna(x) or x == "":
        return pd.NaT
    try:
        return pd.to_datetime(parser.parse(str(x)))
    except Exception:
        return pd.NaT

def normalize_phone(phone: str) -> str | None:
    if phone is None or str(phone).strip() == "":
        return None
    digits = re.sub(r"\D+", "", str(phone))
    if digits.startswith("1") and len(digits) == 11:
        digits = digits[1:]
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) >= 11 and digits.startswith("00"):
        return "+" + digits[2:]
    if len(digits) >= 11 and digits.startswith("011"):
        return "+" + digits[3:]
    if len(digits) >= 11 and digits.startswith("1"):
        return "+" + digits
    return "+" + digits if len(digits) >= 11 else None

def hash_email(email: str) -> str | None:
    if email is None or str(email).strip() == "":
        return None
    return hashlib.sha256(str(email).lower().encode("utf-8")).hexdigest()

def closest_prior_or_same_rate(fx_df: pd.DataFrame, currency: str, asof_date: pd.Timestamp):
    if pd.isna(asof_date) or currency is None or currency == "":
        return None
    cur = currency.upper()
    sub = fx_df[fx_df["currency"] == cur]
    if sub.empty:
        return None
    sub = sub[sub["rate_date"] <= asof_date].sort_values("rate_date", ascending=False)
    if not sub.empty:
        return float(sub.iloc[0]["rate_to_usd"])
    # fallback: latest available
    sub = fx_df[fx_df["currency"] == cur].sort_values("rate_date", ascending=False)
    return float(sub.iloc[0]["rate_to_usd"]) if not sub.empty else None

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

REQUIRED_OPP_COLS = [
    "Id","AccountId","Name","StageName","Amount","CurrencyIsoCode","Probability",
    "CloseDate","CreatedDate","LastModifiedDate","OwnerEmail","Phone","IsWon","IsClosed"
]

REQUIRED_ACCT_COLS = ["Id","Name","Industry","OwnerId"]

def enforce_required(df: pd.DataFrame, required: list[str]) -> list[str]:
    missing = [c for c in required if c not in df.columns]
    return missing
