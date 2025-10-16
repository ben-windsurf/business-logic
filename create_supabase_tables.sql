
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

CREATE TABLE IF NOT EXISTS opportunities_anomalies (
    id SERIAL PRIMARY KEY,
    opportunity_id TEXT,
    code TEXT,
    detail TEXT,
    detected_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_opp_transformed_account_id ON opportunities_transformed(account_id);
CREATE INDEX IF NOT EXISTS idx_opp_transformed_stage_std ON opportunities_transformed(stage_std);
CREATE INDEX IF NOT EXISTS idx_opp_transformed_close_date ON opportunities_transformed(close_date);
CREATE INDEX IF NOT EXISTS idx_opp_anomalies_code ON opportunities_anomalies(code);
