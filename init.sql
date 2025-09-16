CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.cryptos (
    id TEXT,
    symbol TEXT,
    name TEXT,
    current_price NUMERIC,
    market_cap NUMERIC,
    total_volume NUMERIC,
    high_24h NUMERIC,
    low_24h NUMERIC,
    price_change_percentage_24h NUMERIC,
    price_change_ratio NUMERIC,
    is_profitable BOOLEAN,
    last_updated TIMESTAMP
);
