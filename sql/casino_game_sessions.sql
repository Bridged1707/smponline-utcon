-- Apply this once in UTDB. Do not rely on runtime schema creation for gameplay.

CREATE TABLE IF NOT EXISTS casino_game_sessions (
    id BIGSERIAL PRIMARY KEY,
    discord_uuid TEXT NOT NULL,
    game_type TEXT NOT NULL,
    wager_amount NUMERIC NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    outcome TEXT,
    membership_tier TEXT,
    fee_rate_bps INTEGER NOT NULL DEFAULT 0,
    gross_payout_amount NUMERIC NOT NULL DEFAULT 0,
    fee_amount NUMERIC NOT NULL DEFAULT 0,
    net_payout_amount NUMERIC NOT NULL DEFAULT 0,
    profit_amount NUMERIC NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_casino_game_sessions_discord_uuid
    ON casino_game_sessions(discord_uuid, created_at DESC);

GRANT SELECT, INSERT, UPDATE ON casino_game_sessions TO CURRENT_USER;
GRANT USAGE, SELECT ON SEQUENCE casino_game_sessions_id_seq TO CURRENT_USER;
