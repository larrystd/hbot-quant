PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at_us INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS run_manifest (
  run_id TEXT PRIMARY KEY,
  started_at_us INTEGER NOT NULL,
  clean_stopped_at_us INTEGER,
  history_complete INTEGER NOT NULL CHECK (history_complete IN (0, 1))
);
CREATE TABLE IF NOT EXISTS committed_sequence (
  run_id TEXT NOT NULL,
  shard INTEGER NOT NULL,
  last_seq INTEGER NOT NULL,
  PRIMARY KEY (run_id, shard),
  FOREIGN KEY (run_id) REFERENCES run_manifest(run_id)
);
CREATE TABLE IF NOT EXISTS history_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  shard INTEGER NOT NULL,
  shard_sequence INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  owner_key INTEGER,
  received_at_us INTEGER NOT NULL,
  exchange_at_us INTEGER,
  account TEXT,
  market_venue TEXT,
  market_kind INTEGER,
  market_symbol TEXT,
  payload_kind INTEGER NOT NULL,
  record_blob BLOB NOT NULL,
  UNIQUE (run_id, shard, shard_sequence),
  FOREIGN KEY (run_id) REFERENCES run_manifest(run_id)
);
CREATE INDEX IF NOT EXISTS history_records_time_id ON history_records(received_at_us, id);
CREATE INDEX IF NOT EXISTS history_records_account_id ON history_records(account, id);
CREATE INDEX IF NOT EXISTS history_records_owner_id ON history_records(owner_key, id);
CREATE INDEX IF NOT EXISTS history_records_market_id ON history_records(market_venue, market_kind, market_symbol, id);
-- In schema version 2, reason contains an ErrorCode rather than a GapReason ordinal.
CREATE TABLE IF NOT EXISTS history_gaps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  shard INTEGER NOT NULL,
  first_seq INTEGER NOT NULL,
  last_seq INTEGER NOT NULL,
  reason INTEGER NOT NULL,
  UNIQUE (run_id, shard, first_seq, last_seq, reason),
  FOREIGN KEY (run_id) REFERENCES run_manifest(run_id)
);
