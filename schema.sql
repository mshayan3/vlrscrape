-- ============================================================
-- VLR Unified Match DB (SQLite / DuckDB compatible)
-- Option 1: "Silver" canonical schema + optional "Bronze" raw tables
-- ============================================================

PRAGMA foreign_keys = ON;

-- ============================================================
-- 0) Reference / dimension tables
-- ============================================================

CREATE TABLE IF NOT EXISTS events (
  event_id      TEXT PRIMARY KEY,
  event_name    TEXT NOT NULL,
  season_year   INTEGER,
  source        TEXT DEFAULT 'vlr.gg'
);

CREATE TABLE IF NOT EXISTS teams (
  team_id       TEXT PRIMARY KEY,
  team_name     TEXT NOT NULL,
  region        TEXT
);

CREATE TABLE IF NOT EXISTS players (
  player_id     TEXT PRIMARY KEY,
  player_name   TEXT NOT NULL,
  current_team_id TEXT,
  FOREIGN KEY (current_team_id) REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS agents (
  agent_id      TEXT PRIMARY KEY,
  agent_name    TEXT NOT NULL
);

-- ============================================================
-- 1) Match + Map + Round backbone
-- ============================================================

CREATE TABLE IF NOT EXISTS matches (
  match_id      TEXT PRIMARY KEY,
  event_id      TEXT NOT NULL,
  match_name    TEXT,
  stage         TEXT,
  match_date    TEXT,
  team_a_id     TEXT,
  team_b_id     TEXT,
  winner_team_id TEXT,
  source_url    TEXT,
  FOREIGN KEY (event_id) REFERENCES events(event_id),
  FOREIGN KEY (team_a_id) REFERENCES teams(team_id),
  FOREIGN KEY (team_b_id) REFERENCES teams(team_id),
  FOREIGN KEY (winner_team_id) REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS maps (
  map_id        TEXT PRIMARY KEY,
  match_id      TEXT NOT NULL,
  map_number    INTEGER NOT NULL,
  map_name      TEXT NOT NULL,
  team_a_score  INTEGER,
  team_b_score  INTEGER,
  winner_team_id TEXT,
  UNIQUE (match_id, map_number),
  FOREIGN KEY (match_id) REFERENCES matches(match_id),
  FOREIGN KEY (winner_team_id) REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS rounds (
  round_id      TEXT PRIMARY KEY,
  map_id        TEXT NOT NULL,
  round_number  INTEGER NOT NULL,
  score         TEXT,
  winning_team_id TEXT,
  winning_side  TEXT,
  win_method    TEXT,
  UNIQUE (map_id, round_number),
  FOREIGN KEY (map_id) REFERENCES maps(map_id),
  FOREIGN KEY (winning_team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_rounds_map_round
ON rounds(map_id, round_number);

-- ============================================================
-- 2) Map veto (draft)
-- ============================================================

CREATE TABLE IF NOT EXISTS map_veto (
  veto_id       TEXT PRIMARY KEY,
  match_id      TEXT NOT NULL,
  order_no      INTEGER NOT NULL,
  team_id       TEXT,
  action_type   TEXT NOT NULL,
  map_name      TEXT NOT NULL,
  UNIQUE (match_id, order_no),
  FOREIGN KEY (match_id) REFERENCES matches(match_id),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_veto_match
ON map_veto(match_id);

-- ============================================================
-- 3) Economy (Round-level and optional Map-level summary)
-- ============================================================

CREATE TABLE IF NOT EXISTS round_economy (
  econ_id       TEXT PRIMARY KEY,
  map_id        TEXT NOT NULL,
  round_number  INTEGER NOT NULL,
  team_id       TEXT NOT NULL,
  credits       INTEGER,
  loadout_value INTEGER,
  buy_tier      TEXT,
  notes         TEXT,
  UNIQUE (map_id, round_number, team_id),
  FOREIGN KEY (map_id) REFERENCES maps(map_id),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_econ_map_round
ON round_economy(map_id, round_number);

CREATE TABLE IF NOT EXISTS map_economy_summary (
  map_econ_id   TEXT PRIMARY KEY,
  map_id        TEXT NOT NULL,
  team_id       TEXT NOT NULL,
  pistol_wins   INTEGER,
  eco_wins      INTEGER,
  anti_eco_wins INTEGER,
  full_buy_wins INTEGER,
  rounds_won    INTEGER,
  rounds_lost   INTEGER,
  UNIQUE (map_id, team_id),
  FOREIGN KEY (map_id) REFERENCES maps(map_id),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

-- ============================================================
-- 4) Player performance (advanced stats) per map
-- ============================================================

CREATE TABLE IF NOT EXISTS player_map_stats (
  pms_id        TEXT PRIMARY KEY,
  map_id        TEXT NOT NULL,
  player_id     TEXT NOT NULL,
  team_id       TEXT,
  agent_id      TEXT,
  acs           REAL,
  kills         INTEGER,
  deaths        INTEGER,
  assists       INTEGER,
  adr           REAL,
  kast          REAL,
  hs_pct        REAL,
  rating        REAL,
  fk            INTEGER,
  fd            INTEGER,
  extra_stats_json TEXT,
  UNIQUE (map_id, player_id),
  FOREIGN KEY (map_id) REFERENCES maps(map_id),
  FOREIGN KEY (player_id) REFERENCES players(player_id),
  FOREIGN KEY (team_id) REFERENCES teams(team_id),
  FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
);

CREATE INDEX IF NOT EXISTS idx_pms_map
ON player_map_stats(map_id);

-- ============================================================
-- 5) Kills (aggregate matrix)
-- ============================================================

CREATE TABLE IF NOT EXISTS player_vs_player_kills (
  pvpk_id       TEXT PRIMARY KEY,
  map_id        TEXT NOT NULL,
  killer_player_id TEXT NOT NULL,
  victim_player_id TEXT NOT NULL,
  killer_team_id   TEXT,
  victim_team_id   TEXT,
  kills_count   INTEGER NOT NULL DEFAULT 0,
  UNIQUE (map_id, killer_player_id, victim_player_id),
  FOREIGN KEY (map_id) REFERENCES maps(map_id),
  FOREIGN KEY (killer_player_id) REFERENCES players(player_id),
  FOREIGN KEY (victim_player_id) REFERENCES players(player_id),
  FOREIGN KEY (killer_team_id) REFERENCES teams(team_id),
  FOREIGN KEY (victim_team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_pvpk_map
ON player_vs_player_kills(map_id);

-- ============================================================
-- 6) Raw tables for traceability
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_files (
  file_id       TEXT PRIMARY KEY,
  event_id      TEXT,
  match_id      TEXT,
  map_number    INTEGER,
  map_name      TEXT,
  folder        TEXT,
  filename      TEXT NOT NULL,
  ingested_at   TEXT,
  notes         TEXT
);

CREATE TABLE IF NOT EXISTS raw_rows (
  raw_row_id    TEXT PRIMARY KEY,
  file_id       TEXT NOT NULL,
  row_idx       INTEGER NOT NULL,
  row_json      TEXT NOT NULL,
  FOREIGN KEY (file_id) REFERENCES raw_files(file_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_rows_file
ON raw_rows(file_id);

-- ============================================================
-- 7) Helpful views
-- ============================================================

CREATE VIEW IF NOT EXISTS v_rounds_with_econ AS
SELECT
  r.map_id,
  r.round_number,
  r.score,
  r.winning_team_id,
  r.winning_side,
  r.win_method,
  e.team_id AS econ_team_id,
  e.credits,
  e.buy_tier
FROM rounds r
LEFT JOIN round_economy e
  ON e.map_id = r.map_id AND e.round_number = r.round_number;
