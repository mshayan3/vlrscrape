-- ============================================================
-- VLR Unified Match DB v2
-- ============================================================
-- Key changes vs v1:
--   * player_map_stats: added `side` column, fixed UNIQUE
--   * player_map_agents: bridge table (one agent per player per map)
--   * player_map_advanced: multi-kills, clutches, ECON/PL/DE
--   * player_vs_player_kills: added `kill_type` (all/fk/op)
--   * map_economy_summary: restructured per buy-type row
--   * round_economy: added bank_start and side column
--   * team_aliases: for name normalization
--   * matches: added vlr_match_id, match_date, patch
--   * maps: added vlr_game_id
-- ============================================================

PRAGMA foreign_keys = ON;

-- ============================================================
-- 0) Reference / dimension tables
-- ============================================================

CREATE TABLE IF NOT EXISTS events (
  event_id      TEXT PRIMARY KEY,
  event_name    TEXT NOT NULL,
  season_year   INTEGER,
  region        TEXT,
  source        TEXT DEFAULT 'vlr.gg'
);

CREATE TABLE IF NOT EXISTS teams (
  team_id       TEXT PRIMARY KEY,
  team_name     TEXT NOT NULL,
  region        TEXT
);

-- Maps short abbreviations / alternate spellings → canonical team_id
CREATE TABLE IF NOT EXISTS team_aliases (
  alias         TEXT PRIMARY KEY,    -- e.g. "FNC", "FNATIC", "Fnatic"
  team_id       TEXT NOT NULL,
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
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
  match_id        TEXT PRIMARY KEY,
  event_id        TEXT NOT NULL,
  vlr_match_id    TEXT,              -- e.g. "542272" from vlr.gg URL
  match_name      TEXT,
  stage           TEXT,
  match_date      TEXT,
  patch           TEXT,
  team_a_id       TEXT,
  team_b_id       TEXT,
  winner_team_id  TEXT,
  source_url      TEXT,
  FOREIGN KEY (event_id)        REFERENCES events(event_id),
  FOREIGN KEY (team_a_id)       REFERENCES teams(team_id),
  FOREIGN KEY (team_b_id)       REFERENCES teams(team_id),
  FOREIGN KEY (winner_team_id)  REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS maps (
  map_id          TEXT PRIMARY KEY,
  match_id        TEXT NOT NULL,
  vlr_game_id     TEXT,              -- e.g. "233478" from data-game-id
  map_number      INTEGER NOT NULL,  -- 1-based within the match
  map_name        TEXT NOT NULL,
  team_a_score    INTEGER,
  team_b_score    INTEGER,
  winner_team_id  TEXT,
  UNIQUE (match_id, map_number),
  FOREIGN KEY (match_id)        REFERENCES matches(match_id),
  FOREIGN KEY (winner_team_id)  REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS rounds (
  round_id        TEXT PRIMARY KEY,
  map_id          TEXT NOT NULL,
  round_number    INTEGER NOT NULL,
  score_after     TEXT,              -- score string after this round, e.g. "1-0"
  winning_team_id TEXT,
  winning_side    TEXT,              -- "attack" | "defend"
  win_method      TEXT,              -- "elimination" | "detonation" | "defuse" | "time"
  UNIQUE (map_id, round_number),
  FOREIGN KEY (map_id)          REFERENCES maps(map_id),
  FOREIGN KEY (winning_team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_rounds_map_round ON rounds(map_id, round_number);

-- ============================================================
-- 2) Map veto (draft)
-- ============================================================

CREATE TABLE IF NOT EXISTS map_veto (
  veto_id         TEXT PRIMARY KEY,
  match_id        TEXT NOT NULL,
  order_no        INTEGER NOT NULL,  -- sequence in the veto (1-based)
  team_id         TEXT,              -- NULL for decider
  action_type     TEXT NOT NULL,     -- "pick" | "ban" | "decider"
  map_name        TEXT NOT NULL,
  UNIQUE (match_id, order_no),
  FOREIGN KEY (match_id)  REFERENCES matches(match_id),
  FOREIGN KEY (team_id)   REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_veto_match ON map_veto(match_id);

-- ============================================================
-- 3) Economy
-- ============================================================

-- Summary: per buy-type per team per map
-- buy_type: "pistol" | "eco" | "semi" | "semi_buy" | "full_buy"
CREATE TABLE IF NOT EXISTS map_economy_summary (
  mes_id          TEXT PRIMARY KEY,
  map_id          TEXT NOT NULL,
  team_id         TEXT NOT NULL,
  buy_type        TEXT NOT NULL,
  rounds_played   INTEGER NOT NULL DEFAULT 0,
  rounds_won      INTEGER NOT NULL DEFAULT 0,
  UNIQUE (map_id, team_id, buy_type),
  FOREIGN KEY (map_id)    REFERENCES maps(map_id),
  FOREIGN KEY (team_id)   REFERENCES teams(team_id)
);

-- Round-level: one row per team per round
CREATE TABLE IF NOT EXISTS round_economy (
  econ_id         TEXT PRIMARY KEY,
  map_id          TEXT NOT NULL,
  round_number    INTEGER NOT NULL,
  team_id         TEXT NOT NULL,
  side            TEXT,              -- "attack" | "defend" (which side this team is on)
  bank_start      INTEGER,           -- credits left over going into this round (k*1000)
  loadout_value   INTEGER,           -- total credits spent (from title= attribute if re-scraped)
  buy_tier        TEXT,              -- "$" | "$$" | "$$$" | "pistol" (derived from tier symbol)
  UNIQUE (map_id, round_number, team_id),
  FOREIGN KEY (map_id)    REFERENCES maps(map_id),
  FOREIGN KEY (team_id)   REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_econ_map_round ON round_economy(map_id, round_number);

-- ============================================================
-- 4) Player performance per map
-- ============================================================

-- One row per player per map per side (all / attack / defend)
CREATE TABLE IF NOT EXISTS player_map_stats (
  pms_id          TEXT PRIMARY KEY,
  map_id          TEXT NOT NULL,
  player_id       TEXT NOT NULL,
  team_id         TEXT,
  side            TEXT NOT NULL DEFAULT 'all',  -- "all" | "attack" | "defend"
  -- Core stats
  rating          REAL,
  acs             REAL,
  kills           INTEGER,
  deaths          INTEGER,
  assists         INTEGER,
  kd_diff         INTEGER,           -- K-D differential (NOT ratio)
  kast            REAL,              -- stored as 0.0–1.0 fraction
  adr             REAL,
  hs_pct          REAL,              -- stored as 0.0–1.0 fraction
  fk              INTEGER,
  fd              INTEGER,
  fk_fd_diff      INTEGER,
  UNIQUE (map_id, player_id, side),
  FOREIGN KEY (map_id)      REFERENCES maps(map_id),
  FOREIGN KEY (player_id)   REFERENCES players(player_id),
  FOREIGN KEY (team_id)     REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_pms_map     ON player_map_stats(map_id);
CREATE INDEX IF NOT EXISTS idx_pms_player  ON player_map_stats(player_id);

-- Bridge: which agent(s) a player used on a specific map
-- In a standard match each player plays exactly one agent per map,
-- but this table is future-proof and handles All_Maps rows too.
CREATE TABLE IF NOT EXISTS player_map_agents (
  map_id          TEXT NOT NULL,
  player_id       TEXT NOT NULL,
  agent_id        TEXT NOT NULL,
  PRIMARY KEY (map_id, player_id, agent_id),
  FOREIGN KEY (map_id)      REFERENCES maps(map_id),
  FOREIGN KEY (player_id)   REFERENCES players(player_id),
  FOREIGN KEY (agent_id)    REFERENCES agents(agent_id)
);

-- Advanced stats from the Performance tab (per map)
CREATE TABLE IF NOT EXISTS player_map_advanced (
  pma_id          TEXT PRIMARY KEY,
  map_id          TEXT NOT NULL,
  player_id       TEXT NOT NULL,
  team_id         TEXT,
  multikill_2     INTEGER DEFAULT 0,
  multikill_3     INTEGER DEFAULT 0,
  multikill_4     INTEGER DEFAULT 0,
  multikill_5     INTEGER DEFAULT 0,
  clutch_1v1      INTEGER DEFAULT 0,
  clutch_1v2      INTEGER DEFAULT 0,
  clutch_1v3      INTEGER DEFAULT 0,
  clutch_1v4      INTEGER DEFAULT 0,
  clutch_1v5      INTEGER DEFAULT 0,
  econ_rating     INTEGER,           -- ECON column
  plant_success   INTEGER DEFAULT 0, -- PL column
  defuse_success  INTEGER DEFAULT 0, -- DE column
  UNIQUE (map_id, player_id),
  FOREIGN KEY (map_id)      REFERENCES maps(map_id),
  FOREIGN KEY (player_id)   REFERENCES players(player_id),
  FOREIGN KEY (team_id)     REFERENCES teams(team_id)
);

-- ============================================================
-- 5) Kill matrix
-- ============================================================

-- kill_type: "all" | "fk" (first kills) | "op" (operator kills)
CREATE TABLE IF NOT EXISTS player_vs_player_kills (
  pvpk_id           TEXT PRIMARY KEY,
  map_id            TEXT NOT NULL,
  killer_player_id  TEXT NOT NULL,
  victim_player_id  TEXT NOT NULL,
  killer_team_id    TEXT,
  victim_team_id    TEXT,
  kill_type         TEXT NOT NULL DEFAULT 'all',
  kills_count       INTEGER NOT NULL DEFAULT 0,
  UNIQUE (map_id, killer_player_id, victim_player_id, kill_type),
  FOREIGN KEY (map_id)              REFERENCES maps(map_id),
  FOREIGN KEY (killer_player_id)    REFERENCES players(player_id),
  FOREIGN KEY (victim_player_id)    REFERENCES players(player_id),
  FOREIGN KEY (killer_team_id)      REFERENCES teams(team_id),
  FOREIGN KEY (victim_team_id)      REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_pvpk_map     ON player_vs_player_kills(map_id);
CREATE INDEX IF NOT EXISTS idx_pvpk_killer  ON player_vs_player_kills(killer_player_id);

-- ============================================================
-- 6) Ingestion audit trail
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
  row_count     INTEGER,
  notes         TEXT
);

-- ============================================================
-- 7) Convenience views
-- ============================================================

CREATE VIEW IF NOT EXISTS v_player_map_overview AS
SELECT
  p.player_name,
  t.team_name,
  ma.map_name,
  pms.side,
  pms.rating,
  pms.acs,
  pms.kills,
  pms.deaths,
  pms.assists,
  pms.kd_diff,
  pms.kast,
  pms.adr,
  pms.hs_pct,
  pms.fk,
  pms.fd
FROM player_map_stats pms
JOIN players p  ON p.player_id  = pms.player_id
JOIN teams t    ON t.team_id    = pms.team_id
JOIN maps ma    ON ma.map_id    = pms.map_id
WHERE pms.side = 'all';

CREATE VIEW IF NOT EXISTS v_rounds_with_econ AS
SELECT
  r.map_id,
  r.round_number,
  r.score_after,
  r.winning_team_id,
  r.winning_side,
  r.win_method,
  re.team_id    AS econ_team_id,
  re.side       AS team_side,
  re.bank_start,
  re.buy_tier
FROM rounds r
LEFT JOIN round_economy re
  ON re.map_id = r.map_id AND re.round_number = r.round_number;

CREATE VIEW IF NOT EXISTS v_kill_matrix AS
SELECT
  mc.match_name,
  mc.stage,
  ma.map_number,
  ma.map_name,
  pvpk.map_id,
  pk.player_name  AS killer,
  kt.team_name    AS killer_team,
  pv.player_name  AS victim,
  vt.team_name    AS victim_team,
  pvpk.kill_type,
  pvpk.kills_count
FROM player_vs_player_kills pvpk
JOIN maps    ma  ON ma.map_id    = pvpk.map_id
JOIN matches mc  ON mc.match_id  = ma.match_id
JOIN players pk  ON pk.player_id = pvpk.killer_player_id
JOIN players pv  ON pv.player_id = pvpk.victim_player_id
LEFT JOIN teams  kt  ON kt.team_id  = pvpk.killer_team_id
LEFT JOIN teams  vt  ON vt.team_id  = pvpk.victim_team_id;
