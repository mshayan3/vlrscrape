-- ============================================================
-- VLR Unified Match DB v3
-- ============================================================
-- Derived from a fresh analysis of the NRG vs FNATIC GF scrape
-- cross-checked against the source VLR.gg HTML.
--
-- Changes vs v2 (see CHANGES block below each table):
--   * matches: + team_a_map_score / team_b_map_score (series score, e.g. 3-2)
--   * maps:    + duration_seconds, + pick_team_id (HTML "PICK" / decider)
--   * rounds:  + is_overtime; ingest MUST skip padded/blank round rows
--   * player_map_stats: + kd_ratio (GENERATED), clarified units
--   * data quality: explicit CHECK constraints on enumerations
--   * everything else carried over unchanged from v2
--
-- Units (unchanged from v2, restated for clarity):
--   kast, hs_pct          -> 0.0–1.0 fractions
--   kd_diff, fk_fd_diff   -> signed differentials (NOT ratios)
--   kd_ratio              -> derived kills/deaths (generated)
-- ============================================================

PRAGMA foreign_keys = ON;

-- ============================================================
-- 0) Reference / dimension tables  (unchanged from v2)
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

CREATE TABLE IF NOT EXISTS team_aliases (
  alias         TEXT PRIMARY KEY,
  team_id       TEXT NOT NULL,
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS players (
  player_id       TEXT PRIMARY KEY,
  player_name     TEXT NOT NULL,
  vlr_player_id   TEXT,              -- v3: numeric VLR id from meta.csv "id|name"
  current_team_id TEXT,             -- NOTE: latest known team, NOT team-in-match.
                                    --       For team-in-match use player_map_stats.team_id.
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
  match_id           TEXT PRIMARY KEY,
  event_id           TEXT NOT NULL,
  vlr_match_id       TEXT,
  match_name         TEXT,
  stage              TEXT,
  match_date         TEXT,
  patch              TEXT,
  team_a_id          TEXT,
  team_b_id          TEXT,
  team_a_map_score   INTEGER,        -- v3: maps won by team_a (series score)
  team_b_map_score   INTEGER,        -- v3: maps won by team_b
  winner_team_id     TEXT,
  best_of            INTEGER,        -- v3: 1 / 3 / 5 (from "Bo5")
  source_url         TEXT,
  FOREIGN KEY (event_id)        REFERENCES events(event_id),
  FOREIGN KEY (team_a_id)       REFERENCES teams(team_id),
  FOREIGN KEY (team_b_id)       REFERENCES teams(team_id),
  FOREIGN KEY (winner_team_id)  REFERENCES teams(team_id)
);
-- CHANGES vs v2: + team_a_map_score, team_b_map_score, best_of.

CREATE TABLE IF NOT EXISTS maps (
  map_id           TEXT PRIMARY KEY,
  match_id         TEXT NOT NULL,
  vlr_game_id      TEXT,             -- data-game-id (e.g. 233478) — needs scraper support
  map_number       INTEGER NOT NULL,
  map_name         TEXT NOT NULL,
  team_a_score     INTEGER,
  team_b_score     INTEGER,
  winner_team_id   TEXT,
  pick_team_id     TEXT,             -- v3: team that picked the map (NULL = decider)
  duration_seconds INTEGER,         -- v3: map length (HTML "40:34") — needs scraper support
  UNIQUE (match_id, map_number),
  FOREIGN KEY (match_id)        REFERENCES matches(match_id),
  FOREIGN KEY (winner_team_id)  REFERENCES teams(team_id),
  FOREIGN KEY (pick_team_id)    REFERENCES teams(team_id)
);
-- CHANGES vs v2: + pick_team_id, duration_seconds.

CREATE TABLE IF NOT EXISTS rounds (
  round_id        TEXT PRIMARY KEY,
  map_id          TEXT NOT NULL,
  round_number    INTEGER NOT NULL,
  score_after     TEXT,
  winning_team_id TEXT,
  winning_side    TEXT CHECK (winning_side IN ('attack','defend')),
  win_method      TEXT CHECK (win_method IN ('elimination','detonation','defuse','time')),
  is_overtime     INTEGER NOT NULL DEFAULT 0,   -- v3: round_number > 24
  UNIQUE (map_id, round_number),
  FOREIGN KEY (map_id)          REFERENCES maps(map_id),
  FOREIGN KEY (winning_team_id) REFERENCES teams(team_id)
);
-- CHANGES vs v2: + is_overtime; CHECK constraints now REJECT the padded blank
--   rows (empty side/method) that polluted v2 — ingest must skip rounds with no
--   winning_team_id / empty score so the real round count == map score sum.

CREATE INDEX IF NOT EXISTS idx_rounds_map_round ON rounds(map_id, round_number);

-- ============================================================
-- 2) Map veto (draft)  (unchanged from v2)
-- ============================================================

CREATE TABLE IF NOT EXISTS map_veto (
  veto_id         TEXT PRIMARY KEY,
  match_id        TEXT NOT NULL,
  order_no        INTEGER NOT NULL,
  team_id         TEXT,
  action_type     TEXT NOT NULL CHECK (action_type IN ('pick','ban','decider')),
  map_name        TEXT NOT NULL,
  UNIQUE (match_id, order_no),
  FOREIGN KEY (match_id)  REFERENCES matches(match_id),
  FOREIGN KEY (team_id)   REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_veto_match ON map_veto(match_id);

-- ============================================================
-- 3) Economy  (unchanged from v2)
-- ============================================================

CREATE TABLE IF NOT EXISTS map_economy_summary (
  mes_id          TEXT PRIMARY KEY,
  map_id          TEXT NOT NULL,
  team_id         TEXT NOT NULL,
  buy_type        TEXT NOT NULL,   -- pistol | eco | semi | semi_buy | full_buy
  rounds_played   INTEGER NOT NULL DEFAULT 0,
  rounds_won      INTEGER NOT NULL DEFAULT 0,
  UNIQUE (map_id, team_id, buy_type),
  FOREIGN KEY (map_id)    REFERENCES maps(map_id),
  FOREIGN KEY (team_id)   REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS round_economy (
  econ_id         TEXT PRIMARY KEY,
  map_id          TEXT NOT NULL,
  round_number    INTEGER NOT NULL,
  team_id         TEXT NOT NULL,
  side            TEXT CHECK (side IN ('attack','defend')),
  bank_start      INTEGER,
  loadout_value   INTEGER,
  buy_tier        TEXT,
  UNIQUE (map_id, round_number, team_id),
  FOREIGN KEY (map_id)    REFERENCES maps(map_id),
  FOREIGN KEY (team_id)   REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_econ_map_round ON round_economy(map_id, round_number);

-- ============================================================
-- 4) Player performance per map
-- ============================================================

CREATE TABLE IF NOT EXISTS player_map_stats (
  pms_id          TEXT PRIMARY KEY,
  map_id          TEXT NOT NULL,
  player_id       TEXT NOT NULL,
  team_id         TEXT,
  side            TEXT NOT NULL DEFAULT 'all' CHECK (side IN ('all','attack','defend')),
  rating          REAL,
  acs             REAL,
  kills           INTEGER,
  deaths          INTEGER,
  assists         INTEGER,
  kd_diff         INTEGER,           -- K-D differential (the VLR "+/-" column; NOT a ratio)
  kd_ratio        REAL GENERATED ALWAYS AS   -- v3: derived convenience ratio
                  (CASE WHEN deaths > 0 THEN CAST(kills AS REAL)/deaths
                        ELSE CAST(kills AS REAL) END) VIRTUAL,
  kast            REAL,              -- 0.0–1.0 fraction
  adr             REAL,
  hs_pct          REAL,              -- 0.0–1.0 fraction
  fk              INTEGER,
  fd              INTEGER,
  fk_fd_diff      INTEGER,
  UNIQUE (map_id, player_id, side),
  FOREIGN KEY (map_id)      REFERENCES maps(map_id),
  FOREIGN KEY (player_id)   REFERENCES players(player_id),
  FOREIGN KEY (team_id)     REFERENCES teams(team_id)
);
-- CHANGES vs v2: + kd_ratio (generated), + CHECK on side.

CREATE INDEX IF NOT EXISTS idx_pms_map     ON player_map_stats(map_id);
CREATE INDEX IF NOT EXISTS idx_pms_player  ON player_map_stats(player_id);

CREATE TABLE IF NOT EXISTS player_map_agents (
  map_id          TEXT NOT NULL,
  player_id       TEXT NOT NULL,
  agent_id        TEXT NOT NULL,
  PRIMARY KEY (map_id, player_id, agent_id),
  FOREIGN KEY (map_id)      REFERENCES maps(map_id),
  FOREIGN KEY (player_id)   REFERENCES players(player_id),
  FOREIGN KEY (agent_id)    REFERENCES agents(agent_id)
);

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
  econ_rating     INTEGER,
  plant_success   INTEGER DEFAULT 0,
  defuse_success  INTEGER DEFAULT 0,
  UNIQUE (map_id, player_id),
  FOREIGN KEY (map_id)      REFERENCES maps(map_id),
  FOREIGN KEY (player_id)   REFERENCES players(player_id),
  FOREIGN KEY (team_id)     REFERENCES teams(team_id)
);

-- ============================================================
-- 5) Kill matrix  (unchanged from v2)
-- ============================================================

CREATE TABLE IF NOT EXISTS player_vs_player_kills (
  pvpk_id           TEXT PRIMARY KEY,
  map_id            TEXT NOT NULL,
  killer_player_id  TEXT NOT NULL,
  victim_player_id  TEXT NOT NULL,
  killer_team_id    TEXT,
  victim_team_id    TEXT,
  kill_type         TEXT NOT NULL DEFAULT 'all' CHECK (kill_type IN ('all','fk','op')),
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
-- 6) Ingestion audit trail  (unchanged from v2)
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
-- 7) Convenience + pre-aggregated views (carried from v2)
-- ============================================================

CREATE VIEW IF NOT EXISTS v_player_map_overview AS
SELECT p.player_name, t.team_name, ma.map_name, pms.side,
       pms.rating, pms.acs, pms.kills, pms.deaths, pms.assists,
       pms.kd_diff, pms.kd_ratio, pms.kast, pms.adr, pms.hs_pct, pms.fk, pms.fd
FROM player_map_stats pms
JOIN players p  ON p.player_id  = pms.player_id
JOIN teams t    ON t.team_id    = pms.team_id
JOIN maps ma    ON ma.map_id    = pms.map_id
WHERE pms.side = 'all';

CREATE VIEW IF NOT EXISTS v_rounds_with_econ AS
SELECT r.map_id, r.round_number, r.score_after, r.winning_team_id,
       r.winning_side, r.win_method,
       re.team_id AS econ_team_id, re.side AS team_side, re.bank_start, re.buy_tier
FROM rounds r
LEFT JOIN round_economy re
  ON re.map_id = r.map_id AND re.round_number = r.round_number;

CREATE VIEW IF NOT EXISTS v_kill_matrix AS
SELECT mc.match_name, mc.stage, ma.map_number, ma.map_name, pvpk.map_id,
       pk.player_name AS killer, kt.team_name AS killer_team,
       pv.player_name AS victim, vt.team_name AS victim_team,
       pvpk.kill_type, pvpk.kills_count
FROM player_vs_player_kills pvpk
JOIN maps    ma  ON ma.map_id    = pvpk.map_id
JOIN matches mc  ON mc.match_id  = ma.match_id
JOIN players pk  ON pk.player_id = pvpk.killer_player_id
JOIN players pv  ON pv.player_id = pvpk.victim_player_id
LEFT JOIN teams  kt  ON kt.team_id  = pvpk.killer_team_id
LEFT JOIN teams  vt  ON vt.team_id  = pvpk.victim_team_id;

CREATE VIEW IF NOT EXISTS v_player_tournament_totals AS
SELECT e.event_id, e.event_name, p.player_id, p.player_name, t.team_id, t.team_name,
       COUNT(DISTINCT pms.map_id) AS maps_played,
       SUM(pms.kills) AS kills, SUM(pms.deaths) AS deaths, SUM(pms.assists) AS assists,
       ROUND(AVG(pms.rating),2) AS avg_rating, ROUND(AVG(pms.acs),1) AS avg_acs,
       ROUND(AVG(pms.kast)*100,1) AS avg_kast_pct, ROUND(AVG(pms.adr),1) AS avg_adr,
       ROUND(AVG(pms.hs_pct)*100,1) AS avg_hs_pct,
       SUM(pms.fk) AS first_kills, SUM(pms.fd) AS first_deaths
FROM player_map_stats pms
JOIN maps    mp ON mp.map_id   = pms.map_id
JOIN matches mc ON mc.match_id = mp.match_id
JOIN events  e  ON e.event_id  = mc.event_id
JOIN players p  ON p.player_id = pms.player_id
JOIN teams   t  ON t.team_id   = pms.team_id
WHERE pms.side = 'all'
GROUP BY e.event_id, p.player_id;

CREATE VIEW IF NOT EXISTS v_team_map_record AS
SELECT t.team_id, t.team_name, mp.map_name,
       COUNT(*) AS maps_played,
       SUM(CASE WHEN mp.winner_team_id = t.team_id THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN mp.winner_team_id <> t.team_id THEN 1 ELSE 0 END) AS losses,
       ROUND(100.0*SUM(CASE WHEN mp.winner_team_id = t.team_id THEN 1 ELSE 0 END)/COUNT(*),1) AS win_pct
FROM maps mp
JOIN matches mc ON mc.match_id = mp.match_id
JOIN teams   t  ON t.team_id IN (mc.team_a_id, mc.team_b_id)
GROUP BY t.team_id, mp.map_name;

CREATE VIEW IF NOT EXISTS v_agent_pick_rate AS
SELECT e.event_id, e.event_name, mc.stage, a.agent_id, a.agent_name,
       COUNT(*) AS picks
FROM player_map_agents pag
JOIN agents  a  ON a.agent_id  = pag.agent_id
JOIN maps    mp ON mp.map_id   = pag.map_id
JOIN matches mc ON mc.match_id = mp.match_id
JOIN events  e  ON e.event_id  = mc.event_id
GROUP BY e.event_id, mc.stage, a.agent_id;
