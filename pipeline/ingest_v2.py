"""
ingest_v2.py — VLR CSV → SQLite ingestion pipeline (schema_v2)

Walk the "VCT Events/" directory tree.
For every match folder, parse each CSV subfolder and populate the DB.

Usage (run from project root or pipeline/ dir):
    python pipeline/ingest_v2.py                          # ingest all events
    python pipeline/ingest_v2.py --event "Valorant_Champions_2025"
    python pipeline/ingest_v2.py --match "NRG_vs_FNATIC_Playoffs-_Grand_Final"
    python pipeline/ingest_v2.py --db db/my_custom.db
"""

import os
import re
import csv
import uuid
import sqlite3
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

_ROOT       = Path(__file__).parent.parent          # project root (one level up from pipeline/)
EVENTS_ROOT = _ROOT / "data" / "VCT Events"
DEFAULT_DB  = _ROOT / "db" / "vlr_v2.db"
SCHEMA_FILE = Path(__file__).parent / "schema_v2.sql"

# ============================================================
# Utility helpers
# ============================================================

def slugify(text: str) -> str:
    """Convert arbitrary text to a stable lowercase-hyphen slug."""
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-")


def make_id(*parts) -> str:
    """Deterministic ID from concatenated slug parts."""
    return slugify("-".join(str(p) for p in parts))


def parse_k(val: str) -> int | None:
    """Convert '8.5k' → 8500, '20.0k' → 20000, etc. Returns None if unparseable."""
    val = str(val).strip()
    m = re.match(r"^([\d.]+)k$", val, re.I)
    if m:
        return round(float(m.group(1)) * 1000)
    try:
        return int(val)
    except ValueError:
        return None


def clean_pct(val: str) -> float | None:
    """'94%' → 0.94,  '0.94' → 0.94,  '' → None"""
    val = str(val).strip().rstrip("%")
    try:
        f = float(val)
        return f / 100.0 if f > 1.5 else f   # already a fraction if ≤1.5
    except ValueError:
        return None


def clean_int(val) -> int | None:
    if val is None:
        return None
    s = re.sub(r"[^0-9+\-]", "", str(val).strip())
    try:
        return int(s)
    except ValueError:
        return None


def leading_int(val) -> int:
    """Extract the leading integer from a possibly polluted cell like '5 Round 3 ...'."""
    m = re.match(r"^\s*(\d+)", str(val))
    return int(m.group(1)) if m else 0


def parse_win_method(raw: str) -> str:
    raw = raw.lower()
    if "elim" in raw:      return "elimination"
    if "boom" in raw or "detonation" in raw or "spike" in raw: return "detonation"
    if "defuse" in raw:    return "defuse"
    if "time" in raw:      return "time"
    return raw


def parse_side(raw: str) -> str:
    raw = raw.lower()
    if "attack" in raw or "mod-t" in raw:   return "attack"
    if "defend" in raw or "mod-ct" in raw:  return "defend"
    return raw


def buy_tier_symbol(sym: str) -> str:
    """'$$$' → 'full_buy', '$$' → 'semi', '$' → 'eco', '' → 'pistol'"""
    c = sym.count("$") if sym else 0
    return {0: "pistol", 1: "eco", 2: "semi", 3: "full_buy"}.get(c, sym)


def parse_player_team_col(col: str):
    """'brawk NRG' → ('brawk', 'NRG').  Splits on last whitespace."""
    col = col.strip()
    parts = col.rsplit(" ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return col, ""


# ============================================================
# DB setup
# ============================================================

def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    with open(SCHEMA_FILE) as f:
        conn.executescript(f.read())
    conn.commit()
    return conn


# ============================================================
# Normalisation helpers — get-or-create dimension records
# ============================================================

class Normaliser:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._team_cache:   dict[str, str] = {}   # alias → team_id
        self._player_cache: dict[str, str] = {}   # "name|team_id" → player_id
        self._agent_cache:  dict[str, str] = {}   # name_lower → agent_id

    # ---- Teams ----

    def team_id(self, name: str) -> str:
        """Resolve a team name / alias to a canonical team_id, creating if needed."""
        name = name.strip()
        key  = name.lower()
        if key in self._team_cache:
            return self._team_cache[key]

        # Check alias table
        row = self.conn.execute(
            "SELECT team_id FROM team_aliases WHERE LOWER(alias)=?", (key,)
        ).fetchone()
        if row:
            self._team_cache[key] = row[0]
            return row[0]

        # Check teams table directly
        row = self.conn.execute(
            "SELECT team_id FROM teams WHERE LOWER(team_name)=?", (key,)
        ).fetchone()
        if row:
            tid = row[0]
            self._insert_alias(name, tid)
            self._team_cache[key] = tid
            return tid

        # Create new
        tid = make_id("team", name)
        self.conn.execute(
            "INSERT OR IGNORE INTO teams(team_id, team_name) VALUES(?,?)",
            (tid, name)
        )
        self._insert_alias(name, tid)
        self._team_cache[key] = tid
        return tid

    def _insert_alias(self, alias: str, team_id: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO team_aliases(alias, team_id) VALUES(?,?)",
            (alias.strip(), team_id)
        )

    # ---- Players ----

    def player_id(self, name: str, team_id: str) -> str:
        name = name.strip()
        key  = f"{name.lower()}|{team_id}"
        if key in self._player_cache:
            return self._player_cache[key]

        row = self.conn.execute(
            "SELECT player_id FROM players WHERE LOWER(player_name)=? AND current_team_id=?",
            (name.lower(), team_id)
        ).fetchone()
        if row:
            self._player_cache[key] = row[0]
            return row[0]

        # Also check without team constraint (player may have moved)
        row = self.conn.execute(
            "SELECT player_id FROM players WHERE LOWER(player_name)=?",
            (name.lower(),)
        ).fetchone()
        if row:
            self._player_cache[key] = row[0]
            return row[0]

        pid = make_id("player", name, team_id)
        self.conn.execute(
            "INSERT OR IGNORE INTO players(player_id, player_name, current_team_id) VALUES(?,?,?)",
            (pid, name, team_id)
        )
        self._player_cache[key] = pid
        return pid

    # ---- Agents ----

    def agent_id(self, name: str) -> str:
        name = name.strip()
        key  = name.lower()
        if key in self._agent_cache:
            return self._agent_cache[key]

        row = self.conn.execute(
            "SELECT agent_id FROM agents WHERE LOWER(agent_name)=?", (key,)
        ).fetchone()
        if row:
            self._agent_cache[key] = row[0]
            return row[0]

        aid = make_id("agent", name)
        self.conn.execute(
            "INSERT OR IGNORE INTO agents(agent_id, agent_name) VALUES(?,?)",
            (aid, name)
        )
        self._agent_cache[key] = aid
        return aid


# ============================================================
# Folder/path parsing helpers
# ============================================================

def parse_event_folder(event_dir: Path):
    """
    event_dir: .../'VCT Events'/2025/'Valorant_Champions_2025'
    Returns dict with event_id, event_name, season_year, region (guessed).
    """
    year  = int(event_dir.parent.name) if event_dir.parent.name.isdigit() else None
    name  = event_dir.name.replace("_", " ").strip()
    eid   = make_id("event", name)
    region = None
    lname  = name.lower()
    for r in ("americas", "emea", "pacific", "china"):
        if r in lname:
            region = r.title()
            break
    return dict(event_id=eid, event_name=name, season_year=year, region=region)


def parse_match_folder(match_dir: Path, event_id: str, stage: str):
    """
    match_dir: NRG_vs_FNATIC_Playoffs-_Grand_Final
    Returns dict with match_id, team_a_name, team_b_name, match_name, stage.
    """
    folder = match_dir.name
    # Split on "_vs_" to extract team names
    m = re.match(r"^(.+?)_vs_(.+?)(?:_(?:Playoffs|Group|Swiss|Main|Upper|Lower).+)?$", folder)
    if m:
        team_a_raw = m.group(1)
        rest       = m.group(2)
        # team_b_raw is everything up to first known stage keyword
        stage_kw = re.search(
            r"_(Playoffs|Group_Stage|Swiss_Stage|Main_Event|Upper|Lower|Showmatch)-?_",
            rest
        )
        team_b_raw = rest[: stage_kw.start()] if stage_kw else rest
    else:
        # Fallback: first part before _vs_
        parts      = folder.split("_vs_")
        team_a_raw = parts[0]
        team_b_raw = parts[1].split("_")[0] if len(parts) > 1 else "Unknown"

    # Use the full folder name to guarantee uniqueness even when same teams meet twice
    match_id   = make_id("match", event_id, folder)
    return dict(
        match_id    = match_id,
        match_name  = folder.replace("_", " "),
        team_a_name = team_a_raw,
        team_b_name = team_b_raw,
        stage       = stage,
    )


def clean_map_name(raw: str) -> str:
    """
    'Map_1_1Corrode' → 'Corrode'
    '1Corrode'       → 'Corrode'
    'Corrode'        → 'Corrode'
    """
    s = raw.strip()
    # Strip 'Map_N_' prefix
    s = re.sub(r"^Map_\d+_", "", s)
    # Strip leading digit(s) (e.g. '1Corrode' → 'Corrode')
    s = re.sub(r"^\d+", "", s)
    return s.strip()


def map_number_from_filename(filename: str) -> int | None:
    """'Map_1_1Corrode.csv' → 1,  '1Corrode_rounds.csv' → None"""
    m = re.match(r"Map_(\d+)_", filename)
    return int(m.group(1)) if m else None


def map_number_from_economy_filename(filename: str) -> int | None:
    """'1Corrode_economy.csv' → 1, '2Lotus_rounds_economy.csv' → 2"""
    m = re.match(r"(\d+)", filename)
    return int(m.group(1)) if m else None


# ============================================================
# CSV parsers (one per data type)
# ============================================================

# --- player_stats ---

def parse_player_stats(csv_path: Path):
    """
    Yields dicts with keys:
      player_name, team_name, map_name, side, agents[],
      rating, acs, kills, deaths, assists, kd_diff, kast, adr, hs_pct,
      fk, fd, fk_fd_diff
    """
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                agents_raw = row.get("Agents", "").strip()
                agents = [a.strip() for a in agents_raw.split(",") if a.strip()]
                kd_raw = row.get("K/D", "0").strip()
                fkfd   = row.get("FK/FD", "0").strip()
                yield dict(
                    player_name = row.get("Player", "").strip(),
                    team_name   = row.get("Team", "").strip(),
                    map_name    = clean_map_name(row.get("Map", "")),
                    side        = row.get("Side", "all").strip().lower(),
                    agents      = agents,
                    rating      = clean_int(row.get("R2.0", "")),  # keep as float later
                    acs         = clean_int(row.get("ACS", "")),
                    kills       = clean_int(row.get("K", "")),
                    deaths      = clean_int(row.get("D", "")),
                    assists     = clean_int(row.get("A", "")),
                    kd_diff     = clean_int(kd_raw),
                    kast        = clean_pct(row.get("KAST", "")),
                    adr         = clean_int(row.get("ADR", "")),
                    hs_pct      = clean_pct(row.get("HS%", "")),
                    fk          = clean_int(row.get("FK", "")),
                    fd          = clean_int(row.get("FD", "")),
                    fk_fd_diff  = clean_int(fkfd),
                )
    except Exception as e:
        log.warning(f"  parse_player_stats failed for {csv_path.name}: {e}")


def parse_rating(val) -> float | None:
    try:
        return float(str(val).strip())
    except Exception:
        return None


# --- rounds ---

def parse_rounds(csv_path: Path):
    """
    Yields dicts: map_name, round_number, score_after, winning_team, winning_side, win_method
    """
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                score = row.get("Score", "").strip().strip('"')
                rnum  = row.get("Round Number", "").strip()
                if not rnum or rnum == "Round Number":
                    continue
                yield dict(
                    map_name     = clean_map_name(row.get("Map", "")),
                    round_number = clean_int(rnum),
                    score_after  = score,
                    winning_team = row.get("Winning Team", "").strip(),
                    winning_side = parse_side(row.get("Winning Side", "")),
                    win_method   = parse_win_method(row.get("Win Method", "")),
                )
    except Exception as e:
        log.warning(f"  parse_rounds failed for {csv_path.name}: {e}")


# --- map_veto ---

def parse_map_veto(csv_path: Path):
    """
    Returns a list of veto entries sorted by inferred order.
    Each entry: {order_no, team_name, action_type, map_name}
    Note: the CSV loses the original order; we reconstruct a plausible one.
    Bans first (in file order), then picks, then decider.
    """
    bans, picks, deciders = [], [], []
    seen = set()   # deduplicate map names
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                map_name = row.get("map", "").strip().lower()
                pick     = row.get("pick", "").strip()
                ban      = row.get("ban", "").strip()

                if not map_name or map_name in seen:
                    continue
                seen.add(map_name)

                canonical = map_name.capitalize()

                if pick.lower() == "decider" or (not pick and not ban):
                    deciders.append(dict(action_type="decider", team_name=None, map_name=canonical))
                elif ban:
                    bans.append(dict(action_type="ban", team_name=ban, map_name=canonical))
                elif pick:
                    picks.append(dict(action_type="pick", team_name=pick, map_name=canonical))
    except Exception as e:
        log.warning(f"  parse_map_veto failed: {e}")
        return []

    result = []
    for i, entry in enumerate(bans + picks + deciders, start=1):
        result.append({**entry, "order_no": i})
    return result


# --- economy summary ---

def parse_economy_summary(csv_path: Path):
    """
    Parses the _economy.csv (buy-type summary table).
    Yields: {team_name, buy_type, rounds_played, rounds_won}
    """
    # Cell values are tab-polluted: "3\t\t\t(1)" or "3 (1)"
    # Keys must be matched EXACTLY (not as substrings) to avoid
    # "$ (won)" matching inside "$$ (won)" and "$$$ (won)".
    BUY_TYPE_MAP = {
        "pistol won":     "pistol",
        "eco (won)":      "eco",       # pure eco / full save
        "$ (won)":        "semi_eco",  # force / partial buy  (~$1k)
        "$$ (won)":       "semi",      # semi-buy              (~$2k)
        "$$$ (won)":      "full_buy",  # full buy              (~$4k+)
        "semi (won)":     "semi",
        "semi buy (won)": "semi_buy",
    }

    def clean_cell(raw: str):
        """'3\t\t\t\t(1)' → (3, 1),   '1' (pistol) → (1, None)"""
        raw = re.sub(r"\s+", " ", raw).strip()
        m = re.match(r"^(\d+)\s*\((\d+)\)$", raw)
        if m:
            return int(m.group(1)), int(m.group(2))
        try:
            return int(raw), None
        except ValueError:
            return None, None

    try:
        rows = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            for line in f:
                # Replace tab sequences with commas for uniform splitting
                cells = [re.sub(r"\s+", " ", c).strip() for c in next(csv.reader([line]))]
                rows.append(cells)

        if not rows:
            return

        header = [h.lower() for h in rows[0]]   # first row = column headers
        # Find buy-type columns — use EXACT match to prevent
        # "$ (won)" from matching inside "$$ (won)" / "$$$ (won)"
        buy_cols = {}
        for idx, h in enumerate(header):
            for key, btype in BUY_TYPE_MAP.items():
                if h == key:          # exact, not substring
                    buy_cols[idx] = btype
                    break

        for row in rows[1:]:
            if not row or not row[0]:
                continue
            team_name = row[0]
            for col_idx, buy_type in buy_cols.items():
                if col_idx >= len(row):
                    continue
                played, won = clean_cell(row[col_idx])
                if played is not None:
                    yield dict(
                        team_name    = team_name,
                        buy_type     = buy_type,
                        rounds_played = played,
                        rounds_won    = won if won is not None else played,  # pistol = both same
                    )
    except Exception as e:
        log.warning(f"  parse_economy_summary failed for {csv_path.name}: {e}")


# --- round economy ---

def parse_round_economy(csv_path: Path):
    """
    Parses _rounds_economy.csv.
    Format (all on ~2 rows, one per half):
      (BANK) NRG FNC (BANK),1 0.3k 0.4k,2 8.5k $$ 6.1k,...

    Yields: {round_number, team_a, team_b, bank_a, tier_a, bank_b, tier_b}
    """
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            content = f.read()

        # Split into rows
        reader = csv.reader(content.splitlines())
        results = []
        team_a, team_b = None, None

        for row in reader:
            if not row:
                continue
            header_cell = row[0].strip()

            # Extract team names from header cell: "(BANK) NRG FNC (BANK)"
            m_teams = re.match(r"\(BANK\)\s+(\S+)\s+(\S+)\s+\(BANK\)", header_cell)
            if m_teams:
                team_a = m_teams.group(1)
                team_b = m_teams.group(2)
            elif not team_a:
                # Skip rows before first header
                continue

            for token in row[1:]:
                token = token.strip()
                if not token:
                    continue

                parts = token.split()
                if not parts:
                    continue

                try:
                    round_num = int(parts[0])
                except ValueError:
                    continue

                banks = [p for p in parts[1:] if re.match(r"^[\d.]+k$", p, re.I)]
                tiers = [p for p in parts[1:] if re.match(r"^\$+$", p)]

                bank_a = parse_k(banks[0]) if len(banks) > 0 else None
                bank_b = parse_k(banks[1]) if len(banks) > 1 else None
                tier_a = tiers[0] if len(tiers) > 0 else ""
                tier_b = tiers[1] if len(tiers) > 1 else (tiers[0] if len(tiers) == 1 and len(banks) >= 2 else "")

                results.append(dict(
                    round_number = round_num,
                    team_a       = team_a,
                    team_b       = team_b,
                    bank_a       = bank_a,
                    tier_a       = buy_tier_symbol(tier_a),
                    bank_b       = bank_b,
                    tier_b       = buy_tier_symbol(tier_b),
                ))

        return results
    except Exception as e:
        log.warning(f"  parse_round_economy failed for {csv_path.name}: {e}")
        return []


# --- advanced stats ---

def parse_advanced_stats(csv_path: Path):
    """
    Parses _advanced_stats.csv from the Performance tab.
    Columns: [player+team, (blank), 2K, 3K, 4K, 5K, 1v1, 1v2, 1v3, 1v4, 1v5, ECON, PL, DE]
    Yields: {player_name, team_name, mk2, mk3, mk4, mk5, c1v1..c1v5, econ, pl, de}
    """
    COLS = ["mk2", "mk3", "mk4", "mk5", "c1v1", "c1v2", "c1v3", "c1v4", "c1v5", "econ", "pl", "de"]
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))

        # Find header row (contains "2K")
        header_idx = 0
        for i, row in enumerate(rows):
            if any("2k" in str(c).lower() for c in row):
                header_idx = i
                break

        data_rows = rows[header_idx + 1:]
        for row in data_rows:
            if not row or not row[0].strip():
                continue

            player_col = row[0].strip()
            player_name, team_name = parse_player_team_col(player_col)
            if not player_name:
                continue

            # Columns start at index 2 (skip player col + blank col)
            data_start = 2
            values = row[data_start : data_start + len(COLS)]
            record = dict(player_name=player_name, team_name=team_name)
            for col_name, val in zip(COLS, values):
                record[col_name] = leading_int(val)

            yield record
    except Exception as e:
        log.warning(f"  parse_advanced_stats failed for {csv_path.name}: {e}")


# --- kill matrix ---

def parse_kill_matrix(csv_path: Path, kill_type: str = "all"):
    """
    Parses _All_Kills.csv / _First_Kills.csv / _Op_Kills.csv.
    Each cell: 'kills deaths diff'  e.g. '8 0 +8'
    Yields: {killer_name, killer_team, victim_name, victim_team, kills_count, kill_type}
    """
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))

        if not rows:
            return

        # First row is header; first col of each subsequent row is the killer
        header = rows[0]      # ['', 'Chronicle FNC', 'crashies FNC', ...]
        victims = [(parse_player_team_col(h)) for h in header[1:] if h.strip()]

        for row in rows[1:]:
            if not row or not row[0].strip():
                continue
            killer_col = row[0].strip()
            killer_name, killer_team = parse_player_team_col(killer_col)
            if not killer_name:
                continue

            for col_idx, (victim_name, victim_team) in enumerate(victims):
                cell_idx = col_idx + 1
                if cell_idx >= len(row):
                    continue
                cell = row[cell_idx].strip()
                if not cell or cell.lower() == "nan":
                    continue

                # Cell format: "kills deaths diff"  — take first number as kills
                parts = cell.split()
                try:
                    kills = int(parts[0])
                except (IndexError, ValueError):
                    continue

                if kills > 0:
                    yield dict(
                        killer_name  = killer_name,
                        killer_team  = killer_team,
                        victim_name  = victim_name,
                        victim_team  = victim_team,
                        kills_count  = kills,
                        kill_type    = kill_type,
                    )
    except Exception as e:
        log.warning(f"  parse_kill_matrix failed for {csv_path.name}: {e}")


# ============================================================
# DB insertion helpers
# ============================================================

def upsert_event(conn, norm, event_info: dict):
    conn.execute(
        "INSERT OR IGNORE INTO events(event_id, event_name, season_year, region) VALUES(?,?,?,?)",
        (event_info["event_id"], event_info["event_name"],
         event_info.get("season_year"), event_info.get("region"))
    )
    return event_info["event_id"]


def upsert_match(conn, norm, match_info: dict, event_id: str):
    team_a_id = norm.team_id(match_info["team_a_name"])
    team_b_id = norm.team_id(match_info["team_b_name"])
    conn.execute(
        """INSERT OR IGNORE INTO matches(match_id, event_id, match_name, stage,
             team_a_id, team_b_id) VALUES(?,?,?,?,?,?)""",
        (match_info["match_id"], event_id, match_info["match_name"],
         match_info["stage"], team_a_id, team_b_id)
    )
    return match_info["match_id"], team_a_id, team_b_id


def upsert_map(conn, match_id, map_number, map_name) -> str:
    map_id = make_id("map", match_id, map_number)
    conn.execute(
        """INSERT OR IGNORE INTO maps(map_id, match_id, map_number, map_name)
           VALUES(?,?,?,?)""",
        (map_id, match_id, map_number, map_name)
    )
    return map_id


def get_or_create_map_by_name(conn, match_id: str, map_name: str) -> str | None:
    """Look up an existing map row by match + name; return its map_id."""
    row = conn.execute(
        "SELECT map_id FROM maps WHERE match_id=? AND LOWER(map_name)=?",
        (match_id, map_name.lower())
    ).fetchone()
    return row[0] if row else None


def get_map_teams(conn, map_id: str):
    """Return (team_a_id, team_b_id) for the match that contains this map."""
    row = conn.execute(
        """SELECT m.team_a_id, m.team_b_id
           FROM maps mp JOIN matches m ON m.match_id = mp.match_id
           WHERE mp.map_id=?""",
        (map_id,)
    ).fetchone()
    return (row[0], row[1]) if row else (None, None)


def log_raw_file(conn, event_id, match_id, map_number, map_name, folder, filename, row_count=None, notes=None):
    fid = make_id("file", match_id, folder, filename)
    conn.execute(
        """INSERT OR REPLACE INTO raw_files
           (file_id, event_id, match_id, map_number, map_name, folder, filename, ingested_at, row_count, notes)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (fid, event_id, match_id, map_number, map_name, folder, filename,
         datetime.now(timezone.utc).isoformat(), row_count, notes)
    )


# ============================================================
# Per-section ingest functions
# ============================================================

def ingest_player_stats(conn, norm, match_id, stats_dir: Path, event_id):
    """Process all CSVs in player_stats/."""
    for csv_file in sorted(stats_dir.glob("*.csv")):
        if "All_Maps" in csv_file.name:
            # All_Maps: multi-agent rows; skip map-level inserts, use only for agent bridge on all-maps
            continue
        map_num  = map_number_from_filename(csv_file.name)
        if map_num is None:
            log.warning(f"  Cannot infer map number from {csv_file.name}, skipping")
            continue

        rows     = list(parse_player_stats(csv_file))
        if not rows:
            continue

        map_name = rows[0]["map_name"]
        map_id   = upsert_map(conn, match_id, map_num, map_name)
        log_raw_file(conn, event_id, match_id, map_num, map_name,
                     "player_stats", csv_file.name, len(rows))

        for r in rows:
            team_id   = norm.team_id(r["team_name"])
            player_id = norm.player_id(r["player_name"], team_id)

            # Ensure player is linked to a team in current context
            conn.execute(
                "UPDATE players SET current_team_id=? WHERE player_id=?",
                (team_id, player_id)
            )

            pms_id = make_id("pms", map_id, player_id, r["side"])
            rating_val = parse_rating(r.get("rating"))
            acs_val    = None
            try:
                acs_val = float(str(r.get("acs") or "").strip()) if r.get("acs") is not None else None
            except Exception:
                pass
            adr_val    = None
            try:
                adr_val = float(str(r.get("adr") or "").strip()) if r.get("adr") is not None else None
            except Exception:
                pass

            conn.execute(
                """INSERT OR REPLACE INTO player_map_stats
                   (pms_id, map_id, player_id, team_id, side, rating, acs, kills, deaths, assists,
                    kd_diff, kast, adr, hs_pct, fk, fd, fk_fd_diff)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (pms_id, map_id, player_id, team_id, r["side"],
                 rating_val, acs_val,
                 r["kills"], r["deaths"], r["assists"], r["kd_diff"],
                 r["kast"], adr_val, r["hs_pct"],
                 r["fk"], r["fd"], r["fk_fd_diff"])
            )

            # Agent bridge (only for "all" side rows — single map = single agent)
            if r["side"] == "all":
                for agent_name in r["agents"]:
                    if agent_name:
                        agent_id = norm.agent_id(agent_name)
                        conn.execute(
                            "INSERT OR IGNORE INTO player_map_agents(map_id, player_id, agent_id) VALUES(?,?,?)",
                            (map_id, player_id, agent_id)
                        )

    log.info(f"  ✓ player_stats ({stats_dir})")


def ingest_rounds(conn, norm, match_id, rounds_dir: Path, event_id):
    """Process all _rounds.csv files."""
    for csv_file in sorted(rounds_dir.glob("*_rounds.csv")):
        rows = list(parse_rounds(csv_file))
        if not rows:
            continue

        map_name = rows[0]["map_name"]
        map_id   = get_or_create_map_by_name(conn, match_id, map_name)
        if not map_id:
            log.warning(f"  Map not found for rounds file {csv_file.name}, skipping")
            continue

        log_raw_file(conn, event_id, match_id, None, map_name,
                     "rounds", csv_file.name, len(rows))

        team_a_id, team_b_id = get_map_teams(conn, map_id)

        scores_seen = {}   # track score to detect winner
        for r in rows:
            if r["round_number"] is None:
                continue

            winning_team_id = None
            wteam_raw = r["winning_team"]
            if wteam_raw:
                winning_team_id = norm.team_id(wteam_raw)

            round_id = make_id("round", map_id, r["round_number"])
            conn.execute(
                """INSERT OR REPLACE INTO rounds
                   (round_id, map_id, round_number, score_after, winning_team_id,
                    winning_side, win_method)
                   VALUES(?,?,?,?,?,?,?)""",
                (round_id, map_id, r["round_number"], r["score_after"],
                 winning_team_id, r["winning_side"], r["win_method"])
            )

    log.info(f"  ✓ rounds ({rounds_dir})")


def ingest_map_veto(conn, norm, match_id, veto_dir: Path, event_id):
    """Process map_veto.csv."""
    veto_file = veto_dir / "map_veto.csv"
    if not veto_file.exists():
        return

    entries = parse_map_veto(veto_file)
    log_raw_file(conn, event_id, match_id, None, None,
                 "map_veto", veto_file.name, len(entries))

    for entry in entries:
        veto_id = make_id("veto", match_id, entry["order_no"])
        team_id = norm.team_id(entry["team_name"]) if entry.get("team_name") else None
        conn.execute(
            """INSERT OR REPLACE INTO map_veto
               (veto_id, match_id, order_no, team_id, action_type, map_name)
               VALUES(?,?,?,?,?,?)""",
            (veto_id, match_id, entry["order_no"], team_id,
             entry["action_type"], entry["map_name"])
        )

    log.info(f"  ✓ map_veto ({veto_dir})")


def ingest_economy(conn, norm, match_id, econ_dir: Path, event_id):
    """Process economy CSVs."""
    for csv_file in sorted(econ_dir.glob("*.csv")):
        filename = csv_file.name
        map_num  = map_number_from_economy_filename(filename)
        if map_num is None:
            continue

        # Derive map name from filename: "1Corrode_economy.csv" → "Corrode"
        base = re.sub(r"^\d+", "", filename.split("_")[0])
        map_name = base

        map_id = get_or_create_map_by_name(conn, match_id, map_name)
        if not map_id:
            log.debug(f"  Map not found for economy file {filename}, trying partial match")
            # Try partial match
            row = conn.execute(
                "SELECT map_id, map_name FROM maps WHERE match_id=? AND map_number=?",
                (match_id, map_num)
            ).fetchone()
            if row:
                map_id = row[0]
            else:
                log.warning(f"  No map found for economy file {filename}, skipping")
                continue

        if "_rounds_economy" in filename:
            # Round-by-round economy
            rounds_econ = parse_round_economy(csv_file)
            log_raw_file(conn, event_id, match_id, map_num, map_name,
                         "economy", filename, len(rounds_econ))
            for r in rounds_econ:
                team_a_id = norm.team_id(r["team_a"])
                team_b_id = norm.team_id(r["team_b"])
                for team_id, bank, tier in [
                    (team_a_id, r["bank_a"], r["tier_a"]),
                    (team_b_id, r["bank_b"], r["tier_b"]),
                ]:
                    econ_id = make_id("econ", map_id, r["round_number"], team_id)
                    conn.execute(
                        """INSERT OR REPLACE INTO round_economy
                           (econ_id, map_id, round_number, team_id, bank_start, buy_tier)
                           VALUES(?,?,?,?,?,?)""",
                        (econ_id, map_id, r["round_number"], team_id, bank, tier)
                    )
        elif "_economy" in filename:
            # Summary table
            rows = list(parse_economy_summary(csv_file))
            log_raw_file(conn, event_id, match_id, map_num, map_name,
                         "economy", filename, len(rows))
            for r in rows:
                team_id = norm.team_id(r["team_name"])
                mes_id  = make_id("mes", map_id, team_id, r["buy_type"])
                conn.execute(
                    """INSERT OR REPLACE INTO map_economy_summary
                       (mes_id, map_id, team_id, buy_type, rounds_played, rounds_won)
                       VALUES(?,?,?,?,?,?)""",
                    (mes_id, map_id, team_id, r["buy_type"],
                     r["rounds_played"], r["rounds_won"])
                )

    log.info(f"  ✓ economy ({econ_dir})")


def ingest_performance(conn, norm, match_id, perf_dir: Path, event_id):
    """Process kill matrix and advanced stats CSVs."""
    kill_type_map = {
        "All_Kills":   "all",
        "First_Kills": "fk",
        "Op_Kills":    "op",
    }

    for csv_file in sorted(perf_dir.glob("*.csv")):
        filename = csv_file.name
        if "All_Maps" in filename:
            # All_Maps advanced stats
            if "advanced_stats" in filename:
                rows = list(parse_advanced_stats(csv_file))
                log_raw_file(conn, event_id, match_id, 0, "All_Maps",
                             "performance", filename, len(rows))
                # We skip All_Maps advanced stats DB insert since we have no single map_id
                # (could store in a separate all-match table — future enhancement)
            continue

        # Determine map from filename:  "1Corrode_All_Kills.csv"
        map_num = map_number_from_economy_filename(filename)
        base    = re.sub(r"^\d+", "", filename.split("_")[0])
        map_name = base

        map_id = get_or_create_map_by_name(conn, match_id, map_name)
        if not map_id:
            row = conn.execute(
                "SELECT map_id FROM maps WHERE match_id=? AND map_number=?",
                (match_id, map_num or -1)
            ).fetchone()
            if row:
                map_id = row[0]
            else:
                log.warning(f"  No map found for performance file {filename}, skipping")
                continue

        # Kill matrices
        for ktype_key, ktype_val in kill_type_map.items():
            if ktype_key in filename:
                rows = list(parse_kill_matrix(csv_file, ktype_val))
                log_raw_file(conn, event_id, match_id, map_num, map_name,
                             "performance", filename, len(rows))
                for r in rows:
                    killer_team_id = norm.team_id(r["killer_team"]) if r["killer_team"] else None
                    victim_team_id = norm.team_id(r["victim_team"]) if r["victim_team"] else None
                    killer_id = norm.player_id(r["killer_name"], killer_team_id or "")
                    victim_id = norm.player_id(r["victim_name"], victim_team_id or "")
                    pvpk_id   = make_id("pvpk", map_id, killer_id, victim_id, ktype_val)
                    conn.execute(
                        """INSERT OR REPLACE INTO player_vs_player_kills
                           (pvpk_id, map_id, killer_player_id, victim_player_id,
                            killer_team_id, victim_team_id, kill_type, kills_count)
                           VALUES(?,?,?,?,?,?,?,?)""",
                        (pvpk_id, map_id, killer_id, victim_id,
                         killer_team_id, victim_team_id, ktype_val, r["kills_count"])
                    )
                break

        # Advanced stats (per-map)
        if "advanced_stats" in filename:
            rows = list(parse_advanced_stats(csv_file))
            log_raw_file(conn, event_id, match_id, map_num, map_name,
                         "performance", filename, len(rows))
            for r in rows:
                team_id   = norm.team_id(r["team_name"]) if r.get("team_name") else None
                player_id = norm.player_id(r["player_name"], team_id or "")
                pma_id    = make_id("pma", map_id, player_id)
                conn.execute(
                    """INSERT OR REPLACE INTO player_map_advanced
                       (pma_id, map_id, player_id, team_id,
                        multikill_2, multikill_3, multikill_4, multikill_5,
                        clutch_1v1, clutch_1v2, clutch_1v3, clutch_1v4, clutch_1v5,
                        econ_rating, plant_success, defuse_success)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (pma_id, map_id, player_id, team_id,
                     r.get("mk2", 0), r.get("mk3", 0), r.get("mk4", 0), r.get("mk5", 0),
                     r.get("c1v1", 0), r.get("c1v2", 0), r.get("c1v3", 0),
                     r.get("c1v4", 0), r.get("c1v5", 0),
                     r.get("econ"), r.get("pl", 0), r.get("de", 0))
                )

    log.info(f"  ✓ performance ({perf_dir})")


# ============================================================
# Match ingestion orchestrator
# ============================================================

def reconcile_match_teams(conn, norm, match_id: str, folder_team_a: str, folder_team_b: str):
    """
    After player_stats are ingested, replace the placeholder folder-name team IDs
    (e.g. 'team-fnatic' from folder 'FNATIC') with the canonical abbreviation IDs
    (e.g. 'team-fnc' from player stats 'FNC'), and create cross-aliases.
    """
    # Discover actual team IDs present in this match via player_map_stats
    actual_teams = [r[0] for r in conn.execute(
        """SELECT DISTINCT pms.team_id
           FROM player_map_stats pms
           JOIN maps m ON m.map_id = pms.map_id
           WHERE m.match_id = ? AND pms.team_id IS NOT NULL""",
        (match_id,)
    ).fetchall()]

    if not actual_teams:
        return  # no player stats available — nothing to fix

    folder_team_a_id = norm._team_cache.get(folder_team_a.lower())
    folder_team_b_id = norm._team_cache.get(folder_team_b.lower())

    # Track which actual teams have already been successfully mapped
    claimed_actual = set()
    for ftid in [folder_team_a_id, folder_team_b_id]:
        if ftid and ftid in actual_teams:
            claimed_actual.add(ftid)

    for folder_name, folder_tid in [(folder_team_a, folder_team_a_id),
                                    (folder_team_b, folder_team_b_id)]:
        if not folder_tid:
            continue
        if folder_tid in actual_teams:
            claimed_actual.add(folder_tid)
            continue  # already correct — the folder name matched the stats name

        # The folder team ID is an orphan; find the unclaimed actual team for it
        candidate = None
        for at in actual_teams:
            if at not in claimed_actual:
                candidate = at
                break

        if not candidate:
            continue

        claimed_actual.add(candidate)
        log.debug(f"  Reconcile: '{folder_name}' ({folder_tid}) → {candidate}")

        # Add cross-alias: folder_name → canonical actual team
        conn.execute(
            "INSERT OR IGNORE INTO team_aliases(alias, team_id) VALUES(?,?)",
            (folder_name, candidate)
        )
        norm._team_cache[folder_name.lower()] = candidate

        # Update match record to point to canonical team
        conn.execute(
            "UPDATE matches SET team_a_id=? WHERE match_id=? AND team_a_id=?",
            (candidate, match_id, folder_tid)
        )
        conn.execute(
            "UPDATE matches SET team_b_id=? WHERE match_id=? AND team_b_id=?",
            (candidate, match_id, folder_tid)
        )

        # Delete orphan team entry (no players, only created from folder name)
        player_count = conn.execute(
            "SELECT COUNT(*) FROM players WHERE current_team_id=?", (folder_tid,)
        ).fetchone()[0]
        if player_count == 0:
            conn.execute("DELETE FROM team_aliases WHERE team_id=? AND alias=?",
                         (folder_tid, folder_name))
            conn.execute("DELETE FROM teams WHERE team_id=?", (folder_tid,))
            log.debug(f"  Removed orphan team: {folder_tid}")


def ingest_match(conn, norm, match_dir: Path, event_id: str, stage: str):
    match_info = parse_match_folder(match_dir, event_id, stage)
    match_id, team_a_id, team_b_id = upsert_match(conn, norm, match_info, event_id)
    log.info(f"  Match: {match_dir.name}  →  {match_id}")

    subfolders = {d.name.lower(): d for d in match_dir.iterdir() if d.is_dir()}

    if "player_stats" in subfolders:
        ingest_player_stats(conn, norm, match_id, subfolders["player_stats"], event_id)
        # Reconcile folder team names (e.g. "FNATIC") with stat abbreviations (e.g. "FNC")
        reconcile_match_teams(conn, norm, match_id,
                              match_info["team_a_name"], match_info["team_b_name"])
    if "rounds" in subfolders:
        ingest_rounds(conn, norm, match_id, subfolders["rounds"], event_id)
    if "map_veto" in subfolders:
        ingest_map_veto(conn, norm, match_id, subfolders["map_veto"], event_id)
    if "economy" in subfolders:
        ingest_economy(conn, norm, match_id, subfolders["economy"], event_id)
    if "performance" in subfolders:
        ingest_performance(conn, norm, match_id, subfolders["performance"], event_id)

    conn.commit()


def ingest_stage(conn, norm, stage_dir: Path, event_id: str):
    stage_name = stage_dir.name.replace("_", " ")
    log.info(f"Stage: {stage_name}")
    for match_dir in sorted(stage_dir.iterdir()):
        if not match_dir.is_dir():
            continue
        if "_vs_" not in match_dir.name:
            continue
        try:
            ingest_match(conn, norm, match_dir, event_id, stage_name)
        except Exception as e:
            log.error(f"  ERROR ingesting match {match_dir.name}: {e}", exc_info=True)


def ingest_event(conn, norm, event_dir: Path):
    event_info = parse_event_folder(event_dir)
    upsert_event(conn, norm, event_info)
    event_id = event_info["event_id"]
    log.info(f"\nEvent: {event_info['event_name']}  (id={event_id})")

    for stage_dir in sorted(event_dir.iterdir()):
        if stage_dir.is_dir():
            ingest_stage(conn, norm, stage_dir, event_id)

    conn.commit()


# ============================================================
# CLI entry point
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Ingest VLR CSV data into SQLite (schema_v2)")
    parser.add_argument("--db",      default=str(DEFAULT_DB), help="SQLite DB path")
    parser.add_argument("--root",    default=str(EVENTS_ROOT), help="VCT Events root dir")
    parser.add_argument("--event",   help="Filter to a specific event folder name")
    parser.add_argument("--match",   help="Filter to a specific match folder name")
    parser.add_argument("--year",    type=int, help="Filter to a specific year")
    args = parser.parse_args()

    db_path = Path(args.db)
    events_root = Path(args.root)

    log.info(f"DB:   {db_path}")
    log.info(f"Root: {events_root}")

    conn = open_db(db_path)
    norm = Normaliser(conn)

    year_dirs = sorted(events_root.iterdir())
    for year_dir in year_dirs:
        if not year_dir.is_dir():
            continue
        if args.year and year_dir.name != str(args.year):
            continue

        for event_dir in sorted(year_dir.iterdir()):
            if not event_dir.is_dir():
                continue
            if args.event and args.event.lower() not in event_dir.name.lower():
                continue

            if args.match:
                # Only ingest a specific match
                event_info = parse_event_folder(event_dir)
                upsert_event(conn, norm, event_info)
                event_id = event_info["event_id"]
                for stage_dir in sorted(event_dir.iterdir()):
                    if not stage_dir.is_dir():
                        continue
                    for match_dir in sorted(stage_dir.iterdir()):
                        if not match_dir.is_dir():
                            continue
                        if args.match.lower() in match_dir.name.lower():
                            ingest_match(conn, norm, match_dir, event_id, stage_dir.name.replace("_", " "))
            else:
                ingest_event(conn, norm, event_dir)

    conn.close()
    log.info("\nDone.")


if __name__ == "__main__":
    main()
