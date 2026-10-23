"""
app.py — Text-to-SQL frontend for vlr_v3.db using OpenAI

Setup:
    1. Copy .env.example to .env and add your OpenAI key
    2. pip install -r requirements.txt
    3. python app.py

Then open http://localhost:5000
"""

from flask import Flask, request, jsonify, render_template_string
from openai import OpenAI
import sqlite3
import os
from dotenv import load_dotenv

# Load .env from the project root (same folder as this file)
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "db", "vlr_v3.db")

SCHEMA = """
TABLES (use exactly these column names):

  events      : event_id PK, event_name, season_year, region
  teams       : team_id PK, team_name, region
  team_aliases: alias PK, team_id FK->teams
  players     : player_id PK, player_name, vlr_player_id, current_team_id FK->teams
                -- vlr_player_id = numeric VLR.gg id; current_team_id is LATEST team,
                --   NOT team-in-match (use player_map_stats.team_id for that)
  agents      : agent_id PK, agent_name

  matches     : match_id PK, event_id FK, vlr_match_id, match_name, stage, match_date,
                patch, best_of, team_a_id FK->teams, team_b_id FK->teams,
                team_a_map_score, team_b_map_score, winner_team_id FK->teams
                -- match_name e.g. "NRG vs FNATIC Playoffs- Grand Final"
                -- stage EXACT values (case-sensitive): 'Playoffs'  OR  'Group Stage'
                -- best_of = 1/3/5; team_a_map_score/team_b_map_score = SERIES score
                --   (maps won), e.g. 3 and 2. Use these for "final series/match score".
                -- DOES NOT have map_name or map_number

  maps        : map_id PK, match_id FK, vlr_game_id, map_number, map_name,
                team_a_score, team_b_score, winner_team_id FK->teams,
                pick_team_id FK->teams, duration_seconds
                -- map_name e.g. 'Corrode', 'Lotus', 'Abyss', 'Ascent', 'Haven', 'Sunset', 'Bind'
                -- team_a_score/team_b_score = ROUND score on that map (e.g. 13-3)
                -- pick_team_id = team that PICKED the map (NULL for the decider)
                -- duration_seconds = map length in seconds (e.g. 2434 = 40:34)
                -- DOES NOT have match_name or stage
                -- IMPORTANT: many matches share the same map_name; always also filter by match
                --   when asking about a specific match e.g. AND mc.match_name LIKE '%Grand_Final%'

  rounds      : round_id PK, map_id FK, round_number, score_after,
                winning_team_id FK->teams,
                winning_side ('attack'|'defend'),
                win_method ('elimination'|'detonation'|'defuse'|'time'),
                is_overtime INT (1 if round_number>24)

  map_veto    : veto_id PK, match_id FK, order_no, team_id FK->teams,
                action_type ('pick'|'ban'|'decider'), map_name

  player_map_stats: pms_id PK, map_id FK, player_id FK, team_id FK,
                    side ('all'|'attack'|'defend'),
                    rating REAL, acs REAL, kills INT, deaths INT, assists INT,
                    kd_diff INT,  -- K minus D differential (VLR "+/-"), NOT a ratio
                    kd_ratio REAL,-- generated kills/deaths ratio (use for "K/D ratio")
                    kast REAL,    -- fraction 0.0-1.0; multiply by 100 for %
                    adr REAL,
                    hs_pct REAL,  -- fraction 0.0-1.0; multiply by 100 for %
                    fk INT,       -- FIRST KILLS per map (use this for first-kill stats)
                    fd INT

  player_map_agents: map_id FK, player_id FK, agent_id FK

  player_map_advanced: pma_id PK, map_id FK, player_id FK, team_id FK,
                       multikill_2, multikill_3, multikill_4, multikill_5,
                       clutch_1v1, clutch_1v2, clutch_1v3, clutch_1v4, clutch_1v5,
                       econ_rating INT, plant_success INT, defuse_success INT

  player_vs_player_kills: pvpk_id PK, map_id FK,
                          killer_player_id FK->players, victim_player_id FK->players,
                          killer_team_id FK->teams, victim_team_id FK->teams,
                          kill_type ('all'|'fk'|'op'), kills_count INT
                          -- kill_type='all' = all kills between pair on that map
                          -- kill_type='fk'  = first-kill duels won (NOT total first kills per player)
                          -- For player first kill TOTALS use player_map_stats.fk, not this table

  map_economy_summary: mes_id PK, map_id FK, team_id FK,
                       buy_type ('pistol'|'eco'|'semi'|'full_buy'),
                       -- same vocabulary as round_economy.buy_tier
                       -- 'eco' is any sub-10k buy; 'semi' is a 10-18k half buy
                       rounds_played INT, rounds_won INT
                       -- one row per map per team per buy_type
                       -- multiple rows for the same map_name is EXPECTED (different matches)

  round_economy: econ_id PK, map_id FK, round_number INT, team_id FK,
                 side ('attack'|'defend'),  -- safe to filter on
                 bank_start INT,
                 loadout_value INT,
                 buy_tier ('pistol'|'eco'|'semi'|'full_buy')

PRE-AGGREGATED VIEWS — use ONLY for whole-tournament/event totals & records.
They already filter side='all' and pre-sum/average correctly, so they avoid the
2-3x double-counting you get from raw player_map_stats. Use them ONLY when the
question wants ONE row per player/team/agent for the entire event (e.g. "most
kills in the event", "best avg ACS", "team's overall map record").

DO NOT use these views for any per-map / per-match breakdown. If the question
says "per map", "each map", "every map", "map-by-map", "by map", "single best
map", or names/asks for an individual map or match, query the raw tables and
GROUP BY mp.map_id (with WHERE pms.side='all'). The views collapse all maps into
one row and will give the wrong answer for these.

  v_player_tournament_totals: event_id, event_name, player_id, player_name,
        team_id, team_name, maps_played, kills, deaths, assists,
        avg_rating, avg_acs, avg_kast_pct, avg_adr, avg_hs_pct,
        first_kills, first_deaths
        -- one row per (event, player). kast/hs/acs already averaged; *_pct are
        --   already 0-100 (do NOT multiply by 100 again). For "most kills in the
        --   event", "best avg ACS/KAST/ADR", "most maps played" -> use this view.

  v_team_map_record: team_id, team_name, map_name, maps_played, wins, losses, win_pct
        -- one row per (team, map). For "team's record/win rate on each map".

  v_agent_pick_rate: event_id, event_name, stage, agent_id, agent_name, picks
        -- one row per (event, stage, agent). For "most picked agents".

ALIAS CONVENTIONS (always use these):
  matches              -> mc
  maps                 -> mp
  rounds               -> r
  teams                -> t  (ta / tb when joining same table twice)
  players              -> p  (pk = killer, pv = victim in kill matrix)
  agents               -> a
  player_map_stats     -> pms
  player_map_advanced  -> pma
  player_map_agents    -> pag
  player_vs_player_kills -> pvpk
  map_economy_summary  -> mes
  round_economy        -> re
  map_veto             -> mv

CRITICAL RULES:
1. stage is CASE-SENSITIVE: 'Playoffs' and 'Group Stage' exactly.
2. map_name is on 'maps' (mp), match_name and stage are on 'matches' (mc).
3. When filtering by both match and map, always include BOTH conditions:
      AND mp.map_name = 'Corrode'  AND mc.match_name LIKE '%Grand_Final%'
4. FIRST KILLS: always use SUM(pms.fk) from player_map_stats. NEVER use player_vs_player_kills for first-kill totals.
5. kast and hs_pct in player_map_stats are 0.0-1.0 fractions. Multiply by 100 for percentage display.
   (The v_* views already store *_pct as 0-100 — do NOT multiply those again.)
6. For "across the tournament" player aggregations: SUM/AVG with GROUP BY p.player_id.
7. MANDATORY: any SUM/AVG/COUNT over player_map_stats (pms) MUST include
   `WHERE pms.side = 'all'`. There are 3 rows per player per map (all/attack/defend);
   forgetting this double- or triple-counts every total. If you only need overall
   totals, strongly prefer the v_* views above instead.
8. rounds (r) has winning_team_id and winning_side ONLY. It has NO `side` column and
   NO `team_id` column. The `side` and `team_id` columns live on round_economy (re).
   For "rounds won by side", join rounds r and group by r.winning_side / r.winning_team_id.
9. Respect every constraint in the question, including thresholds like
   "min 5 maps" -> add HAVING COUNT(DISTINCT pms.map_id) >= 5.
10. "total maps in the event" = COUNT(*) FROM maps; "total rounds" = COUNT(*) FROM rounds.
    Do not add filters that aren't asked for.
11. round_economy.side values are 'attack' or 'defend' — safe to filter.
12. Limit to 50 rows unless user specifies otherwise.
13. ALWAYS show team_name (join teams t), never a raw team_id slug like 'team-fnc'.
    For a map/match winner, join teams ON t.team_id = mp.winner_team_id and SELECT
    t.team_name. Same for match winners and any *_team_id column.
14. player_map_agents (pag) has ONLY map_id, player_id, agent_id — NO team_id and
    NO stage. To get a player's team or the stage, join player_map_stats (pms) /
    maps (mp) / matches (mc). There is also no pms.stage — stage lives on matches.
15. "min N maps" thresholds use COUNT(DISTINCT pms.map_id) >= N (or maps_played >= N
    on a v_* view). NEVER COUNT(DISTINCT event_id) — there is only one event.
16. "won by detonation/defuse/elimination/time" = GROUP BY r.win_method (NOT
    winning_side). win_method is on rounds (r).
17. SQLite has NO STDEV/STDDEV/VARIANCE. Compute variance manually if needed, or
    avoid. There is no re.rounds_played (use map_economy_summary) and no mc.match_id
    confusion — match_id is the PK on matches and FK on maps.

DATA: the DB holds exactly ONE event. Its stored event_name is
'Valorant Champions 2025' (34 matches, 88 maps, 16 teams, 81 players).
Since there is only one event, do NOT add a WHERE event_name = ... filter
(it is unnecessary and an exact-string mismatch returns 0 rows). If you must
reference it, use the exact string 'Valorant Champions 2025'.

EXAMPLE PATTERNS:

-- Player stats for a specific match + map:
SELECT p.player_name, t.team_name, pms.acs, pms.kills, pms.deaths,
       ROUND(pms.kast*100,1) AS kast_pct
FROM player_map_stats pms
JOIN players p  ON p.player_id  = pms.player_id
JOIN teams   t  ON t.team_id    = pms.team_id
JOIN maps    mp ON mp.map_id    = pms.map_id
JOIN matches mc ON mc.match_id  = mp.match_id
WHERE mc.match_name LIKE '%Grand_Final%' AND mp.map_name = 'Corrode' AND pms.side = 'all'
ORDER BY pms.acs DESC;

-- Tournament first-kill leaderboard (use pms.fk):
SELECT p.player_name, t.team_name, SUM(pms.fk) AS total_fk
FROM player_map_stats pms
JOIN players p ON p.player_id = pms.player_id
JOIN teams   t ON t.team_id   = pms.team_id
WHERE pms.side = 'all'
GROUP BY p.player_id ORDER BY total_fk DESC LIMIT 10;

-- Team win rate by map:
SELECT mp.map_name,
       COUNT(CASE WHEN mp.winner_team_id = t.team_id THEN 1 END) AS wins,
       COUNT(*) AS played,
       ROUND(COUNT(CASE WHEN mp.winner_team_id = t.team_id THEN 1 END)*100.0/COUNT(*),1) AS win_pct
FROM maps mp
JOIN matches mc ON mc.match_id = mp.match_id
JOIN teams t ON (mc.team_a_id = t.team_id OR mc.team_b_id = t.team_id)
WHERE t.team_name = 'NRG'
GROUP BY mp.map_name ORDER BY win_pct DESC;

-- Agent meta in a stage:
SELECT a.agent_name, COUNT(*) AS picks
FROM player_map_agents pag
JOIN agents  a  ON a.agent_id  = pag.agent_id
JOIN maps    mp ON mp.map_id   = pag.map_id
JOIN matches mc ON mc.match_id = mp.match_id
WHERE mc.stage = 'Playoffs'
GROUP BY a.agent_id ORDER BY picks DESC LIMIT 15;

-- Kill matrix for specific match + map:
SELECT pk.player_name AS killer, kt.team_name, pv.player_name AS victim, vt.team_name, pvpk.kills_count
FROM player_vs_player_kills pvpk
JOIN players pk ON pk.player_id = pvpk.killer_player_id
JOIN players pv ON pv.player_id = pvpk.victim_player_id
JOIN teams   kt ON kt.team_id   = pvpk.killer_team_id
JOIN teams   vt ON vt.team_id   = pvpk.victim_team_id
JOIN maps    mp ON mp.map_id    = pvpk.map_id
JOIN matches mc ON mc.match_id  = mp.match_id
WHERE mc.match_name LIKE '%Grand_Final%' AND mp.map_name = 'Corrode' AND pvpk.kill_type = 'all'
ORDER BY pvpk.kills_count DESC;
"""

SYSTEM_PROMPT = f"""You are a SQL expert for a Valorant esports database (SQLite).
Given a natural language question, return ONLY a valid SQLite SELECT query.
No markdown, no explanation, no code fences — raw SQL only.
Follow the alias conventions and critical rules in the schema exactly.
Use meaningful column aliases in SELECT for readability.

{SCHEMA}"""

# ─── HTML UI ───────────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>vlrscrape · Text to SQL</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #0d1117; color: #e6edf3; min-height: 100vh;
    display: flex; flex-direction: column; align-items: center;
    padding: 40px 20px;
  }
  h1 { font-size: 1.4rem; font-weight: 600; margin-bottom: 4px; }
  .sub { color: #7d8590; font-size: 0.85rem; margin-bottom: 32px; }
  .card {
    background: #161b22; border: 1px solid #30363d; border-radius: 10px;
    padding: 24px; width: 100%; max-width: 800px; margin-bottom: 20px;
  }
  label { display: block; font-size: 0.8rem; color: #7d8590;
          text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }
  input[type=text], input[type=password], textarea {
    width: 100%; background: #0d1117; border: 1px solid #30363d;
    border-radius: 6px; color: #e6edf3; padding: 10px 12px;
    font-size: 0.95rem; outline: none; font-family: inherit;
  }
  input:focus, textarea:focus { border-color: #388bfd; }
  textarea { min-height: 80px; resize: vertical; }
  .row { display: flex; gap: 12px; margin-top: 16px; align-items: flex-end; }
  button {
    background: #238636; color: #fff; border: none; border-radius: 6px;
    padding: 10px 20px; font-size: 0.9rem; cursor: pointer; white-space: nowrap;
    transition: background .15s;
  }
  button:hover { background: #2ea043; }
  button:disabled { background: #21262d; color: #484f58; cursor: default; }
  .model-select {
    background: #0d1117; border: 1px solid #30363d; border-radius: 6px;
    color: #e6edf3; padding: 10px 12px; font-size: 0.9rem; cursor: pointer;
  }
  pre {
    background: #0d1117; border: 1px solid #30363d; border-radius: 6px;
    padding: 14px; font-size: 0.85rem; overflow-x: auto;
    color: #79c0ff; margin-top: 16px; white-space: pre-wrap;
  }
  .results-wrap { overflow-x: auto; margin-top: 16px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th {
    background: #21262d; color: #7d8590; text-align: left;
    padding: 8px 12px; font-weight: 500; border-bottom: 1px solid #30363d;
    text-transform: uppercase; font-size: 0.75rem; letter-spacing: .05em;
  }
  td { padding: 8px 12px; border-bottom: 1px solid #21262d; color: #e6edf3; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #161b22; }
  .meta { color: #7d8590; font-size: 0.8rem; margin-top: 10px; }
  .err { color: #f85149; font-size: 0.85rem; margin-top: 12px; }
  .spinner { display: none; color: #7d8590; font-size: 0.85rem; margin-top: 12px; }
  .examples { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
  .chip {
    background: #21262d; border: 1px solid #30363d; border-radius: 20px;
    padding: 5px 12px; font-size: 0.78rem; cursor: pointer; color: #7d8590;
    transition: all .15s;
  }
  .chip:hover { border-color: #388bfd; color: #e6edf3; }
  .section-title { font-size: 0.75rem; color: #7d8590; text-transform: uppercase;
                   letter-spacing: .05em; margin-bottom: 8px; }
</style>
</head>
<body>

<h1>vlrscrape · Text to SQL</h1>
<p class="sub">VCT Champions 2025 · 34 matches · 81 players · GPT powered</p>

<div class="card">
  <div style="margin-top:0">
    <label>Question</label>
    <textarea id="question" placeholder="Who had the highest ACS in the Grand Final?"></textarea>
  </div>

  <div class="row">
    <select class="model-select" id="model">
      <option value="gpt-4o-mini">gpt-4o-mini (fast)</option>
      <option value="gpt-4o">gpt-4o</option>
    </select>
    <button id="runBtn" onclick="run()">Run →</button>
  </div>

  <div class="spinner" id="spinner">⏳ Generating SQL…</div>
  <div class="err" id="err"></div>
</div>

<div class="card" id="sqlCard" style="display:none">
  <div class="section-title">Generated SQL</div>
  <pre id="sqlOut"></pre>
</div>

<div class="card" id="resultsCard" style="display:none">
  <div class="section-title">Results</div>
  <div class="results-wrap"><table id="tbl"></table></div>
  <div class="meta" id="meta"></div>
</div>

<div class="card">
  <div class="section-title">Example questions</div>
  <div class="examples" id="chips"></div>
</div>

<script>
const examples = [
  "Who had the highest ACS in the Grand Final?",
  "Show NRG's win rate on each map across the tournament",
  "Which agents were most picked in the playoffs?",
  "Who had the most triple kills in the whole event?",
  "Show the kill matrix for the Grand Final on Corrode",
  "Which player had the best KAST on attack across all maps?",
  "Compare NRG vs FNC economy on Corrode round by round",
  "Who clutched the most 1v2s in the tournament?",
  "Show all maps where full-buy win rate was below 50%",
  "Who topped the first-kill leaderboard in the playoffs?",
];

// Populate chips
const chips = document.getElementById("chips");
examples.forEach(q => {
  const el = document.createElement("span");
  el.className = "chip";
  el.textContent = q;
  el.onclick = () => { document.getElementById("question").value = q; };
  chips.appendChild(el);
});

async function run() {
  const question = document.getElementById("question").value.trim();
  const model    = document.getElementById("model").value;

  document.getElementById("err").textContent = "";
  document.getElementById("sqlCard").style.display = "none";
  document.getElementById("resultsCard").style.display = "none";

  if (!question) { showErr("Enter a question."); return; }

  setLoading(true);

  try {
    const res = await fetch("/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, model }),
    });

    const data = await res.json();

    if (data.error) { showErr(data.error); setLoading(false); return; }

    // Show SQL
    document.getElementById("sqlOut").textContent = data.sql;
    document.getElementById("sqlCard").style.display = "block";

    // Show results
    const tbl = document.getElementById("tbl");
    tbl.innerHTML = "";
    if (data.columns && data.rows) {
      const thead = tbl.insertRow();
      data.columns.forEach(c => {
        const th = document.createElement("th");
        th.textContent = c;
        thead.appendChild(th);
      });
      data.rows.forEach(row => {
        const tr = tbl.insertRow();
        row.forEach(cell => {
          const td = tr.insertCell();
          td.textContent = cell ?? "—";
        });
      });
      document.getElementById("meta").textContent =
        `${data.rows.length} row${data.rows.length !== 1 ? "s" : ""}`;
      document.getElementById("resultsCard").style.display = "block";
    }
  } catch (e) {
    showErr("Request failed: " + e.message);
  }

  setLoading(false);
}

function showErr(msg) { document.getElementById("err").textContent = msg; }
function setLoading(on) {
  document.getElementById("spinner").style.display = on ? "block" : "none";
  document.getElementById("runBtn").disabled = on;
}

document.getElementById("question").addEventListener("keydown", e => {
  if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) run();
});
</script>
</body>
</html>
"""

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/query", methods=["POST"])
def query():
    data     = request.get_json()
    question = data.get("question", "").strip()
    model    = data.get("model", "gpt-4o-mini")
    api_key  = os.environ.get("OPENAI_API_KEY", "").strip()

    if not api_key:
        return jsonify({"error": "OPENAI_API_KEY not set — add it to your .env file"})
    if not question:
        return jsonify({"error": "Missing question"})

    # 1. Generate SQL via OpenAI
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": question},
            ],
            temperature=0,
        )
        sql = response.choices[0].message.content.strip()
        # Strip markdown fences if model adds them anyway
        sql = sql.strip("` \n")
        if sql.lower().startswith("sql"):
            sql = sql[3:].strip()
    except Exception as e:
        return jsonify({"error": f"OpenAI error: {e}"})

    # 2. Execute SQL against the DB
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql)
        columns = [d[0] for d in cur.description]
        rows    = [list(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        return jsonify({"sql": sql, "error": f"SQL error: {e}"})

    return jsonify({"sql": sql, "columns": columns, "rows": rows})


if __name__ == "__main__":
    print("Starting vlrscrape Text-to-SQL")
    print(f"DB: {DB_PATH}")
    print("Open http://localhost:5000\n")
    app.run(debug=True, port=5000)
