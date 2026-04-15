"""
app.py — Text-to-SQL frontend for vlr_v2.db using OpenAI

Run:
    pip install flask openai
    python app.py

Then open http://localhost:5000
"""

from flask import Flask, request, jsonify, render_template_string
from openai import OpenAI
import sqlite3
import os

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "db", "vlr_v2.db")

SCHEMA = """
TABLES (use exactly these column names):

  events      : event_id PK, event_name, season_year, region
  teams       : team_id PK, team_name, region
  team_aliases: alias PK, team_id FK->teams
  players     : player_id PK, player_name, current_team_id FK->teams
  agents      : agent_id PK, agent_name

  matches     : match_id PK, event_id FK, match_name, stage, match_date,
                team_a_id FK->teams, team_b_id FK->teams, winner_team_id FK->teams
                -- match_name e.g. "NRG vs FNATIC Playoffs- Grand Final"
                -- stage EXACT values (case-sensitive): 'Playoffs'  OR  'Group Stage'
                -- DOES NOT have map_name or map_number

  maps        : map_id PK, match_id FK, map_number, map_name,
                team_a_score, team_b_score, winner_team_id FK->teams
                -- map_name e.g. 'Corrode', 'Lotus', 'Abyss', 'Ascent', 'Haven', 'Sunset', 'Bind'
                -- DOES NOT have match_name or stage
                -- IMPORTANT: many matches share the same map_name; always also filter by match
                --   when asking about a specific match e.g. AND mc.match_name LIKE '%Grand_Final%'

  rounds      : round_id PK, map_id FK, round_number, score_after,
                winning_team_id FK->teams,
                winning_side ('attack'|'defend'),
                win_method ('elimination'|'detonation'|'defuse'|'time')

  map_veto    : veto_id PK, match_id FK, order_no, team_id FK->teams,
                action_type ('pick'|'ban'|'decider'), map_name

  player_map_stats: pms_id PK, map_id FK, player_id FK, team_id FK,
                    side ('all'|'attack'|'defend'),
                    rating REAL, acs REAL, kills INT, deaths INT, assists INT,
                    kd_diff INT,
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
                       buy_type ('pistol'|'eco'|'semi_eco'|'semi'|'full_buy'),
                       rounds_played INT, rounds_won INT
                       -- one row per map per team per buy_type
                       -- multiple rows for the same map_name is EXPECTED (different matches)

  round_economy: econ_id PK, map_id FK, round_number INT, team_id FK,
                 side ('attack'|'defend'),  -- safe to filter on
                 bank_start INT,
                 loadout_value INT,
                 buy_tier ('pistol'|'eco'|'semi'|'full_buy')

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
5. kast and hs_pct are 0.0-1.0 fractions. Multiply by 100 for percentage display.
6. For "across the tournament" player aggregations: SUM/AVG with GROUP BY p.player_id.
7. side='all' gives overall stats; use side='attack' or side='defend' for half-specific.
8. round_economy.side values are 'attack' or 'defend' — safe to filter.
9. Limit to 50 rows unless user specifies otherwise.

DATA: VCT Champions 2025 — 34 matches, 88 maps, 16 teams, 81 players.

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
  <label>OpenAI API Key</label>
  <input type="password" id="apiKey" placeholder="sk-..." autocomplete="off">

  <div style="margin-top:16px">
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
  const apiKey   = document.getElementById("apiKey").value.trim();
  const question = document.getElementById("question").value.trim();
  const model    = document.getElementById("model").value;

  document.getElementById("err").textContent = "";
  document.getElementById("sqlCard").style.display = "none";
  document.getElementById("resultsCard").style.display = "none";

  if (!apiKey) { showErr("Enter your OpenAI API key."); return; }
  if (!question) { showErr("Enter a question."); return; }

  setLoading(true);

  try {
    const res = await fetch("/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ api_key: apiKey, question, model }),
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
    api_key  = data.get("api_key", "").strip()
    question = data.get("question", "").strip()
    model    = data.get("model", "gpt-4o-mini")

    if not api_key:
        return jsonify({"error": "Missing API key"})
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
