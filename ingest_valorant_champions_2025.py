"""
Ingest Valorant Champions 2025 data into normalized database.
Transforms CSV files from VCT Events/2025/Valorant_Champions_2025/ into schema.sql structure.
"""
import sqlite3
import os
import pandas as pd
import re
from datetime import datetime
from db_helpers import DBHelpers

DB_NAME = "valorant_champions_2025.db"
DATA_DIR = "VCT Events/2025/Valorant_Champions_2025"

def init_db():
    """Initialize database with schema."""
    conn = sqlite3.connect(DB_NAME)
    
    # Execute schema
    with open('schema.sql', 'r') as f:
        conn.executescript(f.read())
    
    print(f"[+] Database {DB_NAME} initialized with schema")
    return conn

def ingest_event(conn, helpers):
    """Insert the event record."""
    event_id = 'vc2025'
    
    helpers.cursor.execute(
        "INSERT OR IGNORE INTO events (event_id, event_name, season_year, source) VALUES (?, ?, ?, ?)",
        (event_id, 'Valorant Champions 2025', 2025, 'vlr.gg')
    )
    helpers.commit()
    print(f"[+] Event: {event_id}")
    return event_id

def parse_match_folder_name(folder_name):
    """
    Parse match folder name to extract teams and stage.
    Example: 'NRG_vs_FNATIC_Playoffs-_Grand_Final'
    Returns: (team_a, team_b, stage_info)
    """
    # Split by '_vs_'
    if '_vs_' not in folder_name:
        return None, None, None
    
    parts = folder_name.split('_vs_')
    team_a = parts[0].replace('_', ' ')
    
    # Rest contains team_b and stage info
    remainder = parts[1]
    
    # Find stage marker (usually has '-' or contains keywords like 'Playoffs', 'Group')
    # Strategy: find last occurrence of common stage words
    stage_markers = ['Playoffs', 'Group_Stage', 'Group-Stage', 'Stage']
    stage_start_idx = -1
    
    for marker in stage_markers:
        idx = remainder.rfind(marker)
        if idx != -1:
            stage_start_idx = idx
            break
    
    if stage_start_idx != -1:
        team_b = remainder[:stage_start_idx].rstrip('_-').replace('_', ' ')
        stage_info = remainder[stage_start_idx:].replace('_', ' ')
    else:
        team_b = remainder.replace('_', ' ')
        stage_info = "Unknown"
    
    return team_a.strip(), team_b.strip(), stage_info.strip()

def ingest_match(conn, helpers, event_id, stage_name, match_folder, match_path):
    """Ingest a single match."""
    team_a, team_b, match_info = parse_match_folder_name(match_folder)
    
    if not team_a or not team_b:
        print(f"[!]  Could not parse team names from: {match_folder}")
        return None
    
    # Ensure teams exist
    team_a_id = helpers.ensure_team(team_a)
    team_b_id = helpers.ensure_team(team_b)
    
    # Create match_id
    match_id = helpers.slugify(match_folder)
    
    # Insert match
    helpers.cursor.execute('''
        INSERT OR IGNORE INTO matches 
        (match_id, event_id, match_name, stage, team_a_id, team_b_id)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (match_id, event_id, match_info, stage_name, team_a_id, team_b_id))
    
    print(f"  [*] Match: {team_a} vs {team_b} ({match_info})")
    return match_id, team_a_id, team_b_id

def ingest_map_veto(conn, helpers, match_id, match_path):
    """Ingest map veto data."""
    veto_file = os.path.join(match_path, 'map_veto', 'map_veto.csv')
    if not os.path.exists(veto_file):
        return
    
    df = pd.read_csv(veto_file)
    
    for idx, row in df.iterrows():
        map_name = str(row.get('map', '')).strip() if not pd.isna(row.get('map')) else ''
        pick_team = str(row.get('pick', '')).strip() if not pd.isna(row.get('pick')) else ''
        ban_team = str(row.get('ban', '')).strip() if not pd.isna(row.get('ban')) else ''
        
        if not map_name:
            continue
        
        # Determine action and team
        if pick_team and pick_team != '':
            action_type = 'decider' if pick_team.lower() == 'decider' else 'pick'
            team_id = helpers.ensure_team(pick_team) if pick_team.lower() != 'decider' else None
        elif ban_team and ban_team != '':
            action_type = 'ban'
            team_id = helpers.ensure_team(ban_team)
        else:
            continue
        
        veto_id = f"{match_id}_v{idx}"
        
        helpers.cursor.execute('''
            INSERT OR IGNORE INTO map_veto
            (veto_id, match_id, order_no, team_id, action_type, map_name)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (veto_id, match_id, idx, team_id, action_type, map_name))

def ingest_maps_and_rounds(conn, helpers, match_id, match_path, team_a_id, team_b_id):
    """Ingest maps, rounds, player stats, economy, and kill matrices."""
    player_stats_dir = os.path.join(match_path, 'player_stats')
    
    if not os.path.exists(player_stats_dir):
        return
    
    # Find all map CSV files (not All_Maps.csv)
    map_files = [f for f in os.listdir(player_stats_dir) 
                 if f.endswith('.csv') and f.startswith('Map_')]
    
    for map_file in map_files:
        # Parse map number and name from filename (e.g., "Map_1_1Corrode.csv")
        # The format is Map_{number}_{mapname} where mapname often starts with a digit
        match_pattern = re.match(r'Map_(\d+)_(.+)\.csv', map_file)
        if not match_pattern:
            continue
        
        map_number = int(match_pattern.group(1))
        full_map_name = match_pattern.group(2)  # This is "1Corrode", "2Lotus", etc.
        
        # Extract clean map name (without the leading number)
        map_name_clean = re.sub(r'^\d+', '', full_map_name)  # "Corrode", "Lotus", etc.
        
        map_id = f"{match_id}_m{map_number}"
        
        # Insert map (use clean name for the map_name field)
        helpers.cursor.execute('''
            INSERT OR IGNORE INTO maps
            (map_id, match_id, map_number, map_name)
            VALUES (?, ?, ?, ?)
        ''', (map_id, match_id, map_number, map_name_clean))
        
        # Ingest player stats for this map
        ingest_player_stats(conn, helpers, map_id, match_path, map_file, map_name_clean)
        
        # Ingest rounds for this map (use full_map_name for file lookup)
        ingest_rounds(conn, helpers, map_id, match_path, full_map_name)
        
        # Ingest economy for this map
        ingest_economy(conn, helpers, map_id, match_path, full_map_name, team_a_id, team_b_id)
        
        # Ingest kill matrix for this map
        ingest_kill_matrix(conn, helpers, map_id, match_path, full_map_name)

def ingest_player_stats(conn, helpers, map_id, match_path, map_file, map_name):
    """Ingest player stats for a map."""
    stats_file = os.path.join(match_path, 'player_stats', map_file)
    df = pd.read_csv(stats_file)
    
    # Filter to 'All' side only
    df_all = df[df['Side'] == 'All']
    
    for _, row in df_all.iterrows():
        player_name = row.get('Player', '').strip()
        team_name = row.get('Team', '').strip()
        agents_str = row.get('Agents', '')
        
        if not player_name:
            continue
        
        # Parse agent (first one if multiple)
        agent_name = agents_str.split(',')[0].strip() if agents_str else None
        
        player_id = helpers.ensure_player(player_name, team_name)
        team_id = helpers.normalize_team_name(team_name)
        agent_id = helpers.ensure_agent(agent_name) if agent_name else None
        
        pms_id = f"{map_id}_{player_id}"
        
        helpers.cursor.execute('''
            INSERT OR IGNORE INTO player_map_stats
            (pms_id, map_id, player_id, team_id, agent_id, acs, kills, deaths, assists, 
             adr, kast, hs_pct, rating, fk, fd)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            pms_id, map_id, player_id, team_id, agent_id,
            row.get('ACS'), row.get('K'), row.get('D'), row.get('A'),
            row.get('ADR'), 
            float(row.get('KAST', '0%').rstrip('%')) if row.get('KAST') else None,
            float(row.get('HS%', '0%').rstrip('%')) if row.get('HS%') else None,
            row.get('R2.0'), row.get('FK'), row.get('FD')
        ))

def ingest_rounds(conn, helpers, map_id, match_path, map_name):
    """Ingest round data."""
    # Try with number prefix first (e.g., 1Corrode_rounds.csv)
    rounds_file = os.path.join(match_path, 'rounds', f"{map_name}_rounds.csv")
    if not os.path.exists(rounds_file):
        return
    
    df = pd.read_csv(rounds_file)
    
    for _, row in df.iterrows():
        round_number = row.get('Round Number')
        if pd.isna(round_number):
            continue
        
        try:
            round_number = int(round_number)
        except:
            continue
        
        # Skip empty rows
        score = str(row.get('Score', '')).strip() if not pd.isna(row.get('Score')) else ''
        if not score or score == '':
            continue
            
        winning_team = str(row.get('Winning Team', '')).strip() if not pd.isna(row.get('Winning Team')) else ''
        winning_side = str(row.get('Winning Side', '')).strip() if not pd.isna(row.get('Winning Side')) else ''
        win_method = str(row.get('Win Method', '')).strip() if not pd.isna(row.get('Win Method')) else ''
        
        round_id = f"{map_id}_r{round_number}"
        winning_team_id = helpers.normalize_team_name(winning_team) if winning_team else None
        
        helpers.cursor.execute('''
            INSERT OR IGNORE INTO rounds
            (round_id, map_id, round_number, score, winning_team_id, winning_side, win_method)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (round_id, map_id, round_number, score, winning_team_id, winning_side, win_method))

def ingest_economy(conn, helpers, map_id, match_path, map_name, team_a_id, team_b_id):
    """Ingest round economy data from visual matrix format."""
    econ_file = os.path.join(match_path, 'economy', f"{map_name}_rounds_economy.csv")
    if not os.path.exists(econ_file):
        return
    
    try:
        # Read as list of rows (it's a visual matrix, not standard CSV)
        with open(econ_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            if not line.strip() or line.startswith('(BANK)'):
                # Parse economy row
                cells = line.split(',')
                if len(cells) < 2:
                    continue
                
                # cells[0] is "(BANK) TEAM_A TEAM_B (BANK)"
                # cells[1+] are like "1 0.3k 0.4k" (round, team_a_credits, team_b_credits)
                for cell in cells[1:]:
                    cell = cell.strip()
                    if not cell:
                        continue
                    
                    # Parse: "1 0.3k 0.4k" or "13 0.1k 0.3k"
                    parts = cell.split()
                    if len(parts) < 3:
                        continue
                    
                    try:
                        round_num = int(parts[0])
                        credits_a = helpers.parse_credits(parts[1])
                        credits_b = helpers.parse_credits(parts[2])
                        
                        # Insert for both teams
                        if credits_a is not None:
                            econ_id_a = f"{map_id}_r{round_num}_{team_a_id}"
                            buy_tier_a = helpers.infer_buy_tier(credits_a)
                            helpers.cursor.execute('''
                                INSERT OR IGNORE INTO round_economy
                                (econ_id, map_id, round_number, team_id, credits, buy_tier)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (econ_id_a, map_id, round_num, team_a_id, credits_a, buy_tier_a))
                        
                        if credits_b is not None:
                            econ_id_b = f"{map_id}_r{round_num}_{team_b_id}"
                            buy_tier_b = helpers.infer_buy_tier(credits_b)
                            helpers.cursor.execute('''
                                INSERT OR IGNORE INTO round_economy
                                (econ_id, map_id, round_number, team_id, credits, buy_tier)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (econ_id_b, map_id, round_num, team_b_id, credits_b, buy_tier_b))
                    except (ValueError, IndexError):
                        continue
    except Exception as e:
        print(f"    [!] Error parsing economy for {map_name}: {e}")

def ingest_kill_matrix(conn, helpers, map_id, match_path, map_name):
    """Ingest player vs player kill matrix."""
    kills_file = os.path.join(match_path, 'performance', f"{map_name}_All_Kills.csv")
    if not os.path.exists(kills_file):
        return
    
    try:
        df = pd.read_csv(kills_file, index_col=0)
        
        # Rows are killers, columns are victims
        for killer_player in df.index:
            # Parse "player_name TEAM" format
            killer_parts = killer_player.rsplit(' ', 1)
            if len(killer_parts) < 2:
                continue
            killer_name, killer_team = killer_parts[0], killer_parts[1]
            
            killer_player_id = helpers.ensure_player(killer_name, killer_team)
            killer_team_id = helpers.normalize_team_name(killer_team)
            
            for victim_player in df.columns:
                victim_parts = victim_player.rsplit(' ', 1)
                if len(victim_parts) < 2:
                    continue
                victim_name, victim_team = victim_parts[0], victim_parts[1]
                
                victim_player_id = helpers.ensure_player(victim_name, victim_team)
                victim_team_id = helpers.normalize_team_name(victim_team)
                
                # Parse cell value: "8 0 +8" (kills given, kills received, diff)
                cell_value = str(df.loc[killer_player, victim_player]).strip()
                if cell_value and cell_value != 'nan':
                    parts = cell_value.split()
                    if len(parts) >= 1:
                        try:
                            kills_count = int(parts[0])
                            if kills_count > 0:
                                pvpk_id = f"{map_id}_{killer_player_id}_{victim_player_id}"
                                helpers.cursor.execute('''
                                    INSERT OR IGNORE INTO player_vs_player_kills
                                    (pvpk_id, map_id, killer_player_id, victim_player_id, 
                                     killer_team_id, victim_team_id, kills_count)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                ''', (pvpk_id, map_id, killer_player_id, victim_player_id,
                                      killer_team_id, victim_team_id, kills_count))
                        except ValueError:
                            continue
    except Exception as e:
        print(f"    [!] Error parsing kill matrix for {map_name}: {e}")

def main():
    """Main ingestion flow."""
    print("=== Valorant Champions 2025 Data Ingestion ===\n")
    
    # Initialize database
    conn = init_db()
    helpers = DBHelpers(conn)
    
    # Insert event
    event_id = ingest_event(conn, helpers)
    
    # Walk through stages (Playoffs, Group_Stage)
    if not os.path.exists(DATA_DIR):
        print(f"[-] Data directory not found: {DATA_DIR}")
        return
    
    for stage_name in os.listdir(DATA_DIR):
        stage_path = os.path.join(DATA_DIR, stage_name)
        if not os.path.isdir(stage_path):
            continue
        
        print(f"\n[>] Stage: {stage_name}")
        
        # Walk through matches
        for match_folder in os.listdir(stage_path):
            match_path = os.path.join(stage_path, match_folder)
            if not os.path.isdir(match_path):
                continue
            
            match_result = ingest_match(conn, helpers, event_id, stage_name, match_folder, match_path)
            if not match_result:
                continue
            
            match_id, team_a_id, team_b_id = match_result
            
            # Ingest sub-data
            ingest_map_veto(conn, helpers, match_id, match_path)
            ingest_maps_and_rounds(conn, helpers, match_id, match_path, team_a_id, team_b_id)
            
            helpers.commit()
    
    print(f"\n[+] Ingestion complete! Database: {DB_NAME}")
    conn.close()

if __name__ == "__main__":
    main()
