"""
Test queries for Valorant Champions 2025 database.
"""
import sqlite3

DB_NAME = "valorant_champions_2025.db"

def run_test_queries():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    print("=== Database Verification ===\n")
    
    # 1. Count tables
    print("1. Table Row Counts:")
    tables = ['events', 'teams', 'players', 'agents', 'matches', 'maps', 'rounds', 'map_veto', 'player_map_stats']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"   {table}: {count}")
    
    print("\n2. Sample Teams:")
    cursor.execute("SELECT team_id, team_name FROM teams LIMIT 10")
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]}")
    
    print("\n3. Sample Players:")
    cursor.execute("SELECT player_id, player_name, current_team_id FROM players LIMIT 10")
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]} ({row[2]})")
    
    print("\n4. Matches:")
    cursor.execute("""
        SELECT m.match_name, t1.team_name, t2.team_name
        FROM matches m
        LEFT JOIN teams t1 ON m.team_a_id = t1.team_id
        LEFT JOIN teams t2 ON m.team_b_id = t2.team_id
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]} vs {row[2]}")
    
    print("\n5. Top Players by ACS (All Maps):")
    cursor.execute("""
        SELECT p.player_name, t.team_name, AVG(pms.acs) as avg_acs, COUNT(*) as maps_played
        FROM player_map_stats pms
        JOIN players p ON pms.player_id = p.player_id
        JOIN teams t ON pms.team_id = t.team_id
        GROUP BY p.player_id
        HAVING maps_played >= 3
        ORDER BY avg_acs DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]} ({row[1]}): {row[2]:.1f} ACS ({row[3]} maps)")
    
    print("\n6. Map Veto Sample:")
    cursor.execute("""
        SELECT mv.match_id, mv.action_type, mv.map_name, t.team_name
        FROM map_veto mv
        LEFT JOIN teams t ON mv.team_id = t.team_id
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]} {row[2]} ({row[3] if row[3] else 'decider'})")
    
    print("\n7. Rounds with Economy:")
    cursor.execute("""
        SELECT r.map_id, r.round_number, r.winning_team_id, e.credits, e.buy_tier
        FROM rounds r
        LEFT JOIN round_economy e ON r.map_id = e.map_id AND r.round_number = e.round_number
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"   {row[0]} R{row[1]}: Winner={row[2]}, Credits={row[3]}, Tier={row[4]}")
    
    conn.close()
    print("\n[+] Database verification complete!")

if __name__ == "__main__":
    run_test_queries()
