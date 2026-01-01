import sqlite3
import pandas as pd

conn = sqlite3.connect("valorant_analytics.db")

print("--- Events ---")
print(pd.read_sql_query("SELECT * FROM Events", conn))

print("\n--- Match Count ---")
print(pd.read_sql_query("SELECT COUNT(*) FROM Matches", conn))

print("\n--- Sample Player Stats (Zekken?) ---")
try:
    print(pd.read_sql_query("SELECT * FROM PlayerStats WHERE player_name LIKE '%Zekken%' LIMIT 5", conn))
except:
    print("Query failed")

print("\n--- Top 5 ACS in database ---")
print(pd.read_sql_query("SELECT player_name, acs, map_name FROM PlayerStats ORDER BY acs DESC LIMIT 5", conn))

conn.close()
