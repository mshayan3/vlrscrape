import csv
import requests
from bs4 import BeautifulSoup
import re


def fetch_map_veto(url, output_file='map_veto.csv'):
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print("Failed to fetch the webpage.")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    veto_text = soup.select_one('.match-header-note')

    if not veto_text:
        print("No veto data found.")
        return

    veto_text = veto_text.text.strip()
    map_pattern = re.findall(r"\b(?:ban|pick)\s+([A-Za-z0-9\-]+)", veto_text, re.IGNORECASE)
    map_list = set(map_pattern)

    veto_entries = veto_text.split(';')
    map_data = {}

    for entry in veto_entries:
        parts = entry.strip().split()
        if len(parts) < 3:
            continue

        team, action, map_name = parts[0], parts[1], parts[2].lower()

        if map_name not in map_data:
            map_data[map_name] = {"pick": "", "ban": ""}

        if action == "ban":
            map_data[map_name]["ban"] = team
        elif action == "pick":
            map_data[map_name]["pick"] = team

    banned_maps = {m for m, d in map_data.items() if d["ban"]}
    picked_maps = {m for m, d in map_data.items() if d["pick"]}
    decider_map = list(map_list - banned_maps - picked_maps)

    if decider_map:
        map_data[decider_map[0]] = {"pick": "decider", "ban": ""}

    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["map", "pick", "ban"])
        for map_name, details in map_data.items():
            writer.writerow([map_name, details["pick"], details["ban"]])

    print(f"Data saved to {output_file}")

# Example usage:
# fetch_map_veto("https://www.vlr.gg/428005/100-thieves-vs-nrg-esports-champions-tour-2025-americas-kickoff-lr")
