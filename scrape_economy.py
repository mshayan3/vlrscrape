import requests
import pandas as pd
from bs4 import BeautifulSoup

# Define the base URL
base_url = "https://www.vlr.gg/428005/100-thieves-vs-nrg-esports-champions-tour-2025-americas-kickoff-lr1/"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}

def fetch_soup(url):
    response = requests.get(url, headers=headers)
    return BeautifulSoup(response.text, "html.parser")

# Fetch and parse the HTML
soup = fetch_soup(base_url + "?game=all&tab=economy")

# Find all map tabs
map_tabs = soup.find_all("div", class_="vm-stats-gamesnav-item js-map-switch")
map_ids = {tab.get_text(strip=True): tab.get("data-game-id", "all") for tab in map_tabs}

# Ensure "All Maps" is included manually
if "All Maps" not in map_ids:
    map_ids["All Maps"] = "all"

# Define economy table class
econ_table_class = "wf-table-inset mod-econ"

# Loop through each map (including "All Maps")
for map_name, map_id in map_ids.items():
    print(f"Processing economy data for: {map_name}")

    # Fetch map-specific economy data
    map_soup = fetch_soup(base_url + f"?game={map_id}&tab=economy")
    map_container = map_soup.find("div", class_="vm-stats-game", attrs={"data-game-id": map_id})
    if not map_container:
        print(f"No economy data found for {map_name}")
        continue

    # Extract tables
    tables = map_container.find_all("table", class_=econ_table_class)

    # Ensure we have at least one table
    if len(tables) > 0:
        econ_table = tables[0]  # First table is the economy table
        rows = []
        for tr in econ_table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            row = []
            for cell in cells:
                # Extract text content, ignoring images
                cell_text = " ".join(cell.stripped_strings)
                row.append(cell_text)
            if row:
                rows.append(row)

        # Save economy data
        df = pd.DataFrame(rows)
        filename = f"{map_name.replace(' ', '_').lower()}_economy.csv"
        df.to_csv(filename, index=False, header=False)
        print(f"Economy data saved for {map_name}.")

    # Ensure a second table exists for round-by-round data
    if len(tables) > 1:
        round_table = tables[1]  # Second table is the round-by-round table
        rows = []
        for tr in round_table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            row = []
            for cell in cells:
                # Extract text content, ignoring images
                cell_text = " ".join(cell.stripped_strings)
                row.append(cell_text)
            if row:
                rows.append(row)

        # Save round-by-round data
        df = pd.DataFrame(rows)
        filename = f"{map_name.replace(' ', '_').lower()}_rounds.csv"
        df.to_csv(filename, index=False, header=False)
        print(f"Round-by-round data saved for {map_name}.")
    else:
        print(f"No round-by-round table found for {map_name}.")

print("Data fetching complete!")