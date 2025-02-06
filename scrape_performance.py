import requests
import pandas as pd
from bs4 import BeautifulSoup

# Define the URL with the performance tab extension
url = "https://www.vlr.gg/428005/100-thieves-vs-nrg-esports-champions-tour-2025-americas-kickoff-lr1/?game=all&tab=performance"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}

# Fetch and parse the HTML
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

# Find all map tabs
map_tabs = soup.find_all("div", class_="vm-stats-gamesnav-item js-map-switch")
map_ids = {tab.get_text(strip=True): tab.get("data-game-id", "all") for tab in map_tabs}

# Ensure "All Maps" is included manually
if "All Maps" not in map_ids:
    map_ids["All Maps"] = "all"

# Define interaction categories and their corresponding table class names
interaction_categories = {
    "All Kills": "wf-table-inset mod-matrix mod-normal",
    "First Kills": "wf-table-inset mod-matrix mod-fkfd",
    "Op Kills": "wf-table-inset mod-matrix mod-op"
}

# Loop through each map (including "All Maps")
for map_name, map_id in map_ids.items():
    print(f"Processing data for: {map_name}")

    # Find the corresponding map container
    map_container = soup.find("div", class_="vm-stats-game", attrs={"data-game-id": map_id})
    if not map_container:
        print(f"No data found for {map_name}")
        continue

    # Extracting interaction tables
    interaction_tables = {
        category: map_container.find("table", class_=class_name)
        for category, class_name in interaction_categories.items()
    }

    for category, table in interaction_tables.items():
        if table:
            rows = []
            for tr in table.find_all("tr"):
                cells = tr.find_all(["td", "th"])
                row = [" ".join(cell.stripped_strings) for cell in cells]
                if row:
                    rows.append(row)

            # Save as CSV
            df = pd.DataFrame(rows)
            filename = f"{map_name.replace(' ', '_').lower()}_{category.replace(' ', '_').lower()}.csv"
            df.to_csv(filename, index=False, header=False)
            print(f"{category} data saved for {map_name}.")
        else:
            print(f"No {category} table found for {map_name}.")

    # Extract advanced stats table
    adv_stats_table = map_container.find("table", class_="wf-table-inset mod-adv-stats")
    if adv_stats_table:
        rows = []
        for tr in adv_stats_table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            row = [" ".join(cell.stripped_strings) for cell in cells]
            if row:
                rows.append(row)
        df = pd.DataFrame(rows)
        filename = f"{map_name.replace(' ', '_').lower()}_advanced_stats.csv"
        df.to_csv(filename, index=False, header=False)
        print(f"Advanced stats data saved for {map_name}.")
    else:
        print(f"No advanced stats table found for {map_name}.")
