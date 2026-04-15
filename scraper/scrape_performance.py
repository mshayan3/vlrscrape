import requests
import pandas as pd
from bs4 import BeautifulSoup


def fetch_performance_data(match_url):
    extension = "?game=all&tab=performance"
    url = match_url + extension
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    map_tabs = soup.find_all("div", class_="vm-stats-gamesnav-item js-map-switch")
    map_ids = {tab.get_text(strip=True): tab.get("data-game-id", "all") for tab in map_tabs}
    if "All Maps" not in map_ids:
        map_ids["All Maps"] = "all"

    interaction_categories = {
        "All Kills": "wf-table-inset mod-matrix mod-normal",
        "First Kills": "wf-table-inset mod-matrix mod-fkfd",
        "Op Kills": "wf-table-inset mod-matrix mod-op"
    }

    for map_name, map_id in map_ids.items():
        print(f"Processing data for: {map_name}")

        map_container = soup.find("div", class_="vm-stats-game", attrs={"data-game-id": map_id})
        if not map_container:
            print(f"No data found for {map_name}")
            continue

        for category, class_name in interaction_categories.items():
            table = map_container.find("table", class_=class_name)
            if table:
                save_table_to_csv(table, map_name, category)
            else:
                print(f"No {category} table found for {map_name}.")

        adv_stats_table = map_container.find("table", class_="wf-table-inset mod-adv-stats")
        if adv_stats_table:
            save_table_to_csv(adv_stats_table, map_name, "Advanced Stats")
        else:
            print(f"No advanced stats table found for {map_name}.")


def save_table_to_csv(table, map_name, category):
    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        row = [" ".join(cell.stripped_strings) for cell in cells]
        if row:
            rows.append(row)

    df = pd.DataFrame(rows)
    filename = f"{map_name.replace(' ', '_').lower()}_{category.replace(' ', '_').lower()}.csv"
    df.to_csv(filename, index=False, header=False)
    print(f"{category} data saved for {map_name}.")

# Example usage:
fetch_performance_data("https://www.vlr.gg/428005/100-thieves-vs-nrg-esports-champions-tour-2025-americas-kickoff-lr1/")
