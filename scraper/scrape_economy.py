import requests
import pandas as pd
from bs4 import BeautifulSoup


def fetch_soup(url, headers):
    response = requests.get(url, headers=headers)
    return BeautifulSoup(response.text, "html.parser")


def scrape_economy_data(base_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }

    soup = fetch_soup(base_url + "?game=all&tab=economy", headers)

    # Find all map tabs
    map_tabs = soup.find_all("div", class_="vm-stats-gamesnav-item js-map-switch")
    map_ids = {tab.get_text(strip=True): tab.get("data-game-id", "all") for tab in map_tabs}

    # Ensure "All Maps" is included manually
    if "All Maps" not in map_ids:
        map_ids["All Maps"] = "all"

    econ_table_class = "wf-table-inset mod-econ"

    for map_name, map_id in map_ids.items():
        print(f"Processing economy data for: {map_name}")

        map_soup = fetch_soup(base_url + f"?game={map_id}&tab=economy", headers)
        map_container = map_soup.find("div", class_="vm-stats-game", attrs={"data-game-id": map_id})
        if not map_container:
            print(f"No economy data found for {map_name}")
            continue

        tables = map_container.find_all("table", class_=econ_table_class)

        if len(tables) > 0:
            save_table_data(tables[0], f"{map_name.replace(' ', '_').lower()}_economy.csv")

        if len(tables) > 1:
            save_table_data(tables[1], f"{map_name.replace(' ', '_').lower()}_rounds_economy.csv")
        else:
            print(f"No round-by-round table found for {map_name}.")

    print("Data fetching complete!")


def save_table_data(table, filename):
    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        row = [" ".join(cell.stripped_strings) for cell in cells]
        if row:
            rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False, header=False)
    print(f"Data saved to {filename}")


# Example Usage
base_url = "https://www.vlr.gg/428005/100-thieves-vs-nrg-esports-champions-tour-2025-americas-kickoff-lr1/"
scrape_economy_data(base_url)