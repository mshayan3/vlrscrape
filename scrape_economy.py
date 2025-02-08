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
    team_names = [team.get_text(strip=True) for team in soup.find_all("div", class_="team")]
    if len(team_names) < 2:
        print("Could not fetch team names.")
        exit()

    team_ct, team_t = team_names[0], team_names[1] 
    name_team_ct, name_team_t = team_names[0], team_names[1] 
    # Ensure a second table exists for round-by-round data
    if len(tables) > 1:
        round_table = tables[1]  # Second table is the round-by-round table
        rows = []
        round_counter = 1  # Track round number
        
        for tr in round_table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            for cell in cells:
                # Extract data points
                round_num = cell.find("div", class_="ge-text-light round-num")
                bank_values = cell.find_all("div", class_="bank")
                winning_team = cell.find("div", class_="rnd-sq mod-win mod-ct") or cell.find("div", class_="rnd-sq mod-win mod-t")
                losing_team = cell.find("div", class_="rnd-sq")

                # Get text values safely
                round_num = round_num.get_text(strip=True) if round_num else ""
                bank_1 = bank_values[0].get_text(strip=True) if len(bank_values) > 0 else ""
                bank_2 = bank_values[1].get_text(strip=True) if len(bank_values) > 1 else ""
                win_side = "CT" if "mod-ct" in str(winning_team) else "T" 
                lose_side = "CT" if "mod-ct" in str(losing_team) else "T"

                # Determine actual winning and losing teams
                # Append (CT) or (T) to team names
                if winning_team:
                    win_team = f"{team_ct} (CT)" if win_side == "CT" else f"{team_t} (T)"
                    lose_team = f"{team_t} (T)" if win_side == "CT" else f"{team_ct} (CT)"
                else:
                    win_team, lose_team = "", ""

                # Mark round 13 as a swap
                if round_num == "13":
                    round_num = "13 (Swap)"
                if round_num == "25":
                    round_num = "25 (OverTime)"


                # Handle side switching after 12 rounds
                if round_counter == 13:
                    team_ct, team_t = team_t, team_ct  # Swap sides
                elif round_counter > 24:  # Overtime: Switch every round
                    team_ct, team_t = team_t, team_ct  

                rows.append([round_num, bank_1, win_team, lose_team, bank_2])
                round_counter += 1  # Increment round count

        # Save round-by-round data
        # Convert to DataFrame and drop empty rows
        df = pd.DataFrame(rows, columns=["Round Number", f"Bank {name_team_ct}", "Winning Team", "Losing Team", f"Bank {name_team_t}"])

        # Drop empty rows (all values in a row must not be empty)
        df = df.dropna(how="all").replace("", pd.NA).dropna()

        # Save the cleaned file
        filename = f"{map_name.replace(' ', '_').lower()}_rounds.csv"
        df.to_csv(filename, index=False)
        print(f"Round-by-round data saved for {map_name}, empty rows removed.")

    else:
        print(f"No round-by-round table found for {map_name}.")

print("Data fetching complete!")