import os
import re
import csv
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from utils import fetch_url


def create_folder_structure(match_url, base_path=None):
    """Creates a clean folder structure based on match details from URL."""
    # First, get the match title
    # headers are now handled by fetch_url
    
    response = fetch_url(match_url)
    if not response or response.status_code != 200:
        print("Failed to fetch the webpage.")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Try to extract match title
    match_header = soup.select_one('.match-header-vs')
    folder_name = ""
    
    if match_header:
        team_names = [team.text.strip() for team in match_header.select('.wf-title-med')]
        if len(team_names) >= 2:
            team1, team2 = team_names[0], team_names[1]

            # Get event name
            event_element = soup.select_one('.match-header-event-series')
            event_name = event_element.text.strip() if event_element else "Unknown_Event"

            # Clean and format folder name
            event_name = re.sub(r'[\\/*?:"<>|]', '-', event_name)
            folder_name = f"{team1}_vs_{team2}_{event_name}"
            folder_name = re.sub(r'\s+', '_', folder_name)
            folder_name = re.sub(r'_{2,}', '_', folder_name)
    
    if not folder_name:
        # Fallback if we can't extract a proper name
        match_id = os.path.basename(urlparse(match_url).path)
        folder_name = f"match_{match_id}"

    # Construct full path
    if base_path:
        full_path = os.path.join(base_path, folder_name)
    else:
        full_path = folder_name

    # Create main folder and subfolders
    if not os.path.exists(full_path):
        os.makedirs(full_path)

    subfolders = ['map_veto', 'player_stats', 'rounds', 'economy', 'performance']
    for subfolder in subfolders:
        subfolder_path = os.path.join(full_path, subfolder)
        if not os.path.exists(subfolder_path):
            os.makedirs(subfolder_path)

    return full_path


def clean_filename(name):
    """Clean a string to make it a valid filename."""
    # First, remove any non-printable characters and strip whitespace
    name = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()

    # Replace problematic characters with underscores
    name = re.sub(r'[\\/*?:"<>|\t\n\r]', '_', name)

    # Replace multiple underscores with a single one
    name = re.sub(r'_{2,}', '_', name)

    # Limit filename length
    if len(name) > 100:
        name = name[:100]

    return name


def fetch_map_veto(url, output_folder):
    """Scrapes map veto information and saves to CSV."""
    # Headers handled by fetch_url

    response = fetch_url(url)
    if not response or response.status_code != 200:
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

    output_file = os.path.join(output_folder, 'map_veto', 'map_veto.csv')
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["map", "pick", "ban"])
        for map_name, details in map_data.items():
            writer.writerow([map_name, details["pick"], details["ban"]])

    print(f"Map veto data saved to {output_file}")


def fetch_player_stats(match_url, output_folder):
    """Scrapes player statistics and saves to CSV."""
    # Headers handled by fetch_url

    response = fetch_url(match_url)
    if not response or response.status_code != 200:
        print("Failed to fetch the webpage.")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract maps played and their respective numbers
    maps = {}
    for idx, map_item in enumerate(soup.select('.vm-stats-gamesnav-item.js-map-switch')):
        # Clean the map name properly to avoid invalid filenames
        map_name = clean_filename(map_item.text.strip())
        if not map_name:  # Skip if map name is empty after cleaning
            continue

        game_id = map_item.get("data-game-id")
        if game_id:
            map_label = f"Map_{idx}_{map_name}" if idx else "All_Maps"
            maps[game_id] = map_label

    # Define side classes and labels
    side_classes = {
        "All": ".mod-stat .side.mod-both",
        "Attack": ".mod-stat .side.mod-t",
        "Defend": ".mod-stat .side.mod-ct"
    }

    for map_id, map_name in maps.items():
        output_file = os.path.join(output_folder, 'player_stats', f"{map_name}.csv")

        rows = []
        for side_label, side_class in side_classes.items():
            for row in soup.select(f'.vm-stats-game[data-game-id="{map_id}"] tbody tr'):
                player_name = row.select_one('.text-of').text.strip() if row.select_one('.text-of') else "Unknown"
                team_name = row.select_one('.ge-text-light').text.strip() if row.select_one(
                    '.ge-text-light') else "Unknown"
                agents = ', '.join(img['title'] for img in row.select('.mod-agent img') if 'title' in img.attrs)
                stats = [span.text.strip() for span in row.select(side_class)]

                if not stats:
                    continue

                row_data = [player_name, team_name, map_name, side_label, agents] + stats
                rows.append(row_data)

        # Save data to CSV
        if rows:
            with open(output_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(
                    ['Player', 'Team', 'Map', 'Side', 'Agents', 'R2.0', 'ACS', 'K', 'D', 'A', 'K/D', 'KAST', 'ADR',
                     'HS%', 'FK', 'FD', 'FK/FD'])
                writer.writerows(rows)

            print(f"Player stats saved to {output_file}")


def fetch_round_data(match_url, output_folder):
    """Scrapes round-by-round data and saves to CSV."""
    # Headers handled by fetch_url

    response = fetch_url(match_url)
    if not response or response.status_code != 200:
        print("Failed to fetch the webpage.")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    maps = {}
    for map_item in soup.select('.vm-stats-gamesnav-item.js-map-switch'):
        map_name = clean_filename(map_item.text.strip())
        if not map_name:  # Skip if map name is empty after cleaning
            continue

        game_id = map_item.get("data-game-id")
        if game_id:
            maps[game_id] = map_name

    for map_id, map_name in maps.items():
        output_file = os.path.join(output_folder, 'rounds', f"{map_name}_rounds.csv")

        rounds_data = []
        round_sections = soup.select(f'.vm-stats-game[data-game-id="{map_id}"] .vlr-rounds')

        for section in round_sections:
            teams_in_section = section.select('.team')
            team1_abbr, team2_abbr = (teams_in_section[0].text.strip(), teams_in_section[1].text.strip()) if len(
                teams_in_section) >= 2 else ("T1", "T2")

            for round_col in section.select('.vlr-rounds-row-col[title]'):
                round_num = round_col.select_one('.rnd-num').text.strip() if round_col.select_one(
                    '.rnd-num') else "Unknown"
                round_result = round_col.get("title", "").strip()

                winner, winner_side, method = None, None, None
                win_divs = round_col.select('.rnd-sq.mod-win')
                if win_divs:
                    winner, winner_side = (team1_abbr, "Defenders") if "mod-ct" in win_divs[0].get("class", []) else (
                        team2_abbr, "Attackers")

                img_tag = round_col.select_one('.rnd-sq img')
                if img_tag:
                    img_src = img_tag.get("src", "")
                    method = ("Elimination" if "elim.webp" in img_src else
                              "Spike Detonation" if "boom.webp" in img_src else
                              "Defuse" if "defuse.webp" in img_src else
                              "Time Expired" if "time.webp" in img_src else None)

                rounds_data.append([map_name, round_num, f'"{round_result}"', winner, winner_side, method])

        if rounds_data:
            with open(output_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["Map", "Round Number", "Score", "Winning Team", "Winning Side", "Win Method"])
                writer.writerows(rounds_data)

            print(f"Round data saved to {output_file}")


def fetch_economy_data(match_url, output_folder):
    """Scrapes economy data and saves to CSV with proper formatting."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }

    if not match_url.endswith('/'):
        match_url += '/'

    soup = fetch_soup(match_url + "?game=all&tab=economy", headers)

    map_tabs = soup.find_all("div", class_="vm-stats-gamesnav-item js-map-switch")
    map_ids = {clean_filename(tab.get_text(strip=True)): tab.get("data-game-id", "all") for tab in map_tabs}

    if "All_Maps" not in map_ids:
        map_ids["All_Maps"] = "all"

    econ_table_class = "wf-table-inset mod-econ"
    os.makedirs(os.path.join(output_folder, 'economy'), exist_ok=True)

    for map_name, map_id in map_ids.items():
        print(f"Processing economy data for: {map_name}")

        map_soup = fetch_soup(match_url + f"?game={map_id}&tab=economy", headers)
        map_container = map_soup.find("div", class_="vm-stats-game", attrs={"data-game-id": map_id})
        if not map_container:
            print(f"No economy data found for {map_name}")
            continue

        tables = map_container.find_all("table", class_=econ_table_class)

        if len(tables) > 0:
            output_file = os.path.join(output_folder, 'economy', f"{map_name}_economy.csv")
            save_table_data(tables[0], output_file)

        if len(tables) > 1:
            output_file = os.path.join(output_folder, 'economy', f"{map_name}_rounds_economy.csv")
            save_table_data(tables[1], output_file)
        else:
            print(f"No round-by-round economy table found for {map_name}.")

    print("Economy data fetching complete!")


def fetch_performance_data(match_url, output_folder):
    """Scrapes performance data and saves to CSV."""
    # Make sure URL ends with a slash
    if not match_url.endswith('/'):
        match_url += '/'

    extension = "?game=all&tab=performance"
    url = match_url + extension
    # Headers handled by fetch_url

    response = fetch_url(url)
    soup = BeautifulSoup(response.text, "html.parser")

    map_tabs = soup.find_all("div", class_="vm-stats-gamesnav-item js-map-switch")
    map_ids = {}
    for tab in map_tabs:
        map_name = clean_filename(tab.get_text(strip=True))
        game_id = tab.get("data-game-id", "all")
        if map_name:  # Only add if map name is not empty after cleaning
            map_ids[map_name] = game_id

    if "All_Maps" not in map_ids:
        map_ids["All_Maps"] = "all"

    interaction_categories = {
        "All_Kills": "wf-table-inset mod-matrix mod-normal",
        "First_Kills": "wf-table-inset mod-matrix mod-fkfd",
        "Op_Kills": "wf-table-inset mod-matrix mod-op"
    }

    for map_name, map_id in map_ids.items():
        print(f"Processing performance data for: {map_name}")

        map_container = soup.find("div", class_="vm-stats-game", attrs={"data-game-id": map_id})
        if not map_container:
            print(f"No performance data found for {map_name}")
            continue

        for category, class_name in interaction_categories.items():
            table = map_container.find("table", class_=class_name)
            if table:
                output_file = os.path.join(output_folder, 'performance', f"{map_name}_{category}.csv")
                save_table_data(table, output_file)
            else:
                print(f"No {category} table found for {map_name}.")

        adv_stats_table = map_container.find("table", class_="wf-table-inset mod-adv-stats")
        if adv_stats_table:
            output_file = os.path.join(output_folder, 'performance', f"{map_name}_advanced_stats.csv")
            save_table_data(adv_stats_table, output_file)
        else:
            print(f"No advanced stats table found for {map_name}.")


def fetch_soup(url, headers=None):
    """Fetch the HTML content and return BeautifulSoup object."""
    response = fetch_url(url)
    if response:
        return BeautifulSoup(response.text, "html.parser")
    return None


def save_table_data(table, filename):
    """Save table data to CSV file."""
    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        row = [" ".join(cell.stripped_strings) for cell in cells]
        if row:
            rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False, header=False)
    print(f"Data saved to {filename}")


def process_match(match_url, skip_types=None, base_path=None, check_existing=False):
    """Main function to process a single match"""
    if skip_types is None:
        skip_types = []

    # Create folder structure
    output_folder = create_folder_structure(match_url, base_path)
    if not output_folder:
        print(f"Failed to create folder structure for {match_url}")
        return

    if check_existing:
        # Check if a critical file exists, e.g., All_Maps.csv in player_stats
        common_file = os.path.join(output_folder, 'player_stats', 'All_Maps.csv')
        # Also check for veto or just rely on stats? Stats is most important.
        if os.path.exists(common_file):
            print(f"Skipping match (Data exists): {output_folder}")
            return

    print(f"Saving all data to: {output_folder}")

    # Fetch each type of data
    if 'veto' not in skip_types:
        print("\n===== Fetching Map Veto Data =====")
        fetch_map_veto(match_url, output_folder)

    if 'stats' not in skip_types:
        print("\n===== Fetching Player Stats =====")
        fetch_player_stats(match_url, output_folder)

    if 'rounds' not in skip_types:
        print("\n===== Fetching Round Data =====")
        fetch_round_data(match_url, output_folder)

    if 'economy' not in skip_types:
        print("\n===== Fetching Economy Data =====")
        fetch_economy_data(match_url, output_folder)

    if 'performance' not in skip_types:
        print("\n===== Fetching Performance Data =====")
        fetch_performance_data(match_url, output_folder)

    print("\nAll requested data has been scraped successfully!")


def main():
    parser = argparse.ArgumentParser(description='Scrape VLR.gg match data.')
    parser.add_argument('url', help='URL of the VLR.gg match to scrape')
    parser.add_argument('--skip', nargs='+', choices=['veto', 'stats', 'rounds', 'economy', 'performance'],
                        help='Skip specific data types')

    args = parser.parse_args()
    process_match(args.url, args.skip)


if __name__ == "__main__":
    main()