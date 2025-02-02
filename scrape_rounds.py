import csv
import requests
from bs4 import BeautifulSoup

# Match URL
MATCH_URL = "https://www.vlr.gg/428005/100-thieves-vs-nrg-esports-champions-tour-2025-americas-kickoff-lr1"

# Headers to mimic a real browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def fetch_html(url):
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.text
    else:
        print("Failed to fetch the webpage.")
        return None

# Parse the HTML
soup = BeautifulSoup(fetch_html(MATCH_URL), 'html.parser')

# Extract teams from the match header
teams = [team.text.strip() for team in soup.select('.match-header-link-name div.wf-title-med')]
team1, team2 = teams if len(teams) == 2 else ("Unknown Team 1", "Unknown Team 2")

# Extract maps played
maps = {}
for idx, map_item in enumerate(soup.select('.vm-stats-gamesnav-item.js-map-switch')):
    map_name = " ".join(map_item.text.strip().split())  # Clean whitespace issues
    game_id = map_item.get("data-game-id")
    if game_id:
        maps[game_id] = f"Map {idx} - {map_name}" if idx else "All Maps"

# Extract round data
rounds_data = []
for map_id, map_name in maps.items():
    round_sections = soup.select(f'.vm-stats-game[data-game-id="{map_id}"] .vlr-rounds')

    for section in round_sections:
        # Extract teams playing this section
        teams_in_section = section.select('.team')
        if len(teams_in_section) >= 2:
            team1_abbr = teams_in_section[0].text.strip()
            team2_abbr = teams_in_section[1].text.strip()
        else:
            team1_abbr, team2_abbr = "T1", "T2"

        for round_col in section.select('.vlr-rounds-row-col[title]'):
            round_num = round_col.select_one('.rnd-num').text.strip()
            round_result = round_col.get("title").strip()  # Example: "1-0"

            # Determine which side won
            winner = None
            winner_side = None
            method = None

            win_divs = round_col.select('.rnd-sq.mod-win')
            if win_divs:
                if "mod-ct" in win_divs[0].get("class", []):
                    winner = team1_abbr
                    winner_side = "Defenders"
                elif "mod-t" in win_divs[0].get("class", []):
                    winner = team2_abbr
                    winner_side = "Attackers"

            # Determine win method based on image
            img_tag = round_col.select_one('.rnd-sq img')
            if img_tag:
                img_src = img_tag.get("src", "")
                if "elim.webp" in img_src:
                    method = "Elimination"
                elif "boom.webp" in img_src:
                    method = "Spike Detonation"
                elif "defuse.webp" in img_src:
                    method = "Defuse"
                elif "time.webp" in img_src:
                    method = "Time Expired"

            # Clean map name
            cleaned_map_name = " ".join(map_name.split())

            # Save the round data
            rounds_data.append([cleaned_map_name, round_num, f'"{round_result}"', winner, winner_side, method])
# Save to CSV
with open('rounds_data.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["Map", "Round Number", "Score", "Winning Team", "Winning Side", "Win Method"])
    writer.writerows(rounds_data)

print("Round data saved to rounds_data.csv")
