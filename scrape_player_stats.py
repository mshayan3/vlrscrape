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

# Extract maps played and their respective numbers
maps = {}
for idx, map_item in enumerate(soup.select('.vm-stats-gamesnav-item.js-map-switch')):
    map_name = map_item.text.strip()
    game_id = map_item.get("data-game-id")
    if game_id:
        maps[game_id] = f"Map {idx} - {map_name}" if idx else "All Maps"

# Extract table headers
headers = [th.text.strip() for th in soup.select('thead th')]

# Extract table rows for each map
rows = []
for map_id, map_name in maps.items():
    for row in soup.select(f'.vm-stats-game[data-game-id="{map_id}"] tbody tr'):
        # Extract player name
        player_name = row.select_one('.text-of').text.strip()

        # Extract player team (found in the smaller text below player name)
        team_name = row.select_one('.ge-text-light').text.strip()

        # Extract agent(s)
        agents = [img['title'] for img in row.select('.mod-agent img')]
        agents_str = ', '.join(agents)

        # Extract stats
        stats = [span.text.strip() for span in row.select('.mod-stat .side.mod-both')]

        # Combine all data for the row
        row_data = [player_name, team_name, map_name, agents_str] + stats
        rows.append(row_data)

# Save to CSV
with open('table_data.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Player', 'Team', 'Map', 'Agents'] + headers[2:])  # Write headers
    writer.writerows(rows)  # Write rows

print("Data saved to table_data.csv")
