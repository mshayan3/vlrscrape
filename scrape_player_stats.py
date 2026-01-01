import csv
import requests
from bs4 import BeautifulSoup


def fetch_html(url):
    """Fetch the HTML content of a given URL."""
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=HEADERS)
    return response.text if response.status_code == 200 else None


def scrape_player_stats(match_url, output_file='player_stats.csv'):
    """Scrapes player statistics from a VLR.gg match page and saves them to a CSV file."""
    html = fetch_html(match_url)
    if not html:
        print("Failed to fetch the webpage.")
        return

    soup = BeautifulSoup(html, 'html.parser')

    # Extract maps played and their respective numbers
    maps = {}
    for idx, map_item in enumerate(soup.select('.vm-stats-gamesnav-item.js-map-switch')):
        map_name = map_item.text.strip()
        game_id = map_item.get("data-game-id")
        if game_id:
            maps[game_id] = f"Map {idx} - {map_name}" if idx else "All Maps"

    # Define side classes and labels
    side_classes = {
        "All": ".mod-stat .side.mod-both",
        "Attack": ".mod-stat .side.mod-t",
        "Defend": ".mod-stat .side.mod-ct"
    }

    rows = []
    for map_id, map_name in maps.items():
        for side_label, side_class in side_classes.items():
            for row in soup.select(f'.vm-stats-game[data-game-id="{map_id}"] tbody tr'):
                player_name = row.select_one('.text-of').text.strip()
                team_name = row.select_one('.ge-text-light').text.strip()
                agents = ', '.join(img['title'] for img in row.select('.mod-agent img'))
                stats = [span.text.strip() for span in row.select(side_class)]

                if not stats:
                    continue

                row_data = [player_name, team_name, map_name, side_label, agents] + stats
                rows.append(row_data)

    # Save data to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Player', 'Team', 'Map', 'Side', 'Agents', 'R2.0', 'ACS', 'K', 'D', 'A', 'K/D', 'KAST', 'ADR',
                         'HS%', 'FK', 'FD', 'FK/FD'])
        writer.writerows(rows)

    print(f"Data saved to {output_file}")

# Example usage
scrape_player_stats("https://www.vlr.gg/428005/100-thieves-vs-nrg-esports-champions-tour-2025-americas-kickoff-lr")
