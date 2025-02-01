import requests
from bs4 import BeautifulSoup

# URL of the match page
url = "https://www.vlr.gg/429384/team-vitality-vs-karmine-corp-champions-tour-2025-emea-kickoff-ubqf/?game=196082&tab=overview"

# Fetch the webpage content
headers = {"User-Agent": "Mozilla/5.0"}  # Mimic a real browser
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

# Extract match details
match_info = {}
match_header = soup.find("div", class_="match-header")
if match_header:
    teams = match_header.find_all("div", class_="match-header-link-name")
    if teams and len(teams) == 2:
        match_info["team_1"] = teams[0].text.strip()
        match_info["team_2"] = teams[1].text.strip()

    score = match_header.find("div", class_="match-header-vs-score")
    if score:
        match_info["score"] = score.text.strip()

    date = match_header.find("div", class_="match-header-date")
    if date:
        match_info["date"] = date.text.strip()

# Extract map statistics
maps_info = []
maps_section = soup.find_all("div", class_="vm-stats-game-header")
for game in maps_section:
    map_details = {}
    teams = game.find_all("div", class_="team")
    if len(teams) == 2:
        map_details["team_1"] = teams[0].find("div", class_="team-name").text.strip()
        map_details["team_1_score"] = teams[0].find("div", class_="score").text.strip()
        map_details["team_2"] = teams[1].find("div", class_="team-name").text.strip()
        map_details["team_2_score"] = teams[1].find("div", class_="score").text.strip()

    map_name = game.find("div", class_="map")
    if map_name:
        map_details["map"] = map_name.find("span").text.strip()

    maps_info.append(map_details)

# Extract player statistics
player_stats = []
stats_table = soup.find("table", class_="wf-table-inset mod-overview")
if stats_table:
    rows = stats_table.find("tbody").find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if cells:
            player_data = {
                "agent": cells[1].find("img")["title"] if cells[1].find("img") else "N/A",
                "r2": cells[2].text.strip(),
                "acs": cells[3].text.strip(),
                "kills": cells[4].text.strip(),
                "deaths": cells[5].text.strip(),
                "assists": cells[6].text.strip(),
                "+/-": cells[7].text.strip(),
                "kast": cells[8].text.strip(),
                "adr": cells[9].text.strip(),
                "hs_percent": cells[10].text.strip(),
                "fk": cells[11].text.strip(),
                "fd": cells[12].text.strip(),
            }
            player_stats.append(player_data)

# Print extracted data
print("Match Info:", match_info)
print("\nMaps Info:")
for map_data in maps_info:
    print(map_data)
print("\nPlayer Stats:")
for player in player_stats:
    print(player)
