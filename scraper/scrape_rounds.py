import csv
import requests
from bs4 import BeautifulSoup


def fetch_round_data(match_url, filename='rounds_data.csv'):
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(match_url, headers=HEADERS)
    if response.status_code != 200:
        print("Failed to fetch the webpage.")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    maps = {map_item.get("data-game-id"): " ".join(map_item.text.strip().split())
            for map_item in soup.select('.vm-stats-gamesnav-item.js-map-switch')}

    rounds_data = []
    for map_id, map_name in maps.items():
        round_sections = soup.select(f'.vm-stats-game[data-game-id="{map_id}"] .vlr-rounds')

        for section in round_sections:
            teams_in_section = section.select('.team')
            team1_abbr, team2_abbr = (teams_in_section[0].text.strip(), teams_in_section[1].text.strip()) if len(
                teams_in_section) >= 2 else ("T1", "T2")

            for round_col in section.select('.vlr-rounds-row-col[title]'):
                round_num = round_col.select_one('.rnd-num').text.strip()
                round_result = round_col.get("title").strip()

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

    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Map", "Round Number", "Score", "Winning Team", "Winning Side", "Win Method"])
        writer.writerows(rounds_data)

    print(f"Round data saved to {filename}")


# Example usage
if __name__ == "__main__":
    MATCH_URL = "https://www.vlr.gg/428005/100-thieves-vs-nrg-esports-champions-tour-2025-americas-kickoff-lr1"
    fetch_round_data(MATCH_URL)
