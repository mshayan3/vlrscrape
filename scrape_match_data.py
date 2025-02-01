import csv
import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Define the match URL (Change this for a different match)
MATCH_URL = "https://www.vlr.gg/430843/trace-esports-vs-dragon-ranger-gaming-champions-tour-2025-china-kickoff-ubqf"

# CSV Filenames
MATCH_INFO_CSV = "match_info.csv"
MAPS_CSV = "maps.csv"
PLAYERS_CSV = "players.csv"
ROUNDS_CSV = "rounds.csv"


def save_to_csv(filename, data, headers):
    """Save extracted data to a CSV file."""
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)


def scrape_with_bs4():
    """Scrapes match data using requests + BeautifulSoup."""
    response = requests.get(MATCH_URL, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract Match Info
    event = soup.find("a", class_="match-header-event").text.strip()
    date = soup.find("div", class_="match-header-date").text.strip()
    team_1 = soup.find_all("div", class_="match-header-link-name")[0].text.strip()
    team_2 = soup.find_all("div", class_="match-header-link-name")[1].text.strip()
    score = soup.find("div", class_="match-header-vs-score").text.strip()
    match_info = [(event, date, team_1, team_2, score)]
    save_to_csv(MATCH_INFO_CSV, match_info, ["Event", "Date", "Team 1", "Team 2", "Score"])

    # Extract Maps Data
    maps = []
    for map_item in soup.find_all("div", class_="vm-stats-gamesnav-item"):
        map_name = map_item.text.strip()
        maps.append((event, map_name))
    save_to_csv(MAPS_CSV, maps, ["Event", "Map"])

    # Extract Players Data
    players = []
    for player_row in soup.select("table.wf-table-inset.mod-overview tbody tr"):
        cols = player_row.find_all("td")
        if len(cols) > 5:
            player_name = cols[0].text.strip()
            agent = cols[1].text.strip()
            rating = cols[2].text.strip()
            acs = cols[3].text.strip()
            kills = cols[4].text.strip()
            deaths = cols[5].text.strip()
            assists = cols[6].text.strip()
            players.append((event, player_name, agent, rating, acs, kills, deaths, assists))
    save_to_csv(PLAYERS_CSV, players, ["Event", "Player", "Agent", "Rating", "ACS", "Kills", "Deaths", "Assists"])

    # Extract Rounds Data
    rounds = []
    for round_row in soup.find_all("div", class_="vlr-rounds-row-col"):
        round_number = round_row.find("div", class_="rnd-num").text.strip()
        winner = "Win" if "mod-win" in round_row.get("class", []) else "Loss"
        rounds.append((event, round_number, winner))
    save_to_csv(ROUNDS_CSV, rounds, ["Event", "Round", "Result"])

    print("Match Data Scraped Successfully!")


def scrape_with_selenium():
    """Scrapes match data using Selenium for dynamically loaded content."""
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.get(MATCH_URL)
    time.sleep(5)  # Wait for JavaScript to load

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    # Extract Match Info (Same as BS4)
    event = soup.find("a", class_="match-header-event").text.strip()
    date = soup.find("div", class_="match-header-date").text.strip()
    team_1 = soup.find_all("div", class_="match-header-link-name")[0].text.strip()
    team_2 = soup.find_all("div", class_="match-header-link-name")[1].text.strip()
    score = soup.find("div", class_="match-header-vs-score").text.strip()
    match_info = [(event, date, team_1, team_2, score)]
    save_to_csv(MATCH_INFO_CSV, match_info, ["Event", "Date", "Team 1", "Team 2", "Score"])

    print("Match Data Scraped Successfully via Selenium!")


if __name__ == "__main__":
    choice = input("Choose scraping method: (1) BeautifulSoup (2) Selenium: ")
    if choice == "1":
        scrape_with_bs4()
    elif choice == "2":
        scrape_with_selenium()
    else:
        print("Invalid choice.")
