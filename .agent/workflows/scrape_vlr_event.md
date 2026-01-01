---
description: How to scrape a complete VLR.gg event
---

This workflow guides you through scraping all match data for a specific VLR.gg event.

1. **Locate the Event Matches Page**
   - Go to [VLR.gg](https://www.vlr.gg/events).
   - Click on the event you want to scrape.
   - Click on the **Matches** tab (usually matches the pattern `.../event/matches/...`).
   - Copy the full URL from your browser address bar.

2. **Run the Event Scraper**
   - Open your terminal in the project directory.
   - Run the following command (replace `<EVENT_URL>` with the URL you copied):
   ```bash
   py scrape_event.py "<EVENT_URL>"
   ```
   *Example:*
   ```bash
   py scrape_event.py "https://www.vlr.gg/event/matches/2000/champions-tour"
   ```

3. **Monitor Progress**
   - The script will output the number of matches found.
   - It will proceed to scrape each match one by one.
   - **Note**: This process can take some time depending on the number of matches (approx. 5-10 seconds per match).

4. **Verify Output**
   - Once completed, look for a new folder in your project directory with the name of the event (e.g., `Red_Bull_Home_Ground_2025`).
   - Inside, you will find a folder for each match containing CSV files for:
     - `map_veto`
     - `player_stats`
     - `rounds`
     - `economy`
     - `performance`
