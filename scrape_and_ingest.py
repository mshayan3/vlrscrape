"""
scrape_and_ingest.py — full pipeline: scrape VLR.gg → CSVs → SQLite

Run from the project root. Scrapes a VLR.gg event (or single match),
saves raw CSVs to data/, then ingests into db/vlr_v2.db.

Usage:
    # Scrape + ingest a full event
    python scrape_and_ingest.py --event https://www.vlr.gg/event/2097/valorant-champions-2025

    # Scrape + ingest a single match
    python scrape_and_ingest.py --match https://www.vlr.gg/542272/nrg-vs-fnatic-valorant-champions-2025-gf

    # Skip scraping, just re-ingest existing CSVs
    python scrape_and_ingest.py --ingest-only

    # Custom DB path
    python scrape_and_ingest.py --event <url> --db db/custom.db

    # Skip specific data types during scraping
    python scrape_and_ingest.py --event <url> --skip economy performance
"""

import sys
import os
import argparse
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

ROOT     = Path(__file__).parent
SCRAPER  = ROOT / "scraper"
PIPELINE = ROOT / "pipeline"
DATA_DIR = ROOT / "data"
DB_PATH  = ROOT / "db" / "vlr_v2.db"


def run_scraper(url: str, is_event: bool, skip: list[str], data_dir: Path):
    """Run the appropriate scraper script."""
    sys.path.insert(0, str(SCRAPER))
    os.chdir(str(data_dir))        # scraper writes relative to cwd

    if is_event:
        log.info(f"Scraping event: {url}")
        from scrape_event import process_event
        process_event(url, skip_types=skip, check_existing=True)
    else:
        log.info(f"Scraping match: {url}")
        from main_scrape import process_match
        process_match(url, skip_types=skip, base_path=str(data_dir / "VCT Events"))

    os.chdir(str(ROOT))


def run_ingest(db_path: Path, event_filter: str = None, match_filter: str = None):
    """Run the ingestion pipeline."""
    log.info(f"Ingesting CSVs → {db_path}")
    sys.path.insert(0, str(PIPELINE))

    # Patch paths for the ingest module
    import importlib.util
    spec = importlib.util.spec_from_file_location("ingest_v2", PIPELINE / "ingest_v2.py")
    ingest = importlib.util.module_from_spec(spec)

    # Override path constants before executing
    import pipeline.ingest_v2 as ingest_mod
    ingest_mod.EVENTS_ROOT = DATA_DIR / "VCT Events"
    ingest_mod.DEFAULT_DB  = db_path

    conn = ingest_mod.open_db(db_path)
    norm = ingest_mod.Normaliser(conn)

    year_dirs = sorted((DATA_DIR / "VCT Events").iterdir())
    for year_dir in year_dirs:
        if not year_dir.is_dir():
            continue
        for event_dir in sorted(year_dir.iterdir()):
            if not event_dir.is_dir():
                continue
            if event_filter and event_filter.lower() not in event_dir.name.lower():
                continue
            if match_filter:
                event_info = ingest_mod.parse_event_folder(event_dir)
                ingest_mod.upsert_event(conn, norm, event_info)
                for stage_dir in sorted(event_dir.iterdir()):
                    if not stage_dir.is_dir():
                        continue
                    for match_dir in sorted(stage_dir.iterdir()):
                        if not match_dir.is_dir():
                            continue
                        if match_filter.lower() in match_dir.name.lower():
                            ingest_mod.ingest_match(
                                conn, norm, match_dir,
                                event_info["event_id"],
                                stage_dir.name.replace("_", " ")
                            )
            else:
                ingest_mod.ingest_event(conn, norm, event_dir)

    conn.close()


def print_summary(db_path: Path):
    import sqlite3
    conn = sqlite3.connect(db_path)
    print("\n" + "=" * 52)
    print("  DATABASE SUMMARY")
    print("=" * 52)
    tables = [
        ("events", "Events"),
        ("teams", "Teams"),
        ("players", "Players"),
        ("agents", "Agents"),
        ("matches", "Matches"),
        ("maps", "Maps"),
        ("rounds", "Rounds"),
        ("player_map_stats", "Player map stats"),
        ("player_map_advanced", "Advanced stats rows"),
        ("player_vs_player_kills", "Kill matrix entries"),
        ("map_economy_summary", "Economy summary rows"),
        ("round_economy", "Round economy rows"),
        ("player_map_agents", "Agent assignments"),
    ]
    for tbl, label in tables:
        n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {label:<26} {n:>6,}")

    print("\n  Matches by stage:")
    for r in conn.execute("""
        SELECT stage, COUNT(DISTINCT match_id) n
        FROM matches GROUP BY stage ORDER BY stage
    """):
        print(f"    {r[0]:<20} {r[1]:>3} matches")
    print("=" * 52)
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape VLR.gg and ingest into SQLite (full pipeline)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--event", metavar="URL",
                       help="VLR.gg event URL to scrape all matches from")
    group.add_argument("--match", metavar="URL",
                       help="VLR.gg single match URL to scrape")
    group.add_argument("--ingest-only", action="store_true",
                       help="Skip scraping, just re-ingest existing CSVs in data/")

    parser.add_argument("--db", default=str(DB_PATH),
                        help=f"SQLite DB path (default: {DB_PATH})")
    parser.add_argument("--skip", nargs="+",
                        choices=["veto", "stats", "rounds", "economy", "performance"],
                        default=[],
                        help="Skip specific scraping sections")
    parser.add_argument("--summary", action="store_true", default=True,
                        help="Print DB row counts after ingestion (default: on)")
    args = parser.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Scrape ──────────────────────────────────────────
    if not args.ingest_only:
        if not args.event and not args.match:
            parser.error("Provide --event <url>, --match <url>, or --ingest-only")

        is_event  = bool(args.event)
        scrape_url = args.event or args.match

        try:
            run_scraper(scrape_url, is_event, args.skip, DATA_DIR)
        except Exception as e:
            log.error(f"Scraping failed: {e}", exc_info=True)
            sys.exit(1)

    # ── Step 2: Ingest ──────────────────────────────────────────
    vct_events = DATA_DIR / "VCT Events"
    if not vct_events.exists() or not any(vct_events.iterdir()):
        log.error("No CSV data found in data/VCT Events/. Run scraping first.")
        sys.exit(1)

    try:
        # Derive filters from URL if provided
        event_f = None
        match_f = None
        if args.event:
            # e.g. "valorant-champions-2025" → loose filter on folder name
            event_f = args.event.rstrip("/").split("/")[-1].replace("-", "_")
        if args.match:
            match_f = args.match.rstrip("/").split("/")[-1]

        run_ingest(db_path, event_filter=event_f, match_filter=match_f)
    except Exception as e:
        log.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)

    # ── Step 3: Summary ─────────────────────────────────────────
    if args.summary:
        print_summary(db_path)


if __name__ == "__main__":
    main()
