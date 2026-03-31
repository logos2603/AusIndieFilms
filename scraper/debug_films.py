"""
Quick debug script — tests specific films through the scraper pipeline
without running the full festival scrape.

Usage:
    TMDB_API_KEY=your_key python scraper/debug_films.py
"""

import os
import sys
import logging

# Add scraper directory to path so we can import from scrape.py
sys.path.insert(0, os.path.dirname(__file__))

# Import everything from the main scraper
from scrape import (
    tmdb_search_film,
    verify_australian_on_wikipedia,
    verify_not_rerelease,
    is_feature_film,
    BLOCKLIST_TMDB_IDS,
    AUSTRALIAN_COMPANIES,
)

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Films to test ─────────────────────────────────────────────────────────────
# Add any film you want to debug here: (title, year, festival)
TEST_FILMS = [
    ("Audrey",    2024, "SXSW"),
    ("Monolith",  2023, "SXSW"),
    ("Nitram",    2021, "Cannes"),       # known good — should pass
    ("Housekeeping for Beginners", 2023, "Venice"),  # known bad — should fail
    ("Birdeater", 2024, "SXSW"),
]


def test_film(title: str, year: int, festival: str):
    print(f"\n{'='*60}")
    print(f"Testing: {title} ({year}) — {festival}")
    print('='*60)

    # Step 1: TMDB pre-filter
    print("\n[Step 1] TMDB pre-filter...")
    detail = tmdb_search_film(title, year)
    if not detail:
        print(f"  ✗ FAILED: No TMDB result found")
        return

    tmdb_id    = detail.get("id")
    tmdb_title = detail.get("title")
    prod_countries  = [c.get("iso_3166_1","") for c in detail.get("production_countries", [])]
    origin_countries = detail.get("origin_country", [])
    prod_companies  = [c.get("name") for c in detail.get("production_companies", [])]

    print(f"  TMDB match:     '{tmdb_title}' (id={tmdb_id})")
    print(f"  prod_countries: {prod_countries}")
    print(f"  origin_country: {origin_countries}")
    print(f"  companies:      {prod_companies}")

    au_by_country = "AU" in prod_countries or "AU" in origin_countries
    au_by_company = any(
        aus_co in co.lower()
        for co in prod_companies
        for aus_co in AUSTRALIAN_COMPANIES
    )
    print(f"  AU by country:  {au_by_country}")
    print(f"  AU by company:  {au_by_company}")

    if tmdb_id in BLOCKLIST_TMDB_IDS:
        print(f"  ✗ FAILED: Blocklisted (id={tmdb_id})")
        return

    if not au_by_country and not au_by_company:
        print(f"  ✗ FAILED: No Australian signal in TMDB — would be rejected here")
        return

    print(f"  ✓ PASSED TMDB pre-filter")

    # Step 2: Wikipedia nationality
    print(f"\n[Step 2] Wikipedia nationality check...")
    wiki_result = verify_australian_on_wikipedia(title, year)
    print(f"  Result: {'✓ PASSED' if wiki_result else '✗ FAILED'}")
    if not wiki_result:
        return

    # Step 3: Re-release check
    print(f"\n[Step 3] Re-release check...")
    rerelease_result = verify_not_rerelease(title, year)
    print(f"  Result: {'✓ PASSED' if rerelease_result else '✗ FAILED (re-release detected)'}")
    if not rerelease_result:
        return

    # Step 4: Runtime check
    print(f"\n[Step 4] Feature film check...")
    runtime = detail.get("runtime", 0)
    feature_result = is_feature_film(detail)
    print(f"  Runtime: {runtime} mins")
    print(f"  Result: {'✓ PASSED' if feature_result else '✗ FAILED (too short)'}")
    if not feature_result:
        return

    print(f"\n✅ {title} ({year}) — WOULD BE INCLUDED")


if __name__ == "__main__":
    if not os.environ.get("TMDB_API_KEY"):
        print("ERROR: TMDB_API_KEY not set")
        sys.exit(1)

    for title, year, festival in TEST_FILMS:
        test_film(title, year, festival)

    print(f"\n{'='*60}")
    print("Done.")
