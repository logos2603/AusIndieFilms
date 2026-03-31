"""
Australian Festival Films Scraper
Fetches Australian films from major international film festivals,
enriches with data from TMDB, IMDb, Letterboxd, and Screen Australia.

Run weekly via cron or GitHub Actions.
"""

import os
import json
import time
import logging
import requests
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")  # Set in your environment
OUTPUT_FILE = Path(__file__).parent.parent / "website" / "data" / "films.json"

FESTIVALS = [
    {"name": "Cannes",          "country": "France"},
    {"name": "Venice",          "country": "Italy"},
    {"name": "Berlin",          "country": "Germany"},
    {"name": "Sundance",        "country": "USA"},
    {"name": "Toronto (TIFF)",  "country": "Canada"},
    {"name": "Rotterdam (IFFR)","country": "Netherlands"},
    {"name": "Tribeca",         "country": "USA"},
    {"name": "SXSW",            "country": "USA"},
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Film:
    title: str
    year: int
    festivals: list[str]
    director: str = ""
    synopsis: str = ""
    poster_url: str = ""
    tmdb_id: Optional[int] = None
    tmdb_rating: Optional[float] = None
    imdb_id: Optional[str] = None
    imdb_rating: Optional[float] = None
    letterboxd_rating: Optional[float] = None
    letterboxd_url: Optional[str] = None
    screen_australia_url: Optional[str] = None
    genres: list = None
    runtime_mins: Optional[int] = None
    added_at: str = ""

    def __post_init__(self):
        if self.genres is None:
            self.genres = []
        if not self.added_at:
            self.added_at = datetime.utcnow().isoformat()


# ── TMDB ──────────────────────────────────────────────────────────────────────

def tmdb_search(title: str, year: int) -> Optional[dict]:
    """Search TMDB for a film and return its full details."""
    if not TMDB_API_KEY:
        log.warning("TMDB_API_KEY not set — skipping TMDB enrichment")
        return None

    url = "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": TMDB_API_KEY, "query": title, "year": year, "language": "en-AU"}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        if not results:
            return None

        # Pick the best match
        match = results[0]
        tmdb_id = match["id"]

        # Fetch full details + credits
        detail_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
        detail = requests.get(
            detail_url,
            params={"api_key": TMDB_API_KEY, "append_to_response": "credits,external_ids"},
            timeout=10
        ).json()

        return detail
    except Exception as e:
        log.error(f"TMDB error for '{title}': {e}")
        return None


def extract_tmdb_data(detail: dict) -> dict:
    """Pull the fields we care about from a TMDB detail response."""
    poster_path = detail.get("poster_path", "")
    poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else ""

    director = ""
    crew = detail.get("credits", {}).get("crew", [])
    for member in crew:
        if member.get("job") == "Director":
            director = member.get("name", "")
            break

    imdb_id = detail.get("external_ids", {}).get("imdb_id", "")
    genres = [g["name"] for g in detail.get("genres", [])]
    runtime = detail.get("runtime")

    return {
        "tmdb_id": detail.get("id"),
        "tmdb_rating": detail.get("vote_average"),
        "synopsis": detail.get("overview", ""),
        "poster_url": poster_url,
        "director": director,
        "imdb_id": imdb_id,
        "genres": genres,
        "runtime_mins": runtime,
    }


# ── IMDb ──────────────────────────────────────────────────────────────────────

def fetch_imdb_rating(imdb_id: str) -> Optional[float]:
    """
    Scrape IMDb rating for a given IMDb ID.
    NOTE: IMDb's robots.txt discourages scraping — consider using
    the official IMDb datasets (https://datasets.imdbws.com/) instead
    for bulk lookups. This is a lightweight fallback.
    """
    if not imdb_id:
        return None
    url = f"https://www.imdb.com/title/{imdb_id}/"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        # IMDb rating sits in a specific data attribute
        rating_tag = soup.find("div", {"data-testid": "hero-rating-bar__aggregate-rating__score"})
        if rating_tag:
            span = rating_tag.find("span")
            if span:
                return float(span.text.strip())
    except Exception as e:
        log.warning(f"IMDb scrape failed for {imdb_id}: {e}")
    return None


# ── Letterboxd ────────────────────────────────────────────────────────────────

def fetch_letterboxd_data(title: str, year: int) -> dict:
    """Search Letterboxd and return rating + URL."""
    slug = title.lower().replace(" ", "-").replace(":", "").replace("'", "")
    url = f"https://letterboxd.com/film/{slug}-{year}/"
    result = {"letterboxd_url": None, "letterboxd_rating": None}
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            result["letterboxd_url"] = url
            # Rating is in a meta tag
            meta = soup.find("meta", {"name": "twitter:data2"})
            if meta and meta.get("content"):
                rating_str = meta["content"].split(" ")[0]
                result["letterboxd_rating"] = float(rating_str)
    except Exception as e:
        log.warning(f"Letterboxd lookup failed for '{title}': {e}")
    return result


# ── Screen Australia ──────────────────────────────────────────────────────────

def fetch_screen_australia_films() -> list[dict]:
    """
    Pull Australian film data from Screen Australia's data portal.
    https://www.screenaustralia.gov.au/fact-finders/production-trends

    Screen Australia publishes CSV datasets — we fetch the feature films list.
    Returns a list of dicts with at minimum 'title' and 'year'.
    """
    # Screen Australia makes production data available as CSV downloads
    # Check https://www.screenaustralia.gov.au/fact-finders/production-trends for latest URLs
    CSV_URL = "https://www.screenaustralia.gov.au/getmedia/feature-films-released.csv"
    films = []
    try:
        r = requests.get(CSV_URL, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            import csv, io
            reader = csv.DictReader(io.StringIO(r.text))
            for row in reader:
                title = row.get("Title", "").strip()
                year = row.get("Year", "").strip()
                if title and year:
                    films.append({
                        "title": title,
                        "year": int(year),
                        "screen_australia_url": f"https://www.screenaustralia.gov.au/the-screen-guide/t/{title.lower().replace(' ', '-')}",
                    })
            log.info(f"Screen Australia: loaded {len(films)} titles")
        else:
            log.warning(f"Screen Australia returned {r.status_code}")
    except Exception as e:
        log.error(f"Screen Australia fetch failed: {e}")
    return films


# ── Festival scraping ─────────────────────────────────────────────────────────
# Each function returns a list of {"title": str, "year": int, "festival": str}
# These are intentionally lightweight — festival sites change layouts frequently.
# The TMDB enrichment step does the heavy lifting afterwards.

def scrape_tmdb_festival_films(festival_keyword: str, year: int) -> list[dict]:
    """
    Use TMDB's keyword/discover endpoint to find films associated with a festival.
    This is the most reliable method as it doesn't depend on scraping festival sites.
    """
    if not TMDB_API_KEY:
        return []

    # TMDB lets you search by keyword — "Cannes" etc. returns tagged films
    kw_url = "https://api.themoviedb.org/3/search/keyword"
    try:
        r = requests.get(kw_url, params={"api_key": TMDB_API_KEY, "query": festival_keyword}, timeout=10)
        keywords = r.json().get("results", [])
        if not keywords:
            return []

        kw_id = keywords[0]["id"]
        disc_url = "https://api.themoviedb.org/3/discover/movie"
        films = []
        for page in range(1, 4):  # fetch up to 3 pages (60 results)
            resp = requests.get(disc_url, params={
                "api_key": TMDB_API_KEY,
                "with_keywords": kw_id,
                "primary_release_year": year,
                "page": page,
                "language": "en-AU",
            }, timeout=10).json()
            for m in resp.get("results", []):
                films.append({"title": m["title"], "year": year, "festival": festival_keyword})
            if page >= resp.get("total_pages", 1):
                break
        return films
    except Exception as e:
        log.error(f"TMDB festival discover failed for {festival_keyword}: {e}")
        return []


# ── Orchestration ─────────────────────────────────────────────────────────────

def is_australian(tmdb_detail: dict) -> bool:
    """Return True if TMDB lists Australia as a production country."""
    countries = tmdb_detail.get("production_countries", [])
    return any(c.get("iso_3166_1") == "AU" for c in countries)


def run_scraper():
    current_year = datetime.utcnow().year
    years_to_check = [current_year, current_year - 1]  # current + previous year

    # 1. Collect festival film candidates
    candidates: list[dict] = []
    for festival in FESTIVALS:
        for year in years_to_check:
            log.info(f"Scraping {festival['name']} {year}...")
            results = scrape_tmdb_festival_films(festival["name"], year)
            for r in results:
                r["festival"] = festival["name"]
            candidates.extend(results)
            time.sleep(0.3)  # be polite

    log.info(f"Found {len(candidates)} total candidates across all festivals")

    # 2. Deduplicate by title+year
    seen = {}
    for c in candidates:
        key = (c["title"].lower(), c["year"])
        if key not in seen:
            seen[key] = {"title": c["title"], "year": c["year"], "festivals": []}
        if c["festival"] not in seen[key]["festivals"]:
            seen[key]["festivals"].append(c["festival"])

    unique = list(seen.values())
    log.info(f"{len(unique)} unique titles after deduplication")

    # 3. Enrich with TMDB — filter to Australian films only
    australian_films: list[Film] = []
    for item in unique:
        log.info(f"Enriching: {item['title']} ({item['year']})")
        detail = tmdb_search(item["title"], item["year"])
        if not detail:
            continue
        if not is_australian(detail):
            continue

        data = extract_tmdb_data(detail)
        film = Film(
            title=item["title"],
            year=item["year"],
            festivals=item["festivals"],
            **data,
        )

        # 4. IMDb rating
        if film.imdb_id:
            film.imdb_rating = fetch_imdb_rating(film.imdb_id)
            time.sleep(1)

        # 5. Letterboxd
        lb = fetch_letterboxd_data(film.title, film.year)
        film.letterboxd_rating = lb["letterboxd_rating"]
        film.letterboxd_url = lb["letterboxd_url"]
        time.sleep(0.5)

        australian_films.append(film)
        log.info(f"  ✓ Australian: {film.title} — festivals: {', '.join(film.festivals)}")

    # 6. Cross-reference Screen Australia data
    sa_films = fetch_screen_australia_films()
    sa_titles = {f["title"].lower(): f for f in sa_films}
    for film in australian_films:
        sa_match = sa_titles.get(film.title.lower())
        if sa_match:
            film.screen_australia_url = sa_match.get("screen_australia_url")

    log.info(f"\nTotal Australian festival films found: {len(australian_films)}")

    # 7. Merge with existing data (don't overwrite films from previous runs)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing: list[dict] = []
    if OUTPUT_FILE.exists():
        existing = json.loads(OUTPUT_FILE.read_text())

    existing_keys = {(f["title"].lower(), f["year"]) for f in existing}
    new_films = [f for f in australian_films if (f.title.lower(), f.year) not in existing_keys]

    # Update festival lists for already-known films
    existing_map = {(f["title"].lower(), f["year"]): f for f in existing}
    for film in australian_films:
        key = (film.title.lower(), film.year)
        if key in existing_map:
            for fest in film.festivals:
                if fest not in existing_map[key]["festivals"]:
                    existing_map[key]["festivals"].append(fest)

    merged = list(existing_map.values()) + [asdict(f) for f in new_films]

    # Sort newest first
    merged.sort(key=lambda f: (f["year"], f["title"]), reverse=True)

    OUTPUT_FILE.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
    log.info(f"Saved {len(merged)} films to {OUTPUT_FILE}")
    log.info(f"  → {len(new_films)} new films added this run")


if __name__ == "__main__":
    run_scraper()
