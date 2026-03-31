"""
Australian Festival Films Scraper — v2
Fetches Australian films from major international film festivals,
enriches with data from TMDB, IMDb, Letterboxd, and Screen Australia.
Downloads posters locally so they always display correctly.

Run weekly via cron or GitHub Actions.
"""

import os
import json
import time
import logging
import re
import requests
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict, fields
from typing import Optional
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "")
BASE_DIR      = Path(__file__).parent.parent / "website"
OUTPUT_FILE   = BASE_DIR / "data" / "films.json"
POSTERS_DIR   = BASE_DIR / "posters"

YEARS_BACK = 5   # how many years back to search

FESTIVALS = ["Cannes", "Venice", "Berlin", "Sundance", "Toronto", "Rotterdam", "Tribeca", "SXSW"]

# TMDB keyword search terms for each festival
# Using multiple terms per festival increases recall
FESTIVAL_KEYWORDS = {
    "Cannes":     ["cannes film festival", "cannes"],
    "Venice":     ["venice film festival", "venice international film festival"],
    "Berlin":     ["berlin international film festival", "berlinale"],
    "Sundance":   ["sundance film festival", "sundance"],
    "Toronto":    ["toronto international film festival", "tiff"],
    "Rotterdam":  ["international film festival rotterdam", "iffr", "rotterdam film festival"],
    "Tribeca":    ["tribeca film festival", "tribeca"],
    "SXSW":       ["sxsw", "south by southwest film festival"],
}

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
    festivals: list
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


# ── TMDB helpers ──────────────────────────────────────────────────────────────

def tmdb_get(path, params={}):
    if not TMDB_API_KEY:
        return None
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3{path}",
            params={"api_key": TMDB_API_KEY, **params},
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"TMDB {path} failed: {e}")
        return None


def discover_australian_films(year):
    """Discover feature films (70+ mins) with Australia as production country for a given year."""
    films = []
    page = 1
    while True:
        data = tmdb_get("/discover/movie", {
            "with_origin_country": "AU",
            "primary_release_year": year,
            "sort_by": "vote_count.desc",
            "with_runtime.gte": 70,   # feature films only
            "page": page,
        })
        if not data:
            break
        results = data.get("results", [])
        films.extend(results)
        log.info(f"  AU discover {year} p{page}/{data.get('total_pages',1)}: {len(results)} films")
        if page >= min(data.get("total_pages", 1), 25):
            break
        page += 1
        time.sleep(0.25)
    return films


def get_festival_tmdb_ids(festival_name, years):
    """
    Get all TMDB movie IDs tagged with a festival keyword.
    Searches multiple keyword variants and collects all matching IDs.
    """
    search_terms = FESTIVAL_KEYWORDS.get(festival_name, [festival_name.lower()])

    # Find all keyword IDs for this festival
    kw_ids = []
    for term in search_terms:
        data = tmdb_get("/search/keyword", {"query": term})
        if not data:
            continue
        for kw in data.get("results", []):
            kw_name = kw.get("name", "").lower()
            # Only accept keywords that meaningfully match the festival
            if any(word in kw_name for word in term.lower().split()):
                kw_ids.append(kw["id"])
                log.info(f"  [{festival_name}] keyword match: '{kw['name']}' (id={kw['id']})")
        time.sleep(0.1)

    if not kw_ids:
        log.warning(f"  [{festival_name}] No keyword IDs found")
        return set()

    # Deduplicate keyword IDs
    kw_ids = list(set(kw_ids))
    kw_combined = "|".join(str(k) for k in kw_ids)  # TMDB OR logic

    ids = set()
    for year in years:
        page = 1
        while True:
            data = tmdb_get("/discover/movie", {
                "with_keywords": kw_combined,
                "primary_release_year": year,
                "page": page,
            })
            if not data:
                break
            for m in data.get("results", []):
                ids.add(m["id"])
            if page >= data.get("total_pages", 1):
                break
            page += 1
            time.sleep(0.25)

    log.info(f"  {festival_name}: {len(ids)} tagged films across {years}")
    return ids


def get_full_details(tmdb_id):
    return tmdb_get(f"/movie/{tmdb_id}", {
        "append_to_response": "credits,external_ids,keywords",
        "language": "en-AU",
    })


def extract_tmdb_data(detail):
    director = ""
    for m in detail.get("credits", {}).get("crew", []):
        if m.get("job") == "Director":
            director = m.get("name", "")
            break

    return {
        "tmdb_id":      detail.get("id"),
        "tmdb_rating":  round(detail.get("vote_average", 0) or 0, 1) or None,
        "synopsis":     detail.get("overview", ""),
        "poster_path":  detail.get("poster_path", ""),
        "director":     director,
        "imdb_id":      detail.get("external_ids", {}).get("imdb_id", ""),
        "genres":       [g["name"] for g in detail.get("genres", [])],
        "runtime_mins": detail.get("runtime") or None,
    }


# ── Poster downloading ────────────────────────────────────────────────────────

def download_poster(poster_path, tmdb_id):
    """Download poster from TMDB and save locally. Returns relative URL path."""
    if not poster_path:
        return ""
    POSTERS_DIR.mkdir(parents=True, exist_ok=True)
    local_path = POSTERS_DIR / f"{tmdb_id}.jpg"
    relative  = f"posters/{tmdb_id}.jpg"
    if local_path.exists():
        return relative
    try:
        r = requests.get(f"https://image.tmdb.org/t/p/w500{poster_path}", timeout=20)
        if r.status_code == 200:
            local_path.write_bytes(r.content)
            log.info(f"  ↓ poster saved: {tmdb_id}.jpg")
            return relative
    except Exception as e:
        log.warning(f"  Poster download error: {e}")
    return ""


# ── IMDb ──────────────────────────────────────────────────────────────────────

def fetch_imdb_rating(imdb_id):
    if not imdb_id:
        return None
    try:
        r = requests.get(f"https://www.imdb.com/title/{imdb_id}/", headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        tag = soup.find("div", {"data-testid": "hero-rating-bar__aggregate-rating__score"})
        if tag:
            span = tag.find("span")
            if span:
                return float(span.text.strip())
    except Exception as e:
        log.warning(f"IMDb scrape failed for {imdb_id}: {e}")
    return None


# ── Letterboxd ────────────────────────────────────────────────────────────────

def fetch_letterboxd_data(title, year):
    slug = re.sub(r"[^a-z0-9\s-]", "", title.lower())
    slug = re.sub(r"\s+", "-", slug.strip())
    result = {"letterboxd_url": None, "letterboxd_rating": None}
    for url in [f"https://letterboxd.com/film/{slug}-{year}/", f"https://letterboxd.com/film/{slug}/"]:
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                result["letterboxd_url"] = url
                meta = soup.find("meta", {"name": "twitter:data2"})
                if meta and meta.get("content"):
                    try:
                        result["letterboxd_rating"] = float(meta["content"].split(" ")[0])
                    except ValueError:
                        pass
                return result
        except Exception:
            pass
    return result


# ── Screen Australia ──────────────────────────────────────────────────────────

def fetch_screen_australia_films():
    lookup = {}
    try:
        r = requests.get(
            "https://www.screenaustralia.gov.au/getmedia/feature-films-released.csv",
            headers=HEADERS, timeout=15
        )
        if r.status_code == 200:
            import csv, io
            for row in csv.DictReader(io.StringIO(r.text)):
                title = row.get("Title", "").strip()
                year  = row.get("Year", "").strip()
                if title and year:
                    slug = title.lower().replace(" ", "-")
                    lookup[title.lower()] = {
                        "screen_australia_url": f"https://www.screenaustralia.gov.au/the-screen-guide/t/{slug}",
                    }
            log.info(f"Screen Australia: {len(lookup)} titles loaded")
    except Exception as e:
        log.error(f"Screen Australia fetch failed: {e}")
    return lookup


# ── Main ──────────────────────────────────────────────────────────────────────

def run_scraper():
    if not TMDB_API_KEY:
        log.error("TMDB_API_KEY not set. Exiting.")
        return

    current_year = datetime.utcnow().year
    years = list(range(current_year - YEARS_BACK, current_year + 1))
    log.info(f"Searching years: {years}")

    # Step 1: All Australian films
    log.info("\n── Step 1: Discovering Australian films ──")
    all_au: dict = {}
    for year in years:
        for m in discover_australian_films(year):
            all_au[m["id"]] = m
        time.sleep(0.5)
    log.info(f"Total Australian films discovered: {len(all_au)}")

    # Step 2: Festival-tagged film IDs
    log.info("\n── Step 2: Getting festival tags ──")
    festival_map: dict = {}
    for festival in FESTIVALS:
        ids = get_festival_tmdb_ids(festival, years)
        for tmdb_id in ids:
            festival_map.setdefault(tmdb_id, [])
            if festival not in festival_map[tmdb_id]:
                festival_map[tmdb_id].append(festival)
        time.sleep(0.5)

    # Intersection: Australian + festival-tagged
    target_ids = set(all_au.keys()) & set(festival_map.keys())
    log.info(f"Australian films with festival tags: {len(target_ids)}")

    # Step 3: Load existing cache
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing: list = []
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text())
        except Exception:
            existing = []
    existing_map: dict = {f["tmdb_id"]: f for f in existing if f.get("tmdb_id")}

    # Step 4: Enrich each film
    log.info("\n── Step 3: Enriching films ──")
    field_names = {f.name for f in fields(Film)}
    result_films: list = []

    for tmdb_id in sorted(target_ids):
        title = all_au[tmdb_id].get("title", "Unknown")
        log.info(f"Processing: {title} (id={tmdb_id})")

        # Use cache if fresh (< 30 days)
        if tmdb_id in existing_map:
            cached = existing_map[tmdb_id]
            try:
                age = (datetime.utcnow() - datetime.fromisoformat(cached.get("added_at", "2000-01-01"))).days
                if age < 30:
                    for fest in festival_map.get(tmdb_id, []):
                        if fest not in cached.get("festivals", []):
                            cached["festivals"].append(fest)
                    result_films.append(cached)
                    log.info(f"  (cached, {age}d old)")
                    continue
            except Exception:
                pass

        detail = get_full_details(tmdb_id)
        if not detail:
            continue

        data = extract_tmdb_data(detail)
        poster_path = data.pop("poster_path", "")
        poster_url  = download_poster(poster_path, tmdb_id)
        time.sleep(0.2)

        raw_year = all_au[tmdb_id].get("release_date", "0000")[:4]
        year = int(raw_year) if raw_year.isdigit() else 0

        film = Film(
            title=title,
            year=year,
            festivals=festival_map.get(tmdb_id, []),
            poster_url=poster_url,
            **{k: v for k, v in data.items() if k in field_names},
        )

        if film.imdb_id:
            film.imdb_rating = fetch_imdb_rating(film.imdb_id)
            time.sleep(1)

        lb = fetch_letterboxd_data(film.title, film.year)
        film.letterboxd_rating = lb["letterboxd_rating"]
        film.letterboxd_url    = lb["letterboxd_url"]
        time.sleep(0.5)

        # Skip short films as a safety net
        if film.runtime_mins and film.runtime_mins < 70:
            log.info(f"  ✗ Skipping short film: {film.title} ({film.runtime_mins} mins)")
            continue

        result_films.append(asdict(film))
        log.info(f"  ✓ {film.title} ({film.year}) | festivals: {film.festivals}")

    # Step 5: Screen Australia
    log.info("\n── Step 4: Screen Australia cross-reference ──")
    sa = fetch_screen_australia_films()
    for film in result_films:
        match = sa.get(film.get("title", "").lower())
        if match and not film.get("screen_australia_url"):
            film["screen_australia_url"] = match["screen_australia_url"]

    # Step 6: Merge + save
    new_map = {f["tmdb_id"]: f for f in result_films if f.get("tmdb_id")}
    merged  = list({**existing_map, **new_map}.values())
    merged.sort(key=lambda f: (-(f.get("year") or 0), f.get("title", "")))

    OUTPUT_FILE.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
    new_count = len(set(new_map.keys()) - set(existing_map.keys()))
    log.info(f"\nDone. {len(merged)} total films saved ({new_count} new).")


if __name__ == "__main__":
    run_scraper()
