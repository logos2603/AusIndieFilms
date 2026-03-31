"""
Australian Festival Films Scraper — v3
Strategy: Scrape festival websites directly for official selections,
then cross-reference with TMDB to filter Australian films and enrich data.

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
from dataclasses import dataclass, asdict, fields as datafields
from typing import Optional
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
BASE_DIR     = Path(__file__).parent.parent / "website"
OUTPUT_FILE  = BASE_DIR / "data" / "films.json"
POSTERS_DIR  = BASE_DIR / "posters"

YEARS_BACK = 5

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


# ── Festival scrapers ─────────────────────────────────────────────────────────
# Each returns a list of {"title": str, "year": int, "festival": str}

def scrape_sundance(year: int) -> list[dict]:
    """Scrape Sundance official selections."""
    films = []
    url = f"https://www.sundance.org/festivals/sundance-film-festival/program/{year}/"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        # Film titles appear in various heading/link elements
        for el in soup.find_all(["h2", "h3", "a"], class_=re.compile(r"film|title|program", re.I)):
            text = el.get_text(strip=True)
            if len(text) > 2 and len(text) < 100:
                films.append({"title": text, "year": year, "festival": "Sundance"})
    except Exception as e:
        log.warning(f"Sundance {year} scrape failed: {e}")
    return films


def scrape_tribeca(year: int) -> list[dict]:
    """Scrape Tribeca official selections via their API."""
    films = []
    # Tribeca exposes a JSON endpoint for their program
    url = f"https://tribecafilm.com/filmguide?year={year}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for el in soup.find_all(["h2", "h3"], class_=re.compile(r"film|title", re.I)):
            text = el.get_text(strip=True)
            if 2 < len(text) < 100:
                films.append({"title": text, "year": year, "festival": "Tribeca"})
    except Exception as e:
        log.warning(f"Tribeca {year} scrape failed: {e}")
    return films


def scrape_sxsw(year: int) -> list[dict]:
    """Scrape SXSW film selections."""
    films = []
    url = f"https://www.sxsw.com/film/schedule/?fwp_year={year}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for el in soup.find_all(["h2", "h3", "h4"], class_=re.compile(r"title|film|event", re.I)):
            text = el.get_text(strip=True)
            if 2 < len(text) < 100:
                films.append({"title": text, "year": year, "festival": "SXSW"})
    except Exception as e:
        log.warning(f"SXSW {year} scrape failed: {e}")
    return films


def fetch_wikipedia_festival_films(festival_name: str, wiki_title_template: str, year: int) -> list[dict]:
    """
    Fetch festival selections from Wikipedia — the most reliable source
    as Wikipedia reliably lists official selections for major festivals.
    Uses the Wikipedia API to fetch page content.
    """
    films = []
    # Build the Wikipedia article title, e.g. "72nd Cannes Film Festival"
    title = wiki_title_template.format(year=year)
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": title,
        "prop": "wikitext",
        "format": "json",
        "redirects": True,
    }
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        data = r.json()
        if "error" in data:
            log.warning(f"Wikipedia: page not found — '{title}'")
            return []

        wikitext = data.get("parse", {}).get("wikitext", {}).get("*", "")

        # Extract film titles from wikitext — they appear as [[Film Title]] links
        # or in table rows. We extract all [[...]] wikilinks that look like film titles.
        raw_links = re.findall(r"\[\[([^\]|#]+?)(?:\|[^\]]*)?\]\]", wikitext)

        # Filter out non-film links (categories, people, countries, years etc.)
        skip_patterns = re.compile(
            r"^(File:|Image:|Category:|Wikipedia:|Template:|Help:|Portal:|"
            r"\d{4}|January|February|March|April|May|June|July|August|"
            r"September|October|November|December|United|France|Italy|"
            r"Germany|Australia|USA|UK|Canada|Director|Film|Cinema|Award)",
            re.I
        )
        for link in raw_links:
            link = link.strip()
            if len(link) < 2 or len(link) > 100:
                continue
            if skip_patterns.match(link):
                continue
            if link.lower() in ("film", "cinema", "movie", "award", "prize"):
                continue
            films.append({"title": link, "year": year, "festival": festival_name})

        log.info(f"Wikipedia [{festival_name} {year}]: {len(films)} candidate titles from '{title}'")
    except Exception as e:
        log.warning(f"Wikipedia fetch failed for {festival_name} {year}: {e}")
    return films


# Wikipedia article title templates for each festival
# {year} is replaced with the actual year
WIKI_TEMPLATES = {
    "Cannes":    [
        "{year} Cannes Film Festival",
        "Cannes Film Festival {year}",
    ],
    "Venice":    [
        "{year} Venice International Film Festival",
        "Venice Film Festival {year}",
    ],
    "Berlin":    [
        "{year} Berlin International Film Festival",
        "Berlinale {year}",
    ],
    "Sundance":  [
        "{year} Sundance Film Festival",
        "Sundance Film Festival {year}",
    ],
    "Toronto":   [
        "{year} Toronto International Film Festival",
        "TIFF {year}",
    ],
    "Rotterdam": [
        "{year} International Film Festival Rotterdam",
        "IFFR {year}",
    ],
    "Tribeca":   [
        "{year} Tribeca Film Festival",
        "Tribeca Film Festival {year}",
    ],
    "SXSW":      [
        "{year} SXSW Film Festival",
        "SXSW {year} film",
    ],
}


def get_festival_films(festival: str, years: list[int]) -> list[dict]:
    """Get all films for a festival across given years, using Wikipedia as primary source."""
    all_films = []
    templates = WIKI_TEMPLATES.get(festival, [])

    for year in years:
        found = False
        for template in templates:
            films = fetch_wikipedia_festival_films(festival, template, year)
            if films:
                all_films.extend(films)
                found = True
                break  # stop trying templates once one works
        if not found:
            log.warning(f"  No Wikipedia data found for {festival} {year}")
        time.sleep(0.3)

    return all_films


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


def tmdb_search_film(title: str, year: int) -> Optional[dict]:
    """Search TMDB for a film, return full details if found."""
    data = tmdb_get("/search/movie", {
        "query": title,
        "year": year,
        "language": "en-AU",
    })
    if not data or not data.get("results"):
        # Try without year constraint
        data = tmdb_get("/search/movie", {"query": title, "language": "en-AU"})
    if not data or not data.get("results"):
        return None

    # Pick best match — prefer exact title match
    results = data["results"]
    for r in results:
        if r.get("title", "").lower() == title.lower():
            return tmdb_get(f"/movie/{r['id']}", {
                "append_to_response": "credits,external_ids",
                "language": "en-AU",
            })

    # Fall back to first result
    return tmdb_get(f"/movie/{results[0]['id']}", {
        "append_to_response": "credits,external_ids",
        "language": "en-AU",
    })


def is_australian(detail: dict) -> bool:
    """Return True if TMDB lists Australia as a production country."""
    countries = detail.get("production_countries", [])
    return any(c.get("iso_3166_1") == "AU" for c in countries)


def is_feature_film(detail: dict) -> bool:
    """Return True if the film is 70+ minutes (feature length)."""
    runtime = detail.get("runtime") or 0
    return runtime >= 70


def extract_tmdb_data(detail: dict) -> dict:
    director = ""
    for m in detail.get("credits", {}).get("crew", []):
        if m.get("job") == "Director":
            director = m.get("name", "")
            break
    return {
        "tmdb_id":      detail.get("id"),
        "tmdb_rating":  round(detail.get("vote_average") or 0, 1) or None,
        "synopsis":     detail.get("overview", ""),
        "poster_path":  detail.get("poster_path", ""),
        "director":     director,
        "imdb_id":      detail.get("external_ids", {}).get("imdb_id", ""),
        "genres":       [g["name"] for g in detail.get("genres", [])],
        "runtime_mins": detail.get("runtime") or None,
    }


# ── Poster downloading ────────────────────────────────────────────────────────

def download_poster(poster_path: str, tmdb_id: int) -> str:
    if not poster_path:
        return ""
    POSTERS_DIR.mkdir(parents=True, exist_ok=True)
    local_path = POSTERS_DIR / f"{tmdb_id}.jpg"
    relative   = f"posters/{tmdb_id}.jpg"
    if local_path.exists():
        return relative
    try:
        r = requests.get(f"https://image.tmdb.org/t/p/w500{poster_path}", timeout=20)
        if r.status_code == 200:
            local_path.write_bytes(r.content)
            log.info(f"  ↓ poster: {tmdb_id}.jpg")
            return relative
    except Exception as e:
        log.warning(f"  Poster download error: {e}")
    return ""


# ── IMDb ──────────────────────────────────────────────────────────────────────

def fetch_imdb_rating(imdb_id: str) -> Optional[float]:
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

def fetch_letterboxd_data(title: str, year: int) -> dict:
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

def fetch_screen_australia_films() -> dict:
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
                if title:
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

    # Step 1: Collect all festival film candidates from Wikipedia
    log.info("\n── Step 1: Collecting festival selections from Wikipedia ──")
    candidates: dict = {}  # (title_lower, year) → {title, year, festivals: []}

    for festival in WIKI_TEMPLATES.keys():
        log.info(f"\nFestival: {festival}")
        films = get_festival_films(festival, years)
        for f in films:
            key = (f["title"].lower(), f["year"])
            if key not in candidates:
                candidates[key] = {"title": f["title"], "year": f["year"], "festivals": []}
            if f["festival"] not in candidates[key]["festivals"]:
                candidates[key]["festivals"].append(f["festival"])

    log.info(f"\nTotal unique festival candidates: {len(candidates)}")

    # Step 2: Load existing cache
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing: list = []
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text())
        except Exception:
            existing = []
    existing_map: dict = {f["tmdb_id"]: f for f in existing if f.get("tmdb_id")}
    existing_titles: dict = {(f.get("title","").lower(), f.get("year")): f for f in existing}

    # Step 3: TMDB lookup — filter to Australian feature films only
    log.info("\n── Step 2: Matching candidates against TMDB (Australian features only) ──")
    field_names = {f.name for f in datafields(Film)}
    result_films: list = []
    checked = 0

    for (title_lower, year), info in candidates.items():
        checked += 1
        title = info["title"]

        # Use cache if available and fresh
        cache_key = (title_lower, year)
        if cache_key in existing_titles:
            cached = existing_titles[cache_key]
            try:
                age = (datetime.utcnow() - datetime.fromisoformat(cached.get("added_at", "2000-01-01"))).days
                if age < 30:
                    for fest in info["festivals"]:
                        if fest not in cached.get("festivals", []):
                            cached["festivals"].append(fest)
                    result_films.append(cached)
                    continue
            except Exception:
                pass

        log.info(f"[{checked}/{len(candidates)}] Checking: {title} ({year})")
        detail = tmdb_search_film(title, year)
        if not detail:
            log.info(f"  ✗ Not found on TMDB")
            continue
        if not is_australian(detail):
            log.info(f"  ✗ Not Australian")
            continue
        if not is_feature_film(detail):
            log.info(f"  ✗ Short film ({detail.get('runtime')} mins)")
            continue

        data = extract_tmdb_data(detail)
        poster_path = data.pop("poster_path", "")
        poster_url  = download_poster(poster_path, data["tmdb_id"])
        time.sleep(0.2)

        film = Film(
            title=title,
            year=year,
            festivals=info["festivals"],
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

        result_films.append(asdict(film))
        log.info(f"  ✓ AUSTRALIAN FEATURE: {film.title} ({film.year}) | {film.festivals}")

    # Step 4: Screen Australia cross-reference
    log.info("\n── Step 3: Screen Australia cross-reference ──")
    sa = fetch_screen_australia_films()
    for film in result_films:
        match = sa.get(film.get("title", "").lower())
        if match and not film.get("screen_australia_url"):
            film["screen_australia_url"] = match["screen_australia_url"]

    # Step 5: Merge + save
    new_map    = {f["tmdb_id"]: f for f in result_films if f.get("tmdb_id")}
    merged     = list({**existing_map, **new_map}.values())
    merged.sort(key=lambda f: (-(f.get("year") or 0), f.get("title", "")))

    OUTPUT_FILE.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
    new_count = len(set(new_map.keys()) - set(existing_map.keys()))
    log.info(f"\nDone. {len(merged)} total films saved ({new_count} new).")


if __name__ == "__main__":
    run_scraper()
