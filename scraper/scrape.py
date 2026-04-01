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
BASE_DIR     = Path(__file__).parent.parent / "docs"
OUTPUT_FILE  = BASE_DIR / "data" / "films.json"
POSTERS_DIR  = BASE_DIR / "posters"

YEARS_BACK = 16

# Known Australian festival films to always include regardless of scrape results
# Add films here if they are confirmed Australian but not being picked up automatically
# Format: (title, year, [festivals])
SEED_FILMS = [
    ("Audrey",                      2024, ["SXSW"]),
    ("Monolith",                    2023, ["SXSW"]),
    ("The Moogai",                  2024, ["SXSW"]),
    ("Talk to Me",                  2022, ["Sundance", "SXSW"]),
    ("Dangerous Animals",           2025, ["Cannes"]),
    ("The Drover's Wife: The Legend of Molly Johnson", 2021, ["Toronto", "Berlin"]),
    ("Babyteeth",                   2019, ["Venice", "Toronto"]),
    ("The Nightingale",             2018, ["Venice"]),
    ("Sweet Country",               2017, ["Venice", "Toronto"]),
    ("Samson & Delilah",            2009, ["Cannes"]),
    ("Animal Kingdom",              2010, ["Sundance", "Toronto"]),
]

# TMDB IDs to explicitly exclude (films incorrectly tagged as Australian in TMDB)
BLOCKLIST_TMDB_IDS = {
    1115379,  # Only the River Flows (2023) — Chinese film, incorrectly tagged
    289450,   # Driving Miss Daisy — not Australian
    984056,   # Berlin — not Australian
    884692,   # incorrectly tagged
    51450,    # L'apprenti père Noël — not Australian
    1130852,  # Ka Whawhai Tonu — not Australian
}

# Title-based blocklist — catches films regardless of TMDB ID
# Use when TMDB ID is unknown or unreliable
BLOCKLIST_TITLES = {
    "uvalde mom",           # American documentary
    "three thousand years of longing",  # not primarily Australian despite AU co-production
    "lilith fair: building a mystery",  # Canadian/American
}

# Known Australian production companies and funders
AUSTRALIAN_COMPANIES = {
    "screen australia", "abc", "australian broadcasting corporation",
    "sbs", "special broadcasting service", "film victoria",
    "screen nsw", "screen queensland", "screen west",
    "south australian film corporation", "safc", "screen tasmania",
    "northern territory screen", "act screen industry",
    "adelaide film festival", "melbourne international film festival",
    "miff", "sydney film festival",
    "roadshow", "madman", "umbrella entertainment",
    "transmission films", "foxtel", "stan originals",
    "bunya productions", "porchlight films", "aquarius films",
    "black labrador", "closer productions", "see-saw films",
    "arenamedia", "matchbox pictures", "princess pictures",
    "invisible republic", "orange entertainment", "wildbear",
    "blackfella films", "secret sauce", "join the dots",
    "where's the bear", "gristmill", "calamity films",
    "hopscotch features", "fulcrum media", "good thing productions",
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
    revenue: Optional[int] = None
    budget: Optional[int] = None
    distributor: str = ""
    distributor_intl: str = ""
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
            r"Germany|Australia|USA|UK|Canada|Director|Film|Cinema|Award|"
            r"Golden|Silver|Palme|Bear|Lion|Jury|Prix|Camera|Grand|Special|"
            r"Best|Honorary|Main|Short|Documentary|Animation|Series|Section|"
            r"Competition|Midnight|World|International|National|American|"
            r"New|List|History|Overview)",
            re.I
        )
        # Only keep links that look like film titles (mixed case, not all caps)
        seen_links = set()
        for link in raw_links:
            link = link.strip()
            if len(link) < 3 or len(link) > 80:
                continue
            if link in seen_links:
                continue
            if skip_patterns.match(link):
                continue
            if link.lower() in ("film", "cinema", "movie", "award", "prize", "the", "a", "an"):
                continue
            # Skip if it looks like a person's name (First Last pattern with no other words)
            name_like = re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+$", link)
            if name_like:
                continue
            seen_links.add(link)
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
    "Directors Fortnight": [
        "{year} Directors' Fortnight",
        "Directors' Fortnight {year}",
    ],
    "Rotterdam": [
        "{year} International Film Festival Rotterdam",
        "IFFR {year}",
    ],
    "SXSW":      [
        "{year} South by Southwest Film & TV Festival",
        "{year} South by Southwest Film Festival",
        "South by Southwest {year} film",
    ],
    "Sitges":    [
        "{edition}th Sitges Film Festival",
        "{edition}th Sitges International Film Festival",
    ],
}


# SXSW PDF archive URLs — scraped from sxsw.com/festivals/film/archive/
SXSW_PDF_URLS = {
    2025: "https://sxsw.com/wp-content/uploads/2025/09/25_SXSW_FilmTV-Archive_4.7.pdf",
    2024: "https://sxsw.com/wp-content/uploads/2024/06/24_SXSW_FilmTV-Archive.pdf",
    2023: "https://sxsw.com/wp-content/uploads/2023/07/23_SXSW_FilmTV-Archive.pdf",
    2022: "https://sxsw.com/wp-content/uploads/2022/06/22_SXSW_FilmArchive1.pdf",
    2021: "https://sxsw.com/wp-content/uploads/2022/07/2021FilmArchive-2.pdf",
    2020: "https://sxsw.com/wp-content/uploads/2021/06/FilmPocketGuide2020_resize.pdf",
    2019: "https://sxsw.com/wp-content/uploads/2019/06/2019FilmArchive-1.pdf",
    2018: "https://sxsw.com/wp-content/uploads/2018/06/2018FilmArchive-1.pdf",
    2017: "https://sxsw.com/wp-content/uploads/2016/08/2017-Film-Archive-2.pdf",
    2016: "https://sxsw.com/wp-content/uploads/2016/08/2016_film-archive-1.pdf",
}


def scrape_sxsw_pdf(year: int) -> list[dict]:
    """
    Download and extract film titles from the SXSW annual PDF archive.
    The PDF is a multi-column table: Title | Director | Category | Section | Premiere
    We extract the first column (title) using pdfplumber's table extraction.
    Falls back to text extraction if table extraction fails.
    """
    url = SXSW_PDF_URLS.get(year)
    if not url:
        return []

    try:
        import pdfplumber
        import io

        log.info(f"  Downloading SXSW {year} PDF archive...")
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            log.warning(f"  SXSW PDF {year} returned HTTP {r.status_code}")
            return []

        films = []
        seen = set()

        # Known column header values to skip
        SKIP_VALUES = {
            "title", "director(s)", "director", "film category",
            "screening section", "premiere status", "2024 archive",
            "2023 archive", "2022 archive", "2021 archive", "2020 archive",
            "", None,
        }

        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for page in pdf.pages:
                # Try table extraction first — most reliable for columnar PDFs
                tables = page.extract_tables()
                if tables:
                    for table in tables:
                        for row in table:
                            if not row:
                                continue
                            # First cell is the title
                            title = (row[0] or "").strip()
                            if not title:
                                continue
                            if title.lower() in SKIP_VALUES:
                                continue
                            if title.isupper():
                                continue  # section headers like "2024 ARCHIVE"
                            if title.isdigit():
                                continue
                            if len(title) < 2 or len(title) > 100:
                                continue
                            if title.lower() in seen:
                                continue
                            seen.add(title.lower())
                            films.append({"title": title, "year": year, "festival": "SXSW"})
                else:
                    # Fallback: text extraction — split on multiple spaces to get first column
                    text = page.extract_text()
                    if not text:
                        continue
                    for line in text.split("\n"):
                        line = line.strip()
                        if not line or line.isupper() or line.isdigit():
                            continue
                        # In the text fallback, title is everything before 2+ spaces
                        # (the columns are separated by large whitespace gaps)
                        parts = re.split(r"  +", line)
                        title = parts[0].strip()
                        if len(title) < 2 or len(title) > 100:
                            continue
                        if title.lower() in SKIP_VALUES:
                            continue
                        if title.lower() in seen:
                            continue
                        seen.add(title.lower())
                        films.append({"title": title, "year": year, "festival": "SXSW"})

        log.info(f"  SXSW {year} PDF: extracted {len(films)} candidate titles")
        return films

    except ImportError:
        log.error("pdfplumber not installed — run: pip install pdfplumber")
        return []
    except Exception as e:
        log.error(f"  SXSW PDF scrape failed for {year}: {e}")
        return []


def scrape_directors_fortnight(year: int) -> list[dict]:
    """
    Scrape film titles from the Directors' Fortnight (Quinzaine des cinéastes) website.
    URL pattern: https://www.quinzaine-cineastes.fr/en/selection/{year}
    Film titles appear in <h3> tags within film listing cards.
    """
    url = f"https://www.quinzaine-cineastes.fr/en/selection/{year}"
    films = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            log.warning(f"  Directors Fortnight {year} returned HTTP {r.status_code}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")

        # Film titles are in <h3> tags — the English title is the second <h3>
        # if there are two (original + translated), otherwise just the first
        seen = set()
        # Each film block contains one or two h3 tags (original title + English title)
        # We want the English title where available, otherwise the original
        for card in soup.find_all("a", href=re.compile(r"/en/film/")):
            h3s = card.find_all("h3")
            if not h3s:
                continue
            # Last h3 is typically the English title
            title = h3s[-1].get_text(strip=True)
            if not title or title.lower() in seen:
                continue
            if len(title) < 2 or len(title) > 100:
                continue
            seen.add(title.lower())
            films.append({"title": title, "year": year, "festival": "Directors Fortnight"})

        log.info(f"  Directors Fortnight {year}: {len(films)} films scraped")
    except Exception as e:
        log.warning(f"  Directors Fortnight {year} scrape failed: {e}")
    return films


# ── Screen Australia festival scraper ────────────────────────────────────────

# Map Screen Australia festival names to our internal festival names
SA_FESTIVAL_MAP = {
    "cannes film festival":                          "Cannes",
    "berlin international film festival":            "Berlin",
    "sundance film festival":                        "Sundance",
    "toronto international film festival":           "Toronto",
    "international film festival rotterdam":         "Rotterdam",
    "south by southwest film":                       "SXSW",
    "venice international film festival":            "Venice",
    "sitges international fantastic film festival":  "Sitges",
    "directors' fortnight":                          "Directors Fortnight",
}

def scrape_screen_australia_festivals(years: list[int]) -> dict:
    """
    Scrape Screen Australia's definitive list of Australian films at
    international festivals. Returns a dict keyed by (title_lower, year)
    with {title, year, festivals: [], screen_australia_url} values.
    """
    url = "https://www.screenaustralia.gov.au/australian-success/australian-screenings-at-international-festivals"
    results = {}

    try:
        log.info("Fetching Screen Australia festival listings...")
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            log.warning(f"Screen Australia returned HTTP {r.status_code}")
            return results

        soup = BeautifulSoup(r.text, "html.parser")
        current_year = None

        # The page lists entries under year headings (h3) then film entries (h4 + p)
        # Each entry has: h4 with film title link, p with section + festival name
        for el in soup.find_all(["h3", "h4"]):
            if el.name == "h3":
                # Year heading
                try:
                    current_year = int(el.get_text(strip=True))
                except ValueError:
                    current_year = None
                continue

            if el.name == "h4" and current_year and current_year in years:
                # Film entry
                link = el.find("a")
                if not link:
                    continue

                title = link.get_text(strip=True)
                sa_url = link.get("href", "")
                if sa_url and not sa_url.startswith("http"):
                    sa_url = "https://www.screenaustralia.gov.au" + sa_url

                # Get the festival name from the next <p> sibling
                festival_name = ""
                next_p = el.find_next_sibling("p")
                if not next_p:
                    # Try parent's next sibling
                    parent = el.parent
                    if parent:
                        next_p = parent.find_next_sibling()

                if next_p:
                    # Festival link is inside the <p>
                    fest_link = next_p.find("a")
                    if fest_link:
                        festival_name = fest_link.get_text(strip=True).lower()

                # Map to our festival names
                mapped = None
                for key, val in SA_FESTIVAL_MAP.items():
                    if key in festival_name:
                        mapped = val
                        break

                if not mapped:
                    continue  # festival not in our tracked list

                key = (title.lower(), current_year)
                if key not in results:
                    results[key] = {
                        "title": title,
                        "year": current_year,
                        "festivals": [],
                        "screen_australia_url": sa_url,
                    }
                if mapped not in results[key]["festivals"]:
                    results[key]["festivals"].append(mapped)

        log.info(f"Screen Australia: found {len(results)} unique film/year entries across tracked festivals")

    except Exception as e:
        log.error(f"Screen Australia festival scrape failed: {e}")

    return results


def get_festival_films(festival: str, years: list[int]) -> list[dict]:
    """Get all films for a festival across given years, using Wikipedia as primary source.
    For SXSW, supplements Wikipedia with the official PDF archive.
    For Sitges, uses ordinal edition numbers (festival started 1968)."""
    all_films = []
    templates = WIKI_TEMPLATES.get(festival, [])

    for year in years:
        found = False
        for template in templates:
            # Sitges uses ordinal edition numbers: 2024 = 57th (year - 1967)
            if "{edition}" in template:
                edition = year - 1967
                resolved = template.replace("{edition}", str(edition)).replace("{year}", str(year))
            else:
                resolved = template.replace("{year}", str(year))
            films = fetch_wikipedia_festival_films(festival, resolved, year)
            if films:
                all_films.extend(films)
                found = True
                break  # stop trying templates once one works

        # For Directors' Fortnight: supplement with direct website scraping
        if festival == "Directors Fortnight":
            web_films = scrape_directors_fortnight(year)
            if web_films:
                all_films.extend(web_films)
                found = True

        # For SXSW: supplement with (or fall back to) the official PDF archive
        if festival == "SXSW":
            pdf_films = scrape_sxsw_pdf(year)
            if pdf_films:
                all_films.extend(pdf_films)
                found = True

        if not found:
            log.warning(f"  No data found for {festival} {year}")
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
    """Search TMDB for a film, return full details if found.
    Prefers Australian results when multiple matches exist."""
    def get_details(tmdb_id):
        return tmdb_get(f"/movie/{tmdb_id}", {
            "append_to_response": "credits,external_ids,release_dates",
            "language": "en-AU",
        })

    def au_score(result):
        """Score a search result by how likely it is to be Australian."""
        score = 0
        if result.get("original_language") == "en":
            score += 1
        origin = result.get("origin_country", [])
        if "AU" in origin:
            score += 5
        # Prefer results whose release year matches
        release = (result.get("release_date") or "")[:4]
        if release == str(year):
            score += 2
        elif release == str(year - 1) or release == str(year + 1):
            score += 1
        return score

    # Search with year first
    data = tmdb_get("/search/movie", {"query": title, "year": year, "language": "en-AU"})
    if not data or not data.get("results"):
        # Try without year constraint
        data = tmdb_get("/search/movie", {"query": title, "language": "en-AU"})
    if not data or not data.get("results"):
        return None

    results = data["results"]

    # Sort by: exact title match first, then AU score
    exact = [r for r in results if r.get("title", "").lower() == title.lower()]
    others = [r for r in results if r.get("title", "").lower() != title.lower()]

    # Among exact matches prefer Australian ones
    exact.sort(key=au_score, reverse=True)
    others.sort(key=au_score, reverse=True)

    ranked = exact + others

    # If top result has a strong AU signal, use it directly
    if ranked and au_score(ranked[0]) >= 5:
        return get_details(ranked[0]["id"])

    # Otherwise fetch details for top 3 and pick the most Australian
    candidates = []
    for r in ranked[:3]:
        detail = get_details(r["id"])
        if detail:
            candidates.append(detail)
        time.sleep(0.1)

    if not candidates:
        return None

    # Prefer the one with AU in production_countries or origin_country
    for d in candidates:
        countries = [c.get("iso_3166_1") for c in d.get("production_countries", [])]
        origins = d.get("origin_country", [])
        if "AU" in countries or "AU" in origins:
            return d

    return candidates[0]


def is_australian(detail: dict) -> bool:
    """Return True if TMDB lists Australia as a production country."""
    countries = detail.get("production_countries", [])
    return any(c.get("iso_3166_1") == "AU" for c in countries)


def is_feature_film(detail: dict) -> bool:
    """Return True if the film is 70+ minutes (feature length)."""
    runtime = detail.get("runtime") or 0
    return runtime >= 70


def fetch_wikipedia_film_article(title: str, year: int) -> Optional[str]:
    """
    Fetch the wikitext for a film's Wikipedia article.
    Tries several common title formats. Returns wikitext or None.
    Includes retry with exponential backoff to handle rate limiting.
    """
    params = {
        "action": "parse",
        "prop": "wikitext",
        "format": "json",
        "redirects": True,
    }
    for page_title in [f"{title} ({year} film)", f"{title} ({year-1} film)", f"{title} (film)", title]:
        for attempt in range(3):  # up to 3 retries per title format
            try:
                time.sleep(0.5 + attempt)  # 0.5s, 1.5s, 2.5s between attempts
                r = requests.get(
                    "https://en.wikipedia.org/w/api.php",
                    params={**params, "page": page_title},
                    headers={**HEADERS, "Api-User-Agent": "AusFilmScraper/1.0 (film research tool)"},
                    timeout=15,
                )
                if r.status_code == 429:
                    wait = 5 * (attempt + 1)
                    log.warning(f"  Wikipedia rate limited — waiting {wait}s")
                    time.sleep(wait)
                    continue
                if not r.text.strip():
                    log.warning(f"  Wikipedia empty response for '{page_title}', retrying...")
                    time.sleep(2 * (attempt + 1))
                    continue
                data = r.json()
                if "error" in data:
                    break  # page not found, try next title format
                wikitext = data.get("parse", {}).get("wikitext", {}).get("*", "")
                if wikitext:
                    return wikitext
                break
            except ValueError as e:
                # JSON parse error — usually empty response from rate limiting
                log.warning(f"  Wikipedia rate limit likely hit for '{page_title}' (attempt {attempt+1}/3): {e}")
                time.sleep(3 * (attempt + 1))
            except Exception as e:
                log.warning(f"  Wikipedia fetch failed for '{page_title}': {e}")
                break
    return None


def verify_australian_on_wikipedia(title: str, year: int) -> bool:
    """
    Cross-check a film against Wikipedia to confirm it has genuine Australian production.
    Requires BOTH:
      - Australia listed as a production country in the infobox, AND
      - An Australian production company OR funding body present
    If either piece of info is missing from Wikipedia, falls back gracefully.
    Gives benefit of the doubt if no Wikipedia article is found.
    """
    AU_FUNDING_KEYWORDS = [
        "screen australia", "film victoria", "screen nsw", "screen queensland",
        "screen west", "south australian film corporation", "safc",
        "abc film", "abc television", "foxtel", "stan originals",
        "adelaide film festival", "melbourne international film festival",
        "miff", "sydney film festival", "screen tasmania",
        "northern territory screen",
    ]

    wikitext = fetch_wikipedia_film_article(title, year)

    if wikitext is None:
        log.info(f"  ? No Wikipedia article found for '{title}', accepting on TMDB signal")
        return True

    wikitext_lower = wikitext.lower()

    # Check 1: Is Australia listed as a production country in the infobox?
    country_match = re.search(r"[|][^{\n]*countr(?:y|ies)[^{\n]*=([^{\n}|]{1,300})", wikitext, re.IGNORECASE)
    au_in_countries = False
    if country_match:
        country_field = country_match.group(1).lower()
        au_in_countries = "australia" in country_field
        log.info(f"  Infobox country field for '{title}': {'AU found' if au_in_countries else 'AU not found'}")

    # Check 2: Is there an Australian production company in the infobox?
    company_match = re.search(r"[|][^{\n]*(?:production_company|producer|studio)[^{\n]*=([^}{\n]{1,500})", wikitext, re.IGNORECASE)
    au_company_found = False
    if company_match:
        company_field = company_match.group(1).lower()
        for aus_co in AUSTRALIAN_COMPANIES:
            if aus_co in company_field:
                log.info(f"  ✓ Australian company in infobox: '{aus_co}'")
                au_company_found = True
                break

    # If both checks ran and both pass — confirm Australian
    if au_in_countries and au_company_found:
        log.info(f"  ✓ Confirmed Australian: country + company both found for '{title}'")
        return True

    # If country field found Australia but no company field — check funding keywords
    if au_in_countries:
        for kw in AU_FUNDING_KEYWORDS:
            if kw in wikitext_lower:
                log.info(f"  ✓ Confirmed Australian: country field + funding keyword '{kw}' for '{title}'")
                return True
        # Country listed but no other Australian signal — still accept
        log.info(f"  ✓ Australia in country field for '{title}' — accepting")
        return True

    # If company found but no country field match — accept on company alone
    if au_company_found:
        log.info(f"  ✓ Australian company found, accepting '{title}'")
        return True

    # No infobox data found — fall back to funding keywords anywhere in article
    for kw in AU_FUNDING_KEYWORDS:
        if kw in wikitext_lower:
            log.info(f"  ✓ Australian funding keyword found for '{title}': '{kw}'")
            return True

    # Australia mentioned somewhere but no production evidence
    if "australia" in wikitext_lower:
        log.info(f"  ✗ Australia mentioned but no production evidence for '{title}'")
        return False

    log.info(f"  ✗ No Australian production evidence found for '{title}'")
    return False


def verify_not_rerelease(title: str, festival_year: int) -> bool:
    """
    Check whether a film is a re-release of an older film at a festival.
    If Wikipedia shows the film's original release year is more than 2 years
    before the festival year, it's likely a retrospective and we reject it.
    """
    wikitext = fetch_wikipedia_film_article(title, festival_year)
    if wikitext is None:
        return True  # Can't verify, give benefit of the doubt

    # Look for release year in the infobox — e.g. | released = 1994
    # or plain 4-digit years near "release" keywords
    release_patterns = [
        r"\|\s*release[d\s_]*=\s*.*?(\d{4})",
        r"\|\s*released\s*=\s*.*?(\d{4})",
        r"release_date\s*=\s*.*?(\d{4})",
        r"\{\{film date[^}]*?(\d{4})",
        r"\{\{start date[^}]*?(\d{4})",
    ]
    for pattern in release_patterns:
        match = re.search(pattern, wikitext, re.IGNORECASE)
        if match:
            original_year = int(match.group(1))
            age = festival_year - original_year
            if age > 2:
                log.info(f"  ✗ Re-release detected: '{title}' originally released {original_year}, festival year {festival_year} ({age} years gap)")
                return False
            else:
                log.info(f"  ✓ Release year check passed: '{title}' ({original_year})")
                return True

    # No release year found in article — give benefit of the doubt
    return True


def extract_tmdb_data(detail: dict) -> dict:
    directors = sorted([
        m.get("name", "")
        for m in detail.get("credits", {}).get("crew", [])
        if m.get("job") == "Director" and m.get("name")
    ])
    director = ", ".join(directors[:2]) if directors else ""
    # Extract distributors from release_dates
    # AU distributor: note field from Australian release entry
    # International distributor: note field from US release entry (most reliable proxy)
    au_distributor   = ""
    intl_distributor = ""

    for entry in detail.get("release_dates", {}).get("results", []):
        iso = entry.get("iso_3166_1", "")
        for rd in entry.get("release_dates", []):
            note = rd.get("note", "").strip()
            if not note:
                continue
            if iso == "AU" and not au_distributor:
                au_distributor = note
            elif iso == "US" and not intl_distributor:
                intl_distributor = note

    revenue = detail.get("revenue") or None
    budget  = detail.get("budget") or None

    return {
        "tmdb_id":           detail.get("id"),
        "tmdb_rating":       round(detail.get("vote_average") or 0, 1) or None,
        "synopsis":          detail.get("overview", ""),
        "poster_path":       detail.get("poster_path", ""),
        "director":          director,
        "imdb_id":           detail.get("external_ids", {}).get("imdb_id", ""),
        "genres":            [g["name"] for g in detail.get("genres", [])],
        "runtime_mins":      detail.get("runtime") or None,
        "revenue":           revenue,
        "budget":            budget,
        "distributor":       au_distributor,
        "distributor_intl":  intl_distributor,
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

    # Inject seed films as guaranteed candidates — these bypass PDF/Wikipedia discovery
    # but still go through all nationality and quality checks
    for seed_title, seed_year, seed_festivals in SEED_FILMS:
        key = (seed_title.lower(), seed_year)
        if key not in candidates:
            candidates[key] = {"title": seed_title, "year": seed_year, "festivals": []}
            log.info(f"  + Seed film added: {seed_title} ({seed_year})")
        for fest in seed_festivals:
            if fest not in candidates[key]["festivals"]:
                candidates[key]["festivals"].append(fest)

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
    australian_details: list = []  # collect AU films first, enrich after

    for (title_lower, year), info in candidates.items():
        checked += 1
        title = info["title"]

        # Use cache if available and fresh — skip all API calls
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

        # Step 1: Fast TMDB pre-filter — cheap broad check before Wikipedia
        detail = tmdb_search_film(title, year)
        if not detail:
            log.debug(f"  ✗ TMDB: no result for '{title}' ({year})")
            continue

        tmdb_id   = detail.get("id")
        tmdb_title = detail.get("title", "")

        # Blocklist checks — by TMDB ID and by title
        if tmdb_id in BLOCKLIST_TMDB_IDS:
            continue
        if title.lower() in BLOCKLIST_TITLES:
            log.info(f"  ✗ Title blocklisted: '{title}'")
            continue

        # Broad Australian signal from TMDB — pass if ANY of these are true:
        # a) production_countries includes AU
        # b) origin_country includes AU
        # c) a known Australian production company is listed
        prod_countries  = [c.get("iso_3166_1","") for c in detail.get("production_countries", [])]
        origin_countries = detail.get("origin_country", [])
        prod_companies  = [c.get("name","").lower() for c in detail.get("production_companies", [])]

        au_by_country = "AU" in prod_countries or "AU" in origin_countries
        au_by_company = any(
            aus_co in co
            for co in prod_companies
            for aus_co in AUSTRALIAN_COMPANIES
        )

        if not au_by_country and not au_by_company:
            # Log specifically for known films we want to track
            log.debug(
                f"  ✗ TMDB pre-filter rejected '{title}' ({year}) → "
                f"matched '{tmdb_title}' (id={tmdb_id}) | "
                f"countries={prod_countries} origin={origin_countries} | "
                f"companies={prod_companies[:3]}"
            )
            continue

        log.info(f"  ~ TMDB AU signal for '{title}' (matched: '{tmdb_title}') — verifying with Wikipedia")

        # Step 2: Wikipedia nationality confirmation (only for TMDB-shortlisted films)
        if not verify_australian_on_wikipedia(title, year):
            continue

        # Step 3: Re-release check
        if not verify_not_rerelease(title, year):
            continue

        # Step 4: Runtime check
        if not is_feature_film(detail):
            log.info(f"  ✗ Short film: {title} ({detail.get('runtime')} mins)")
            continue

        log.info(f"  ✓ [{checked}/{len(candidates)}] AUSTRALIAN FEATURE: {title} ({year})")
        australian_details.append((info, detail))

    # Now enrich only the Australian films (much smaller set)
    log.info(f"\n── Step 2b: Enriching {len(australian_details)} Australian films ──")
    for info, detail in australian_details:
        title = info["title"]
        year  = info["year"]
        data  = extract_tmdb_data(detail)
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
        log.info(f"  Enriched: {film.title} ({film.year})")

    # Step 4: Screen Australia URL — use what we already fetched in Step 1b
    log.info("\n── Step 3: Applying Screen Australia URLs ──")
    for film in result_films:
        if not film.get("screen_australia_url"):
            key = (film.get("title", "").lower(), film.get("year"))
            sa_entry = sa_entries.get(key)
            if sa_entry and sa_entry.get("screen_australia_url"):
                film["screen_australia_url"] = sa_entry["screen_australia_url"]

    # Step 5: Merge + save
    new_map    = {f["tmdb_id"]: f for f in result_films if f.get("tmdb_id")}
    merged     = list({**existing_map, **new_map}.values())
    merged.sort(key=lambda f: (-(f.get("year") or 0), f.get("title", "")))

    OUTPUT_FILE.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
    new_count = len(set(new_map.keys()) - set(existing_map.keys()))
    log.info(f"\nDone. {len(merged)} total films saved ({new_count} new).")


if __name__ == "__main__":
    run_scraper()
