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

# ── Manual distribution data ──────────────────────────────────────────────────
# Sourced from IMDb company credits pages and trade press (Variety, Deadline, Screen Daily)
# Keyed by TMDB ID — find at themoviedb.org/movie/<ID>
# Fields: sales_agent (intl sales), distributor (AU theatrical), distributor_intl (US/UK theatrical)
# All fields optional — only include what you know
MANUAL_FILM_DATA = {

    # ── Talk to Me (2022) — tt10638522 / tmdb 943822 ──
    # Sales: Bankside Films (worldwide). AU: Umbrella Entertainment. US: A24
    943822: {
        "sales_agent":      "Bankside Films",
        "distributor":      "Umbrella Entertainment",
        "distributor_intl": "A24",
    },

    # ── Birdeater (2023) — tt20674132 / tmdb 1117321 ──
    # AU: Umbrella Entertainment. US: Dark Sky Films. No intl sales agent listed.
    1117321: {
        "distributor":      "Umbrella Entertainment",
        "distributor_intl": "Dark Sky Films",
    },

    # ── Late Night with the Devil (2023) — tt14966898 / tmdb 1029825 ──
    # Sales: AGC International (worldwide). Cinetic Media (US negotiator). AU: no separate AU listed. US/UK: IFC Films / Shudder
    1029825: {
        "sales_agent":      "AGC International",
        "distributor_intl": "IFC Films / Shudder",
    },

    # ── Nitram (2021) — tt13694628 / tmdb 793409 ──
    # Sales: Wild Bunch International (worldwide). AU: Madman Entertainment. US: IFC Films
    793409: {
        "sales_agent":      "Wild Bunch International",
        "distributor":      "Madman Entertainment",
        "distributor_intl": "IFC Films",
    },

    # ── Memoir of a Snail (2024) — tt23770030 / tmdb 1232448 ──
    # Sales: Anton (worldwide) / Charades (worldwide). AU: Sharmill Films. US: MUBI
    1232448: {
        "sales_agent":      "Anton / Charades",
        "distributor_intl": "MUBI",
    },

    # ── Relic (2020) — tt9072352 / tmdb 604155 ──
    # Sales: Film Constellation (intl) / AGBO Films (early). AU: Umbrella Entertainment. US: IFC Midnight
    604155: {
        "sales_agent":      "Film Constellation",
        "distributor":      "Umbrella Entertainment",
        "distributor_intl": "IFC Midnight",
    },

    # ── Babyteeth (2019) — tt8399664 / tmdb 561218 ──
    # Sales: Beta Cinema (Germany, worldwide). AU: Universal Pictures. US: IFC Films
    561218: {
        "sales_agent":      "Beta Cinema",
        "distributor":      "Universal Pictures",
        "distributor_intl": "IFC Films",
    },

    # ── You Won't Be Alone (2022) — tt8296030 / tmdb 806108 ──
    # Sales: Bankside Films (worldwide). AU: Madman Films. US: Focus Features
    806108: {
        "sales_agent":      "Bankside Films",
        "distributor":      "Madman Films",
        "distributor_intl": "Focus Features",
    },

    # ── The New Boy (2023) — tt18180926 / tmdb 952516 ──
    # Sales: The Veterans (worldwide). AU: Roadshow Films. US: Vertical Entertainment
    952516: {
        "sales_agent":      "The Veterans",
        "distributor":      "Roadshow Films",
        "distributor_intl": "Vertical Entertainment",
    },

    # ── Monolith (2022) — tt18298588 / tmdb 1000305 ──
    # Sales: XYZ Films (N. America) / Blue Finch Films (UK). AU: Bonsai Films. US: Well Go USA
    1000305: {
        "sales_agent":      "XYZ Films",
        "distributor":      "Bonsai Films",
        "distributor_intl": "Well Go USA Entertainment",
    },

    # ── The Babadook (2014) — tt2321549 / tmdb 242224 ──
    # Sales: Causeway Films (early). AU: Umbrella Entertainment. US: IFC Midnight
    242224: {
        "sales_agent":      "Causeway Films",
        "distributor":      "Umbrella Entertainment",
        "distributor_intl": "IFC Midnight",
    },

    # ── The Nightingale (2018) — tt7984734 / tmdb 584867 ──
    # Sales: Bankside Films (worldwide). AU: Causeway Films / Umbrella. US: IFC Films
    584867: {
        "sales_agent":      "Bankside Films",
        "distributor":      "Umbrella Entertainment",
        "distributor_intl": "IFC Films",
    },

    # ── Sweet Country (2017) — tt6958212 / tmdb 480041 ──
    # Sales: Memento International. AU: Transmission Films. US: Kino Lorber
    480041: {
        "sales_agent":      "Memento International",
        "distributor":      "Transmission Films",
        "distributor_intl": "Kino Lorber",
    },

    # ── Nitram (2021) alternate TMDB ID check ──
    # (in case TMDB ID differs — leaving both)
    738971: {
        "sales_agent":      "Wild Bunch International",
        "distributor":      "Madman Entertainment",
        "distributor_intl": "IFC Films",
    },

    # ── Animal Kingdom (2010) — tt1313092 / tmdb 39254 ──
    # Sales: Memento Films International. AU: Madman Entertainment. US: Sony Pictures Classics
    39254: {
        "sales_agent":      "Memento Films International",
        "distributor":      "Madman Entertainment",
        "distributor_intl": "Sony Pictures Classics",
    },

    # ── Samson & Delilah (2009) — tt1340123 / tmdb 34772 ──
    # Sales: Memento Films International. AU: Madman Entertainment. US: Kino Lorber
    34772: {
        "sales_agent":      "Memento Films International",
        "distributor":      "Madman Entertainment",
        "distributor_intl": "Kino Lorber",
    },

    # ════════════════════════════════════════════════════════
    # POST-2015 FILMS — sourced from Variety/Deadline/Screen Daily
    # ════════════════════════════════════════════════════════

    # ── Furiosa: A Mad Max Saga (2024) — tt12037194 / tmdb 718821 ──
    # Sales: Rocket Science (intl). AU: Roadshow Films. US: Paramount Pictures
    718821: {
        "sales_agent":      "Rocket Science",
        "distributor":      "Roadshow Films",
        "distributor_intl": "Paramount Pictures",
    },

    # ── Better Man (2024) — tt14208742 / tmdb 1064213 ──
    # Sales: Rocket Science (intl). AU: Roadshow Films. US: Paramount Pictures
    1064213: {
        "sales_agent":      "Rocket Science",
        "distributor":      "Roadshow Films",
        "distributor_intl": "Paramount Pictures",
    },

    # ── Went Up the Hill (2024) — tt14303268 / tmdb 1219902 ──
    # Sales: Bankside Films / CAA Media Finance. AU: (TBC). US: Greenwich Entertainment
    1219902: {
        "sales_agent":      "Bankside Films",
        "distributor_intl": "Greenwich Entertainment",
    },

    # ── Every Little Thing (2024) — tt29340714 / tmdb 1367014 ──
    # Sales: Dogwoof. AU: (streaming). US: Kino Lorber
    1367014: {
        "sales_agent":      "Dogwoof",
        "distributor_intl": "Kino Lorber",
    },

    # ── The Moogai (2024) — tt21328456 / tmdb 1359671 ──
    # Sales: Bankside Films. US: Samuel Goldwyn Films
    1359671: {
        "sales_agent":      "Bankside Films",
        "distributor_intl": "Samuel Goldwyn Films",
    },

    # ── Audrey (2024) — tt10939802 / tmdb 1133751 ──
    # AU: Rialto Distribution. UK: Vertigo Releasing. US: Sunrise Films
    1133751: {
        "distributor":      "Rialto Distribution",
        "distributor_intl": "Sunrise Films",
    },

    # ── Together (2025) — tt31806461 / tmdb 1370637 ──
    # Sales: WME Independent (worldwide). US/worldwide: Neon
    1370637: {
        "sales_agent":      "WME Independent",
        "distributor_intl": "Neon",
    },

    # ── We Bury the Dead (2025) — tt15397070 / tmdb 1233075 ──
    # Sales: Neon International. AU: Umbrella Entertainment (exec). US: Vertical Entertainment. UK: Signature Entertainment
    1233075: {
        "sales_agent":      "Neon International",
        "distributor":      "Umbrella Entertainment",
        "distributor_intl": "Vertical Entertainment",
    },

    # ── Dangerous Animals (2025) — tt32299316 / tmdb 1388417 ──
    # Sales: LD Entertainment / Range Select. US: IFC Films / Shudder
    1388417: {
        "sales_agent":      "LD Entertainment",
        "distributor_intl": "IFC Films / Shudder",
    },

    # ── Deeper (2025) — tt34546353 / tmdb 1399060 ──
    # Sales: Dogwoof. Worldwide: Netflix
    1399060: {
        "sales_agent":      "Dogwoof",
        "distributor_intl": "Netflix",
    },

    # ── The Royal Hotel (2023) — tt15072632 / tmdb 927107 ──
    # Sales: HanWay Films / Cross City Films. AU: Transmission Films. US: Neon
    927107: {
        "sales_agent":      "HanWay Films",
        "distributor":      "Transmission Films",
        "distributor_intl": "Neon",
    },

    # ── Shayda (2023) — tt20903900 / tmdb 1040982 ──
    # Sales: HanWay Films. AU: Madman Entertainment. US: Sony Pictures Classics
    1040982: {
        "sales_agent":      "HanWay Films",
        "distributor":      "Madman Entertainment",
        "distributor_intl": "Sony Pictures Classics",
    },

    # ── Run Rabbit Run (2023) — tt12547822 / tmdb 969946 ──
    # Sales: XYZ Films. Worldwide: Netflix
    969946: {
        "sales_agent":      "XYZ Films",
        "distributor_intl": "Netflix",
    },

    # ── Add more films below following the same pattern ──
    # Find TMDB ID at themoviedb.org/movie/<ID>
    # Source distributor info from imdb.com/title/<imdb_id>/companycredits
    # tmdb_id: {"sales_agent": "...", "distributor": "...", "distributor_intl": "..."},
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
    sales_agent: str = ""
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
        # The page uses <li> elements containing:
        #   <h3> for year headings  OR
        #   <h4><a>Title</a></h4> + <a>Festival link</a> for film entries
        current_year = None
        for li in soup.find_all("li"):
            h3 = li.find("h3")
            if h3:
                try:
                    current_year = int(h3.get_text(strip=True))
                except ValueError:
                    pass
                continue

            h4 = li.find("h4")
            if not h4 or not current_year or current_year not in years:
                continue

            # Title is the first link (inside h4)
            all_links = li.find_all("a")
            if not all_links:
                continue

            title_link = all_links[0]
            title = title_link.get_text(strip=True)
            sa_url = title_link.get("href", "")
            if sa_url and not sa_url.startswith("http"):
                sa_url = "https://www.screenaustralia.gov.au" + sa_url

            # Festival is the second link (sibling of h4 inside li)
            if len(all_links) < 2:
                continue
            fest_link = all_links[1]
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
            "append_to_response": "credits,external_ids,release_dates,watch/providers",
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
    # ── Distributors from watch/providers ──
    # TMDB watch/providers gives us the actual streaming/theatrical distributors
    # per country — much more reliable than the release_dates note field
    au_distributor   = ""
    intl_distributor = ""
    sales_agent      = ""

    providers = detail.get("watch/providers", {}).get("results", {})

    # AU distributor — flatrate (streaming) or rent/buy (theatrical) providers for AU
    au_providers = providers.get("AU", {})
    for ptype in ("flatrate", "rent", "buy"):
        for p in au_providers.get(ptype, []):
            name = p.get("provider_name", "").strip()
            if name and not au_distributor:
                au_distributor = name
                break
        if au_distributor:
            break

    # Intl distributor — check US first, then GB as proxy
    for iso in ("US", "GB"):
        region_providers = providers.get(iso, {})
        for ptype in ("flatrate", "rent", "buy"):
            for p in region_providers.get(ptype, []):
                name = p.get("provider_name", "").strip()
                if name and not intl_distributor:
                    intl_distributor = name
                    break
            if intl_distributor:
                break
        if intl_distributor:
            break

    # Sales agent — production companies that are known sales agents
    # These are typically smaller companies with "sales", "intl", or "world" in name
    SALES_KEYWORDS = {
        "sales", "world sales", "intl sales", "international sales",
        "mk2", "films boutique", "wild bunch", "memento", "vision films",
        "alchemy", "protagonist", "sierra/affinity", "endeavor content",
        "cornerstone", "kinology", "hanway", "studiocanal intl"
    }
    EXCLUDE_WORDS = {
        "australia", "australian", "production", "studio", "pictures",
        "entertainment", "media", "screen"
    }
    for company in detail.get("production_companies", []):
        name = company.get("name", "").strip()
        name_lower = name.lower()
        if (any(kw in name_lower for kw in SALES_KEYWORDS) and
                not any(ex in name_lower for ex in EXCLUDE_WORDS)):
            if not sales_agent:
                sales_agent = name
                break

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
        "sales_agent":       sales_agent,
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

def fetch_imdb_company_credits(imdb_id: str) -> dict:
    """
    Scrape IMDb company credits page for distributor and sales agent info.
    Returns dict with keys: sales_agent, distributor, distributor_intl.
    Falls back gracefully to empty dict if blocked or unavailable.
    """
    if not imdb_id:
        return {}
    try:
        url = f"https://www.imdb.com/title/{imdb_id}/companycredits"
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return {}

        soup = BeautifulSoup(r.text, "html.parser")
        result = {}

        # IMDb company credits page has sections with id="distributors" and id="sales"
        # Each section is an <h4> followed by a <ul> of companies with territory notes

        AU_TERMS  = {"australia", "aus", "au"}
        US_TERMS  = {"united states", "usa", "u.s.", "north america"}
        WORLD_TERMS = {"world", "worldwide", "international", "all territories"}
        SALES_SECTION_IDS = {"sales", "miscellaneous"}

        def clean(text):
            # Strip territory/year notes in parentheses — e.g. "(Australia, 2022)(theatrical)"
            return re.sub(r"\s*\(.*?\)", "", text).strip()

        def note_lower(li):
            return li.get_text(" ", strip=True).lower()

        sections = {}
        for h4 in soup.find_all("h4"):
            sid = h4.get("id", "").lower()
            ul = h4.find_next_sibling("ul")
            if ul:
                sections[sid] = ul

        # ── Distributors ──
        dist_ul = sections.get("distributors")
        if dist_ul:
            for li in dist_ul.find_all("li"):
                text_lower = note_lower(li)
                name_tag = li.find("a") or li
                name = clean(name_tag.get_text(strip=True) if li.find("a") else li.get_text(strip=True))
                if not name:
                    continue
                if any(t in text_lower for t in AU_TERMS) and not result.get("distributor"):
                    result["distributor"] = name
                elif (any(t in text_lower for t in US_TERMS) or
                      any(t in text_lower for t in WORLD_TERMS)) and not result.get("distributor_intl"):
                    result["distributor_intl"] = name

        # ── Sales agents ──
        for sid in ("sales", "miscellaneous"):
            sales_ul = sections.get(sid)
            if sales_ul and not result.get("sales_agent"):
                for li in sales_ul.find_all("li"):
                    text_lower = note_lower(li)
                    # Only pick entries explicitly flagged as worldwide/international sales
                    if any(t in text_lower for t in WORLD_TERMS) or "sales" in text_lower:
                        name_tag = li.find("a") or li
                        name = clean(name_tag.get_text(strip=True))
                        if name:
                            result["sales_agent"] = name
                            break

        return result

    except Exception as e:
        log.debug(f"  IMDb company credits fetch failed for {imdb_id}: {e}")
        return {}


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

    # ── Step 1: Screen Australia — primary definitive source ─────────────────
    log.info("\n── Step 1: Fetching Screen Australia definitive festival list ──")
    candidates: dict = {}  # (title_lower, year) → {title, year, festivals: [], screen_australia_url}
    sa_entries = scrape_screen_australia_festivals(years)
    for key, sa_data in sa_entries.items():
        candidates[key] = {
            "title":                sa_data["title"],
            "year":                 sa_data["year"],
            "festivals":            list(sa_data["festivals"]),
            "screen_australia_url": sa_data.get("screen_australia_url", ""),
        }
    log.info(f"Screen Australia: {len(candidates)} candidates loaded")

    # ── Step 1b: Wikipedia + PDFs — add extra festivals to SA films only ──────
    # We don't use Wikipedia to discover new films — only to find additional
    # festival appearances for films already confirmed by Screen Australia.
    log.info("\n── Step 1b: Adding extra festival data from Wikipedia / PDFs ──")
    wiki_titles: dict = {}  # (title_lower, year) → set of festivals
    for festival in WIKI_TEMPLATES.keys():
        films = get_festival_films(festival, years)
        for f in films:
            key = (f["title"].lower(), f["year"])
            if key not in wiki_titles:
                wiki_titles[key] = set()
            wiki_titles[key].add(f["festival"])

    # Merge wiki festivals — add to SA candidates, or promote to candidate
    # if SA fetch failed (sa_entries empty) so we always have something
    sa_failed = len(sa_entries) == 0
    wiki_added = 0
    wiki_new = 0
    for key, festivals in wiki_titles.items():
        if key in candidates:
            for fest in festivals:
                if fest not in candidates[key]["festivals"]:
                    candidates[key]["festivals"].append(fest)
                    wiki_added += 1
        elif sa_failed:
            # SA returned nothing — fall back to Wikipedia as discovery source
            title, year = key[0], key[1]
            candidates[key] = {"title": title, "year": year, "festivals": list(festivals)}
            wiki_new += 1
    if sa_failed:
        log.warning(f"Screen Australia returned 0 results — using Wikipedia as fallback ({wiki_new} candidates)")
    else:
        log.info(f"Wikipedia/PDFs added {wiki_added} extra festival tags to existing SA films")

    # ── Seed films — guaranteed candidates ───────────────────────────────────
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

    # Step 2: TMDB enrichment — Screen Australia guarantees nationality,
    # so we just look up metadata (poster, rating, runtime etc.) and
    # skip the slow Wikipedia nationality verification entirely.
    log.info("\n── Step 2: Enriching candidates via TMDB ──")
    field_names = {f.name for f in datafields(Film)}
    result_films: list = []
    checked = 0
    australian_details: list = []

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
                    if info.get("screen_australia_url") and not cached.get("screen_australia_url"):
                        cached["screen_australia_url"] = info["screen_australia_url"]
                    result_films.append(cached)
                    continue
            except Exception:
                pass

        # Search TMDB for metadata
        detail = tmdb_search_film(title, year)
        if not detail:
            log.warning(f"  ✗ TMDB: no result for '{title}' ({year}) — skipping")
            continue

        tmdb_id = detail.get("id")

        # Blocklist checks
        if tmdb_id in BLOCKLIST_TMDB_IDS:
            log.info(f"  ✗ TMDB ID blocklisted: '{title}'")
            continue
        if title.lower() in BLOCKLIST_TITLES:
            log.info(f"  ✗ Title blocklisted: '{title}'")
            continue

        # Skip short films — runtime sanity check still useful
        if not is_feature_film(detail):
            log.info(f"  ✗ Short film: {title} ({detail.get('runtime')} mins)")
            continue

        # Re-release check — skip if the film originally released more than 10 years
        # before the festival year. This catches retrospectives like Muriel's Wedding (1994)
        # appearing at Berlin 2023. We allow a 10-year window to accommodate genuine
        # re-releases and anniversary screenings of recent films.
        release_date = detail.get("release_date", "")
        if release_date:
            try:
                original_year = int(release_date[:4])
                gap = year - original_year
                if gap > 10:
                    log.info(f"  ✗ Re-release: '{title}' originally {original_year}, festival year {year} ({gap}yr gap)")
                    continue
            except (ValueError, TypeError):
                pass

        log.info(f"  ✓ [{checked}/{len(candidates)}] {title} ({year})")
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

        # ── Distribution data priority: manual > IMDb scrape > TMDB watch/providers ──
        # Build the Film object first from TMDB data (watch/providers as base fallback)
        film_field_names = {f.name for f in datafields(Film)}
        film = Film(
            title=title,
            year=year,
            festivals=info["festivals"],
            screen_australia_url=info.get("screen_australia_url", ""),
            poster_url=poster_url,
            **{k: v for k, v in data.items() if k in film_field_names},
        )

        if film.imdb_id:
            film.imdb_rating = fetch_imdb_rating(film.imdb_id)
            # IMDb company credits override TMDB watch/providers for dist fields
            cc = fetch_imdb_company_credits(film.imdb_id)
            if cc:
                if cc.get("sales_agent"):      film.sales_agent      = cc["sales_agent"]
                if cc.get("distributor"):      film.distributor      = cc["distributor"]
                if cc.get("distributor_intl"): film.distributor_intl = cc["distributor_intl"]
                if any(cc.values()):
                    log.info(f"  IMDb credits: {cc}")
            time.sleep(1)

        # Manual data wins over everything — applied last, unconditionally
        manual = MANUAL_FILM_DATA.get(film.tmdb_id)
        if manual:
            dist_fields = {"sales_agent", "distributor", "distributor_intl"}
            for field, val in manual.items():
                if field in dist_fields and val:
                    setattr(film, field, val)
            log.info(f"  Manual override: {manual}")

        lb = fetch_letterboxd_data(film.title, film.year)
        film.letterboxd_rating = lb["letterboxd_rating"]
        film.letterboxd_url    = lb["letterboxd_url"]
        time.sleep(0.5)

        result_films.append(asdict(film))
        log.info(f"  Enriched: {film.title} ({film.year})")

    # Step 4: Apply Screen Australia URLs to any films not already stamped
    log.info("\n── Step 3: Applying Screen Australia URLs ──")
    for film in result_films:
        if not film.get("screen_australia_url"):
            key = (film.get("title", "").lower(), film.get("year"))
            sa_match = sa_entries.get(key)
            if sa_match and sa_match.get("screen_australia_url"):
                film["screen_australia_url"] = sa_match["screen_australia_url"]
        # Also carry SA URL from candidates dict (already fetched in Step 1a)
        if not film.get("screen_australia_url"):
            key = (film.get("title", "").lower(), film.get("year"))
            cand = candidates.get(key, {})
            if cand.get("screen_australia_url"):
                film["screen_australia_url"] = cand["screen_australia_url"]

    # Step 5: Merge + save
    new_map    = {f["tmdb_id"]: f for f in result_films if f.get("tmdb_id")}
    merged     = list({**existing_map, **new_map}.values())
    merged.sort(key=lambda f: (-(f.get("year") or 0), f.get("title", "")))

    OUTPUT_FILE.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
    new_count = len(set(new_map.keys()) - set(existing_map.keys()))
    log.info(f"\nDone. {len(merged)} total films saved ({new_count} new).")


if __name__ == "__main__":
    run_scraper()
