"""Extract English clubs' European campaigns from Wikipedia's per-season UEFA competition pages.

Every page (e.g. "2003–04 UEFA Cup") flags clubs with their country icon ({{fbaicon|ENG}}) and is
split into sections in tournament order (qualifying, rounds, groups, knockouts, final). A club's
furthest section is the round it reached; the infobox gives the winners and runners-up.
Content is CC BY-SA 4.0 (Wikipedia).
"""

import json
import re
import time
from collections.abc import Callable
from pathlib import Path
from urllib.parse import quote

import requests

from ingestion.sources import FIRST_SEASON_START, fetch_text, season_label

API_URL = "https://en.wikipedia.org/w/api.php?action=parse&format=json&prop=wikitext&redirects=1&page={title}"
PAGE_URL = "https://en.wikipedia.org/wiki/{title}"

# (competition name, first season start year, last season start year, page title templates).
# Titles changed over time, so each template is tried in turn; redirects are followed.
COMPETITIONS = [
    ("European Cup / Champions League", 1992, 2100,
     ["{s} UEFA Champions League", "{s} European Cup"]),
    ("UEFA Cup", 1992, 2008, ["{s} UEFA Cup"]),
    ("UEFA Europa League", 2009, 2100, ["{s} UEFA Europa League"]),
    ("Cup Winners' Cup", 1992, 1998,
     ["{s} UEFA Cup Winners' Cup", "{s} European Cup Winners' Cup"]),
    ("UEFA Conference League", 2021, 2100,
     ["{s} UEFA Conference League", "{s} UEFA Europa Conference League"]),
]

HEADING_RE = re.compile(r"^(={2,5})\s*(.+?)\s*\1\s*$", flags=re.M)
ENG_CLUB_RE = re.compile(
    r"\{\{[^{}|]*\|ENG[^{}]*\}\}\s*(?:'''|'')?\s*\[\[([^\]|#]+)(?:#[^\]|]*)?(?:\|[^\]]+)?\]\]"
)
# Sub-pages list the country code in the cell after the club link: [[Newcastle United F.C.|Newcastle]]|ENG|
ENG_CELL_RE = re.compile(r"\[\[([^\]|#]+)(?:#[^\]|]*)?(?:\|[^\]]+)?\]\]'*\s*\|\s*ENG(?![A-Za-z])")
LST_RE = re.compile(r"\{\{#lst:([^|}]+)\|([^|}]+)\}\}")
MAIN_RE = re.compile(r"\{\{main\|([^|}]+)", flags=re.I)
CLUB_SUFFIX_RE = re.compile(r"\s+(?:A\.?F\.?C\.?|F\.?C\.?)$")

ROUND_NAMES = {"first": "R1", "second": "R2", "third": "R3", "fourth": "R4", "fifth": "R5"}


def season_title(start_year: int) -> str:
    """Wikipedia's season string, e.g. 1999–2000 or 2003–04."""
    end = start_year + 1
    return f"{start_year}–{end}" if start_year == 1999 else f"{start_year}–{end % 100:02d}"


def club_name(link_target: str) -> str:
    return CLUB_SUFFIX_RE.sub("", link_target.strip())


def english_clubs(text: str) -> set[str]:
    matches = list(ENG_CLUB_RE.finditer(text)) + list(ENG_CELL_RE.finditer(text))
    return {club_name(m.group(1)) for m in matches}


def stage_for(heading: str) -> str | None:
    """Map a section heading to a stage label. None means the section isn't a stage."""
    h = heading.lower()
    h = re.sub(r"\[\[|\]\]|<[^>]*>|&nbsp;", " ", h).strip()
    if h in ("teams", "participants", "clubs"):
        return "Entered"
    if re.search(r"final phase|knockout phase$|bracket|^knockout stage$", h):
        return None
    if "knockout" in h and "play-off" in h:
        return "KO play-off"
    if "second group" in h or "second league" in h or "second phase" in h:
        return "Second group stage"
    if "group" in h or "league phase" in h or "league stage" in h:
        return "Group stage"
    if "qualifying" in h or "preliminary" in h or "play-off" in h:
        return "Qualifying"
    for word, label in ROUND_NAMES.items():
        if h.startswith(word + " round"):
            return label
    if "round of 32" in h:
        return "R32"
    if "round of 16" in h:
        return "R16"
    if "quarter" in h:
        return "QF"
    if "semi" in h:
        return "SF"
    if re.fullmatch(r"(the )?final", h):
        return "F"
    return None


def infobox_finalists(wikitext: str) -> tuple[set[str], set[str]]:
    """English winners and runners-up named in the infobox."""
    winners, runners_up = set(), set()
    for line in wikitext.split("\n")[:60]:
        key = line.split("=", 1)[0].strip(" |").lower()
        if key.startswith("champion"):
            winners |= english_clubs(line)
        elif key.startswith(("second", "runner")):
            runners_up |= english_clubs(line)
    return winners, runners_up


def labelled_section(text: str, label: str) -> str | None:
    """Text between <section begin=label /> and <section end=label /> (possibly several blocks)."""
    pattern = rf"<section begin={re.escape(label.strip())} ?/>(.*?)<section end={re.escape(label.strip())} ?/>"
    blocks = re.findall(pattern, text, flags=re.S)
    return "\n".join(blocks) if blocks else None


def resolve_section(section: str, load: Callable[[str], str | None], problems: list[str]) -> str:
    """Section text plus the sub-page text it transcludes ({{#lst:...}} or {{main|...}})."""
    parts = [section]
    transcluded = LST_RE.findall(section)
    if transcluded:
        for title, label in transcluded:
            page = load(title.strip())
            if page is None:
                problems.append(f"missing sub-page '{title.strip()}'")
                continue
            extract = labelled_section(page, label)
            if extract is None:
                problems.append(f"no section '{label.strip()}' in '{title.strip()}', used whole page")
            parts.append(extract if extract is not None else page)
    else:
        for title in MAIN_RE.findall(section):
            page = load(title.strip())
            if page is None:
                problems.append(f"missing sub-page '{title.strip()}'")
            else:
                parts.append(page)
    return "\n".join(parts)


def parse_season_page(
    wikitext: str, load: Callable[[str], str | None], problems: list[str]
) -> dict[str, dict]:
    """Return {club: {"round_reached": label, "stage_heading": heading}} for English clubs."""
    reached: dict[str, dict] = {}
    headings = list(HEADING_RE.finditer(wikitext))
    for i, m in enumerate(headings):
        stage = stage_for(m.group(2))
        if stage is None:
            continue
        end = headings[i + 1].start() if i + 1 < len(headings) else len(wikitext)
        text = resolve_section(wikitext[m.end():end], load, problems)
        for club in english_clubs(text):
            # Sections run in tournament order, so a later section overwrites an earlier one.
            reached[club] = {"round_reached": stage, "stage_heading": m.group(2)}
    winners, runners_up = infobox_finalists(wikitext)
    for club in winners:
        reached[club] = {"round_reached": "Winners", "stage_heading": "Final"}
    for club in runners_up:
        reached[club] = {"round_reached": "Runners-up", "stage_heading": "Final"}
    return reached


def fetch_wikitext(title: str, session: requests.Session, cache_dir: Path | None, delay: float) -> str | None:
    """Fetch a page's wikitext, using an on-disk cache so reruns don't hammer Wikipedia."""
    cache_file = cache_dir / f"{re.sub(r'[^A-Za-z0-9]+', '_', title)}.txt" if cache_dir else None
    if cache_file and cache_file.exists():
        return cache_file.read_text(encoding="utf-8")
    text = fetch_text(API_URL.format(title=quote(title, safe="")), session)
    time.sleep(delay)
    if text is None or '"error"' in text[:200]:
        return None
    wikitext = json.loads(text)["parse"]["wikitext"]["*"]
    if cache_file:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(wikitext, encoding="utf-8")
    return wikitext


def extract_european_campaigns(
    session: requests.Session, last_season_start: int, cache_dir: Path | None = None, delay: float = 1.2
) -> tuple[list[dict], list[str]]:
    """Return (campaign rows, problems) for every English club in every UEFA season page."""
    campaigns: list[dict] = []
    problems: list[str] = []
    for competition, first, last, templates in COMPETITIONS:
        for start_year in range(max(first, FIRST_SEASON_START), min(last, last_season_start) + 1):
            label = season_label(start_year)
            for template in templates:
                title = template.format(s=season_title(start_year))
                wikitext = fetch_wikitext(title, session, cache_dir, delay)
                if wikitext is not None:
                    break
            else:
                problems.append(f"{competition} {label}: no Wikipedia page found")
                continue
            load = lambda t: fetch_wikitext(t, session, cache_dir, delay)  # noqa: E731
            page_problems: list[str] = []
            clubs = parse_season_page(wikitext, load, page_problems)
            problems.extend(f"{competition} {label}: {p}" for p in page_problems)
            source_url = PAGE_URL.format(title=quote(title.replace(" ", "_"), safe="_.,()'"))
            campaigns.extend(
                {"club": club, "season": label, "competition": competition, **info, "source_url": source_url}
                for club, info in sorted(clubs.items())
            )
    return campaigns, problems
