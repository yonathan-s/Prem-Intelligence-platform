"""Extract and light-parse the raw data sources used by the platform.

Each extractor returns a list of plain dicts. Field names from the source are kept
as-is (this is the "bronze" layer); we only add `season` and ISO `date`.
"""

import csv
import io
import time
from datetime import date, datetime

import requests

FIRST_SEASON_START = 1992  # 1992/93, first Premier League season

FOOTBALL_DATA_URL = "https://football-data.co.uk/mmz4281/{code}/E0.csv"
ENGSOCCERDATA_URL = "https://raw.githubusercontent.com/jalapic/engsoccerdata/master/data-raw/{name}.csv"

# Season lengths: 42 matches per team until 1994/95, 38 afterwards.
def matches_per_team(start_year: int) -> int:
    return 42 if start_year <= 1994 else 38


def season_label(start_year: int) -> str:
    return f"{start_year}/{(start_year + 1) % 100:02d}"


def fetch_text(url: str, session: requests.Session, retries: int = 4) -> str | None:
    """GET a text file. Returns None on 404; retries on 429/5xx with backoff."""
    for attempt in range(retries):
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        if resp.status_code == 429 or resp.status_code >= 500:
            time.sleep(2**attempt)
            continue
        resp.raise_for_status()
        try:
            return resp.content.decode("utf-8-sig")
        except UnicodeDecodeError:
            return resp.content.decode("latin-1")
    raise RuntimeError(f"Giving up on {url} after {retries} attempts")


def parse_football_data_date(value: str) -> str | None:
    """football-data uses dd/mm/yy (older seasons) or dd/mm/yyyy."""
    value = value.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            parsed = datetime.strptime(value, fmt).date()
        except ValueError:
            continue
        # %y maps 69-99 -> 19xx and 00-68 -> 20xx, which is right for 1993+.
        return parsed.isoformat()
    return None


def clean_row(row: dict) -> dict:
    """Drop blank columns/values and strip whitespace; keep everything else untouched."""
    return {
        k.strip(): v.strip()
        for k, v in row.items()
        if k and k.strip() and v is not None and v.strip() != ""
    }


def parse_football_data_csv(text: str, start_year: int) -> list[dict]:
    matches = []
    for row in csv.DictReader(io.StringIO(text)):
        row = clean_row(row)
        if "HomeTeam" not in row or "AwayTeam" not in row:  # trailing blank rows
            continue
        row["season"] = season_label(start_year)
        row["date_iso"] = parse_football_data_date(row.get("Date", ""))
        matches.append(row)
    return matches


def extract_football_data(session: requests.Session, delay: float = 0.5) -> dict[str, list[dict]]:
    """Premier League results, shots, cards and odds from football-data.co.uk (1993/94 onward)."""
    seasons: dict[str, list[dict]] = {}
    start_year = 1993
    while start_year <= date.today().year:
        code = f"{start_year % 100:02d}{(start_year + 1) % 100:02d}"
        text = fetch_text(FOOTBALL_DATA_URL.format(code=code), session)
        if text is None:
            break  # first season that doesn't exist yet
        matches = parse_football_data_csv(text, start_year)
        if matches:
            seasons[season_label(start_year)] = matches
        start_year += 1
        time.sleep(delay)
    return seasons


def parse_engsoccerdata_csv(text: str, min_season: int = FIRST_SEASON_START) -> list[dict]:
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        row = {k: (None if v == "NA" else v) for k, v in row.items()}
        if int(row["Season"]) < min_season:
            continue
        rows.append(row)
    return rows


def extract_engsoccerdata_england(session: requests.Session) -> list[dict]:
    """Top-flight English results from engsoccerdata; fills the 1992/93 gap."""
    text = fetch_text(ENGSOCCERDATA_URL.format(name="england"), session)
    if text is None:
        raise RuntimeError("engsoccerdata england.csv not found")
    rows = [r for r in parse_engsoccerdata_csv(text) if r["tier"] == "1"]
    for r in rows:
        r["season_label"] = season_label(int(r["Season"]))
    return rows


def extract_engsoccerdata_champions_league(session: requests.Session) -> list[dict]:
    """European Cup / Champions League matches from engsoccerdata (ends 2017/18)."""
    text = fetch_text(ENGSOCCERDATA_URL.format(name="champs"), session)
    if text is None:
        raise RuntimeError("engsoccerdata champs.csv not found")
    rows = parse_engsoccerdata_csv(text)
    for r in rows:
        r["season_label"] = season_label(int(r["Season"]))
    return rows
