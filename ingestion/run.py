"""Run the ingestion pipeline: extract all sources, validate, write one JSON file.

    python -m ingestion.run [--out data/raw/premier_league_raw.json]
"""

import argparse
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import requests

from ingestion import sources

log = logging.getLogger("ingestion")

DEFAULT_OUT = Path("data/raw/premier_league_raw.json")


def validate_football_data(seasons: dict[str, list[dict]]) -> list[str]:
    """Return a list of human-readable problems (empty list = all good)."""
    problems = []
    for label, matches in seasons.items():
        start_year = int(label[:4])
        teams = {m["HomeTeam"] for m in matches} | {m["AwayTeam"] for m in matches}
        expected_teams = 22 if start_year <= 1994 else 20
        expected = expected_teams * (expected_teams - 1)
        # The current season is incomplete, so only flag too many matches there.
        is_last = label == max(seasons)
        if len(matches) > expected or (len(matches) < expected and not is_last):
            problems.append(f"{label}: {len(matches)} matches, expected {expected}")
        if len(teams) > expected_teams:
            problems.append(f"{label}: {len(teams)} teams, expected {expected_teams}")
        if any(m["date_iso"] is None for m in matches):
            problems.append(f"{label}: unparseable dates")
    return problems


def build_dataset(session: requests.Session) -> dict:
    log.info("football-data.co.uk: Premier League results (1993/94 onward)")
    football_data = sources.extract_football_data(session)
    log.info("engsoccerdata: top-flight English results")
    england = sources.extract_engsoccerdata_england(session)
    log.info("engsoccerdata: European Cup / Champions League")
    champions_league = sources.extract_engsoccerdata_champions_league(session)

    problems = validate_football_data(football_data)
    for p in problems:
        log.warning("validation: %s", p)

    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "sources": {
                "football_data": {
                    "url": "https://www.football-data.co.uk/englandm.php",
                    "coverage": f"{min(football_data)} to {max(football_data)}",
                },
                "engsoccerdata": {
                    "url": "https://github.com/jalapic/engsoccerdata",
                    "license": "Free for non-commercial use; credit James Curley",
                },
            },
            "counts": {
                "football_data_matches": sum(len(m) for m in football_data.values()),
                "engsoccerdata_england_matches": len(england),
                "champions_league_matches": len(champions_league),
            },
            "matches_per_team": {
                sources.season_label(y): sources.matches_per_team(y)
                for y in range(sources.FIRST_SEASON_START, int(max(football_data)[:4]) + 1)
            },
            "validation_problems": problems,
        },
        "football_data_matches": football_data,
        "engsoccerdata_england_matches": england,
        "champions_league_matches": champions_league,
    }


def write_json_atomic(data: dict, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=out.parent, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.replace(tmp, out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with requests.Session() as session:
        session.headers["User-Agent"] = "prem-intelligence-platform/0.1 (personal research project)"
        dataset = build_dataset(session)

    write_json_atomic(dataset, args.out)
    log.info("Wrote %s (%s)", args.out, dataset["metadata"]["counts"])


if __name__ == "__main__":
    main()
