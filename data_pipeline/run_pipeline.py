"""Read the two CSVs, transform them, and load everything into Postgres.

Run from the repo root:  python data_pipeline/run_pipeline.py
"""

from pathlib import Path

import pandas as pd

from load import empty_tables, get_engine, load_table, run_schema
from reference_data import COMPETITIONS, SEASONS
from transform import build_clubs, clean_team_seasons, clean_uefa_matches

RAW_DIR = Path(__file__).resolve().parent.parent / "raw_data"
TEAMS_CSV = RAW_DIR / "team_season_records (2).csv"
UEFA_CSV = RAW_DIR / "uefacompetitionresults (1).csv"


def main() -> None:
    teams_raw = pd.read_csv(TEAMS_CSV)
    uefa_raw = pd.read_csv(UEFA_CSV)

    # Transform everything first, so a data problem stops the run before the database is touched.
    clubs = build_clubs(teams_raw, uefa_raw)
    tables = {
        "season": pd.DataFrame(SEASONS),
        "club": clubs,
        "competition": pd.DataFrame(COMPETITIONS).astype({"strength_factor": "float64"}),  # empty for now, but numeric
        "team_season": clean_team_seasons(teams_raw, clubs),
        "uefa_match": clean_uefa_matches(uefa_raw, clubs),
    }

    # One transaction: if anything fails, the database is left as it was.
    with get_engine().begin() as conn:
        run_schema(conn)
        empty_tables(conn)
        for table, df in tables.items():  # parents before children
            load_table(conn, table, df)


if __name__ == "__main__":
    main()
