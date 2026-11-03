"""Print a first look at the two raw CSVs. Read-only: it writes nothing."""

from pathlib import Path

import pandas as pd

DATA_DIR = Path("../raw_data")
TEAMS_CSV = DATA_DIR / "team_season_records (2).csv"
UEFA_CSV = DATA_DIR / "uefacompetitionresults (1).csv"


def show_overview(name: str, df: pd.DataFrame, season_col: str) -> None:
    """Column names, first 5 rows, and the season range (raw text and parsed start year)."""
    print(f"\n{'=' * 70}\n{name}: {len(df)} rows\n{'=' * 70}")
    print("\nColumns:", df.columns.tolist())
    print("\nFirst 5 rows:")
    print(df.head(5).to_string())

    seasons = df[season_col]
    start_years = seasons.str[:4].astype(int)  # "1992-93" and "1992/1993" both start with 1992
    print(f"\n{season_col} as written:   min={seasons.min()!r}  max={seasons.max()!r}")
    print(f"{season_col} start year:   min={start_years.min()}  max={start_years.max()}")


def show_distinct(df: pd.DataFrame, column: str) -> None:
    """Every distinct value in a column, with how many rows use it."""
    print(f"\nDistinct values in {column!r}:")
    print(df[column].value_counts().sort_index().to_string())


def main() -> None:
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", None)

    teams = pd.read_csv(TEAMS_CSV)
    uefa = pd.read_csv(UEFA_CSV)

    show_overview("team_season_records", teams, season_col="season")
    show_overview("uefacompetitionresults", uefa, season_col="Season")
    show_distinct(uefa, "Cup")
    show_distinct(uefa, "Round")


if __name__ == "__main__":
    main()
