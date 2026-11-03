"""Turn the two raw CSVs into DataFrames that match the tables in schema.sql."""

import pandas as pd

from reference_data import (
    COMPETITIONS,
    ENGLISH_CLUBS_NOT_IN_PREMIER_LEAGUE,
    ROUND_SPELLING_FIXES,
    SHOOTOUT_WINNERS,
    UEFA_NAME_TO_CLUB,
)

FIRST_YEAR = 1992


def english_uefa_matches(uefa_raw: pd.DataFrame) -> pd.DataFrame:
    """UEFA rows in the five competitions, from 1992/93, with at least one English side."""
    in_scope = uefa_raw["Cup"].isin([c["code"] for c in COMPETITIONS])
    english = (uefa_raw["HomeCountry"] == "England") | (uefa_raw["AwayCountry"] == "England")
    matches = uefa_raw[in_scope & english].copy()
    matches["season_start"] = matches["Season"].str[:4].astype(int)
    return matches[matches["season_start"] >= FIRST_YEAR]


def build_clubs(teams_raw: pd.DataFrame, uefa_raw: pd.DataFrame) -> pd.DataFrame:
    """One row per English club: its team-file name, plus the UEFA spelling when it differs."""
    matches = english_uefa_matches(uefa_raw)
    english_names = pd.concat(
        [
            matches.loc[matches["HomeCountry"] == "England", "Home"],
            matches.loc[matches["AwayCountry"] == "England", "Away"],
        ]
    ).unique()

    uefa_spelling = {}  # club name -> UEFA spelling
    for uefa_name in english_names:
        name = UEFA_NAME_TO_CLUB.get(uefa_name, uefa_name)
        uefa_spelling[name] = uefa_name

    team_names = set(teams_raw["team"])
    unknown = set(uefa_spelling) - team_names - ENGLISH_CLUBS_NOT_IN_PREMIER_LEAGUE
    if unknown:
        raise ValueError(f"English clubs in the UEFA file with no match in the team file: {sorted(unknown)}")

    names = sorted(team_names | set(uefa_spelling))
    return pd.DataFrame(
        {
            "club_id": range(1, len(names) + 1),
            "name": names,
            "alt_name": [uefa_spelling[n] if uefa_spelling.get(n, n) != n else None for n in names],
        }
    )


def clean_team_seasons(teams_raw: pd.DataFrame, clubs: pd.DataFrame) -> pd.DataFrame:
    """team_season: the season as a start year, the club as a club_id."""
    df = teams_raw.rename(columns={"champion": "is_champion", "relegated": "is_relegated"})
    df["season_start"] = df["season"].str[:4].astype(int)
    df = df.merge(clubs[["club_id", "name"]], left_on="team", right_on="name", how="left")

    bad = df[df["won"] + df["drawn"] + df["lost"] != df["played"]]
    if not bad.empty:
        raise ValueError(f"won + drawn + lost does not equal played for: {bad[['season', 'team']].values.tolist()}")

    return df[
        [
            "season_start", "club_id", "position", "played", "won", "drawn", "lost",
            "goals_for", "goals_against", "goal_difference", "points", "is_champion", "is_relegated",
        ]
    ]


def clean_uefa_matches(uefa_raw: pd.DataFrame, clubs: pd.DataFrame) -> pd.DataFrame:
    """uefa_match: English-club matches only, with club ids for the English sides."""
    df = english_uefa_matches(uefa_raw)

    # Look up a club_id by either spelling; foreign clubs aren't in the club table, so they stay empty.
    with_alt_name = clubs.dropna(subset=["alt_name"])
    id_by_spelling = {**dict(zip(clubs["name"], clubs["club_id"])), **dict(zip(with_alt_name["alt_name"], with_alt_name["club_id"]))}

    out = pd.DataFrame(
        {
            "match_date": pd.to_datetime(df["Date"]).dt.date,
            "season_start": df["season_start"],
            "competition": df["Cup"],
            "round": df["Round"].replace(ROUND_SPELLING_FIXES),
            "home_name": df["Home"],
            "away_name": df["Away"],
            "home_club_id": df["Home"].where(df["HomeCountry"] == "England").map(id_by_spelling).astype("Int64"),
            "away_club_id": df["Away"].where(df["AwayCountry"] == "England").map(id_by_spelling).astype("Int64"),
            "home_goals": df["gh"],
            "away_goals": df["ga"],
            "home_country": df["HomeCountry"],
            "away_country": df["AwayCountry"],
            "home_goals_ht": df["gh1h"],
            "away_goals_ht": df["ga1h"],
            "is_walkover": df["IsWalkover"].astype(bool),
        }
    )

    out["shootout_winner"] = None
    for (year, competition), winner in SHOOTOUT_WINNERS.items():
        final = (out["season_start"] == year) & (out["competition"] == competition) & (out["round"] == "Final")
        out.loc[final, "shootout_winner"] = winner
    return out.reset_index(drop=True)
