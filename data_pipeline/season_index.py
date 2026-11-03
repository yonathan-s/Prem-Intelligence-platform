"""Work out the European-strength score for every Premier League club and season.

For a club in a given season, the score says how well the OTHER Premier League clubs did in
Europe that season. The club itself is left out, so it isn't rewarded twice for its own run.
Only clubs that entered a European competition count, and the score is a percentage of what
those clubs could have earned, so a season with more European places isn't scored higher
just for having more clubs.

Run after run_pipeline.py, from the repo root:  python data_pipeline/season_index.py
"""

import numpy as np
import pandas as pd
from sqlalchemy import text

from load import get_engine, load_table

MODEL_VERSION = "v1"  # change this when the method changes, so old and new scores can be told apart


def club_results(matches: pd.DataFrame, round_rank: pd.DataFrame) -> pd.DataFrame:
    """One row per club, season and competition: the weight of the furthest round the club played."""
    # Put each match on two rows, one for the home side and one for the away side.
    home = matches.rename(columns={"home_club_id": "club_id"}).assign(
        won_final=lambda d: (d["home_goals"] > d["away_goals"])
        | ((d["home_goals"] == d["away_goals"]) & (d["shootout_winner"] == d["home_name"]))
    )
    away = matches.rename(columns={"away_club_id": "club_id"}).assign(
        won_final=lambda d: (d["away_goals"] > d["home_goals"])
        | ((d["home_goals"] == d["away_goals"]) & (d["shootout_winner"] == d["away_name"]))
    )
    sides = pd.concat([home, away])
    sides = sides[sides["club_id"].notna()]  # keep English sides only

    # A Final that finished level must have a shootout winner, or we can't say who won it.
    level_no_winner = sides[
        (sides["round"] == "Final") & (sides["home_goals"] == sides["away_goals"]) & sides["shootout_winner"].isna()
    ]
    if not level_no_winner.empty:
        raise ValueError("A Final finished level with no shootout_winner; add it to SHOOTOUT_WINNERS")

    # The Final becomes "Winner" or "Runner-up"; every other round keeps its name.
    final_result = np.where(sides["won_final"], "Winner", "Runner-up")
    sides["round_name"] = np.where(sides["round"] == "Final", final_result, sides["round"])

    sides = sides.merge(
        round_rank.rename(columns={"round": "round_name"}), on=["competition", "round_name"], how="left"
    )
    if sides["weight"].isna().any():
        raise ValueError("Some rounds have no weight in round_rank")

    return sides.groupby(["season_start", "club_id", "competition"], as_index=False)["weight"].max()


def compute_season_index(
    matches: pd.DataFrame, round_rank: pd.DataFrame, team_season: pd.DataFrame
) -> pd.DataFrame:
    results = club_results(matches, round_rank)

    # Ignore English clubs that never played in the Premier League (Millwall, Wrexham).
    results = results[results["club_id"].isin(team_season["club_id"])]

    # For every competition a club entered: what it earned, and the most it could have earned there
    # (by winning it). "share" is how close it got to the top of that competition, from 0 to 1.
    winner_weight = round_rank[round_rank["round"] == "Winner"].set_index("competition")["weight"]
    results = results.assign(best_possible=results["competition"].map(winner_weight))
    results["share"] = (results["weight"] / results["best_possible"]).round(6)  # rounded so equal shares tie

    # A club in two competitions in one season (say it dropped from the Champions League into the
    # Europa League) is judged on the one where it got closest to the top; if that is a tie, on the
    # run that earned more. That leaves one row per club per season, with "earned" and
    # "best_possible" both taken from that one competition.
    clubs = (
        results.sort_values(["season_start", "club_id", "share", "weight"], ascending=[True, True, False, False])
        .drop_duplicates(["season_start", "club_id"])
        .rename(columns={"weight": "earned"})
    )

    # Season totals across every Premier League club that played in Europe.
    totals = clubs.groupby("season_start").agg(total_earned=("earned", "sum"), total_possible=("best_possible", "sum"))

    # For each club-season, take the season total and subtract the club's own contribution (if it
    # was in Europe at all). What is left is the other clubs' score.
    out = team_season[["season_start", "club_id"]].merge(clubs, on=["season_start", "club_id"], how="left")
    out = out.merge(totals, on="season_start", how="left")
    out["raw_score"] = out["total_earned"].fillna(0) - out["earned"].fillna(0)
    out["ceiling"] = out["total_possible"].fillna(0) - out["best_possible"].fillna(0)

    # A percentage of the ceiling. If no other club was in Europe the ceiling is 0, so there's no score.
    out["weighted_score"] = (100 * out["raw_score"] / out["ceiling"]).where(out["ceiling"] > 0)
    out["model_version"] = MODEL_VERSION

    out[["raw_score", "ceiling", "weighted_score"]] = out[["raw_score", "ceiling", "weighted_score"]].round(2)
    return out[["season_start", "club_id", "raw_score", "ceiling", "weighted_score", "model_version"]]


def main() -> None:
    engine = get_engine()
    matches = pd.read_sql("SELECT * FROM uefa_match", engine)
    round_rank = pd.read_sql("SELECT competition, round, rank, weight::float AS weight FROM round_rank", engine)
    team_season = pd.read_sql("SELECT season_start, club_id FROM team_season", engine)

    season_index = compute_season_index(matches, round_rank, team_season)

    # Replace the old scores in one transaction, so a failure never leaves the table half-written.
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM season_index"))
        load_table(conn, "season_index", season_index)


if __name__ == "__main__":
    main()
