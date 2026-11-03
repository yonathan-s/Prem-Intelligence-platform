"""Work out the Historical Dominance Index (HDI) for every Premier League club and season.

    D   domestic dominance: how well the club did at home
    E   European strength:  how well the other English clubs did in Europe (from season_index)
    C   domestic parity:    how evenly matched the rest of the league was
    L   league strength:    75% E + 25% C
    HDI D scaled by L, so a dominant club in a strong league scores higher

Every part runs from 0 to 100. The weights live in reference_data.py.

Run after season_index.py, from the repo root:  python data_pipeline/dominance_score.py
"""

import numpy as np
import pandas as pd
from sqlalchemy import text

from load import get_engine, load_table
from reference_data import D_WEIGHTS, HDI_BASE, HDI_LEAGUE_SHARE, L_EUROPE_WEIGHT, L_PARITY_WEIGHT

MODEL_VERSION = "v1"  # change this when the method changes, so old and new scores can be told apart


def scale_0_to_100(values: pd.Series) -> pd.Series:
    """Lowest value in the whole data set becomes 0, highest becomes 100."""
    return 100 * (values - values.min()) / (values.max() - values.min())


def compute_dominance_score(team_season: pd.DataFrame, season_index: pd.DataFrame) -> pd.DataFrame:
    df = team_season.copy()

    # ---- D: domestic dominance ----------------------------------------------------------------
    # Everything is per game, so the 42-game seasons are comparable with the 38-game ones.
    df["points_per_game"] = df["points"] / df["played"]
    df["win_rate"] = df["won"] / df["played"]
    df["goal_diff_per_game"] = df["goal_difference"] / df["played"]

    # Title margin: points per game ahead of the strongest rival. For the best club that is the
    # gap to the runner-up; for every other club it is the (negative) gap to the best club.
    by_season = df.groupby("season_start")["points_per_game"]
    best = by_season.transform("max")
    runner_up = by_season.transform(lambda s: s.nlargest(2).iloc[-1])
    strongest_rival = best.where(df["points_per_game"] < best, runner_up)
    df["title_margin"] = df["points_per_game"] - strongest_rival

    # Put each measure on a 0-100 scale across all club-seasons, then take the weighted average.
    df["d_score"] = sum(weight * scale_0_to_100(df[measure]) for measure, weight in D_WEIGHTS.items())

    # ---- C: domestic parity -------------------------------------------------------------------
    # How spread out the OTHER clubs' points per game were. The club itself is left out, as it is
    # for E. A small spread means an evenly matched league, which scores high.
    n = df.groupby("season_start")["points_per_game"].transform("size")
    total = by_season.transform("sum")
    total_squares = df.assign(sq=df["points_per_game"] ** 2).groupby("season_start")["sq"].transform("sum")
    others_n = n - 1
    others_mean = (total - df["points_per_game"]) / others_n
    others_variance = (total_squares - df["points_per_game"] ** 2) / others_n - others_mean**2
    df["rest_of_league_ppg_sd"] = np.sqrt(others_variance.clip(lower=0))
    df["c_score"] = 100 - scale_0_to_100(df["rest_of_league_ppg_sd"])  # smallest spread = 100

    # ---- E, L and the HDI ---------------------------------------------------------------------
    df = df.merge(
        season_index[["season_start", "club_id", "weighted_score"]].rename(columns={"weighted_score": "e_score"}),
        on=["season_start", "club_id"],
        how="left",
    )
    df["l_score"] = L_EUROPE_WEIGHT * df["e_score"] + L_PARITY_WEIGHT * df["c_score"]
    df["hdi"] = df["d_score"] * (HDI_BASE + HDI_LEAGUE_SHARE * df["l_score"] / 100)
    df["model_version"] = MODEL_VERSION

    columns = [
        "season_start", "club_id", "points_per_game", "win_rate", "goal_diff_per_game", "title_margin",
        "rest_of_league_ppg_sd", "d_score", "e_score", "c_score", "l_score", "hdi", "model_version",
    ]
    out = df[columns].copy()
    number_columns = [c for c in columns if c not in ("season_start", "club_id", "model_version")]
    out[number_columns] = out[number_columns].round(4)
    out[["d_score", "e_score", "c_score", "l_score", "hdi"]] = out[["d_score", "e_score", "c_score", "l_score", "hdi"]].round(2)
    return out


def main() -> None:
    engine = get_engine()
    team_season = pd.read_sql(
        "SELECT season_start, club_id, played, won, goal_difference, points FROM team_season", engine
    )
    season_index = pd.read_sql(
        "SELECT season_start, club_id, weighted_score::float AS weighted_score FROM season_index", engine
    )

    dominance_score = compute_dominance_score(team_season, season_index)

    # Replace the old scores in one transaction, so a failure never leaves the table half-written.
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dominance_score"))
        load_table(conn, "dominance_score", dominance_score)


if __name__ == "__main__":
    main()
