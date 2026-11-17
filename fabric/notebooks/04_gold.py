# Gold: turn Silver into a star schema and compute the Historical Dominance Index.
#
#   Cell 1  setup and helpers
#   Cell 2  dimensions (dim_club, dim_season, dim_competition) and facts (fact_team_season, fact_uefa_match)
#   Cell 3  round_rank          port of build_round_rank()       in data_pipeline/transform.py
#   Cell 4  season_index (E)    port of data_pipeline/season_index.py
#   Cell 5  dominance_score     port of data_pipeline/dominance_score.py (D, C, L and the HDI)
#
# Run 02_silver_team_season and 03_silver_uefa_match first. There is no parameters cell: Gold has
# nothing a pipeline needs to override, and code in a parameters cell would run before overrides
# arrive. Paste each "# %%" block into its own cell, with prem_lakehouse as the default lakehouse.

# %% Cell 1: setup and helpers
import json
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.window import Window

reference_path = "Files/config/reference_data.json"
quarantine_table = "silver_quarantine"  # the same quarantine the Silver notebooks write to

with open(f"/lakehouse/default/{reference_path}", encoding="utf-8") as f:
    ref = json.load(f)
MODEL_VERSION = ref["model_version"]  # change it in the config when the method changes

gold_quarantined = {}  # table name -> how many rows were quarantined while building it


def lookup(mapping: dict):
    """A Spark lookup built from a dictionary: lookup(d)[column] gives the mapped value, or null."""
    return F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])


def save(df, table: str) -> None:
    """Replace a Gold table with df. Gold is rebuilt from Silver on every run, so overwrite is safe."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"{table}: {spark.table(table).count()} rows")


def quarantine(bad, table: str) -> None:
    """Log rows that failed a Gold check. Gold is rebuilt from scratch, so this table's old rows go first."""
    if spark.catalog.tableExists(quarantine_table):
        spark.sql(f"DELETE FROM {quarantine_table} WHERE source_table = '{table}'")
    gold_quarantined[table] = bad.count()
    if gold_quarantined[table] > 0:
        row_columns = [c for c in bad.columns if c != "reason"]
        (
            bad.select(
                F.lit(table).alias("source_table"),
                "batch_id",
                "reason",
                F.to_json(F.struct(*row_columns)).alias("row_json"),
                F.current_timestamp().alias("quarantined_at"),
            )
            .write.format("delta").mode("append").saveAsTable(quarantine_table)
        )


# %% Cell 2: dimensions and facts
silver_team = spark.table("silver_team_season")
silver_uefa = spark.table("silver_uefa_match")

# ---- dim_competition: from the config --------------------------------------------------------
dim_competition = spark.createDataFrame(
    [(c["code"], c["name"], float(c["strength_factor"])) for c in ref["competitions"]],
    "code string, name string, strength_factor double",
).withColumn("strength_factor", F.col("strength_factor").cast("decimal(4,2)"))

# ---- dim_season: worked out from the data, not hard-coded ------------------------------------
# Clubs in the league = rows in that season; games per team = the most games any club played.
dim_season = silver_team.groupBy("season_start").agg(
    F.count("*").cast("int").alias("teams_in_league"),
    F.max("played").alias("games_per_team"),
).select(
    F.col("season_start").alias("start_year"),
    F.concat(  # 1992 -> "1992/93"
        F.col("season_start").cast("string"),
        F.lit("/"),
        F.lpad(((F.col("season_start") + 1) % 100).cast("string"), 2, "0"),
    ).alias("label"),
    "teams_in_league",
    "games_per_team",
)

# ---- dim_club: one row per English club, numbered from 1 in alphabetical order ----------------
# Same numbering as the pandas pipeline, so club_id matches Supabase. alt_name is the UEFA file's
# spelling when it differs from the team file's.
club_names = (
    silver_team.select(F.col("club_name").alias("name"))
    .union(silver_uefa.select(F.col("home_club_name").alias("name")))
    .union(silver_uefa.select(F.col("away_club_name").alias("name")))
    .filter(F.col("name").isNotNull())
    .distinct()
)
uefa_spellings = (
    silver_uefa.select(F.col("home_club_name").alias("name"), F.col("home_name").alias("alt"))
    .union(silver_uefa.select(F.col("away_club_name").alias("name"), F.col("away_name").alias("alt")))
    .filter(F.col("name").isNotNull() & (F.col("name") != F.col("alt")))
    .groupBy("name")
    .agg(F.max("alt").alias("alt_name"))
)
dim_club = (
    club_names.join(uefa_spellings, "name", "left")
    .withColumn("club_id", F.row_number().over(Window.orderBy("name")).cast("int"))
    .select("club_id", "name", "alt_name")
)

save(dim_competition, "dim_competition")
save(dim_season, "dim_season")
save(dim_club, "dim_club")

# ---- Checks that replace Postgres's foreign keys ----------------------------------------------
# Delta tables don't enforce REFERENCES, so every fact row is checked against its dimensions here.
valid_seasons = [r[0] for r in dim_season.select("start_year").collect()]
valid_competitions = [r[0] for r in dim_competition.select("code").collect()]

# ---- fact_team_season ---------------------------------------------------------------------------
team_columns = [
    "season_start", "club_id", "position", "played", "won", "drawn", "lost",
    "goals_for", "goals_against", "goal_difference", "points", "is_champion", "is_relegated",
    "ingested_at", "source_file", "batch_id",  # audit columns, carried forward from Bronze
]
team_checked = silver_team.join(
    dim_club.select("club_id", F.col("name").alias("club_name")), "club_name", "left"
).withColumn(
    "reason",
    F.concat_ws(
        "; ",
        F.when(F.col("club_id").isNull(), F.lit("club not found in dim_club")),
        F.when(~F.col("season_start").isin(valid_seasons), F.lit("season not found in dim_season")),
    ),
)
save(team_checked.filter(F.col("reason") == "").select(*team_columns), "fact_team_season")
quarantine(team_checked.filter(F.col("reason") != "").select(*team_columns, "reason"), "fact_team_season")

# ---- fact_uefa_match ----------------------------------------------------------------------------
# Foreign clubs have no club_id, and that is expected. An English side that fails to find its
# club_id is not.
match_columns = [
    "match_id", "match_date", "season_start", "competition", "round", "home_name", "away_name",
    "home_club_id", "away_club_id", "home_goals", "away_goals", "home_country", "away_country",
    "home_goals_ht", "away_goals_ht", "is_walkover", "shootout_winner",
    "ingested_at", "source_file", "batch_id",
]
home_clubs = dim_club.select(F.col("club_id").alias("home_club_id"), F.col("name").alias("home_club_name"))
away_clubs = dim_club.select(F.col("club_id").alias("away_club_id"), F.col("name").alias("away_club_name"))

match_checked = (
    silver_uefa.join(home_clubs, "home_club_name", "left")
    .join(away_clubs, "away_club_name", "left")
    # A key worked out from the match itself, so the same match gets the same id on every run.
    .withColumn("match_id", F.xxhash64("match_date", "competition", "round", "home_name", "away_name"))
    .withColumn(
        "reason",
        F.concat_ws(
            "; ",
            F.when(F.col("home_club_name").isNotNull() & F.col("home_club_id").isNull(),
                   F.lit("home club not found in dim_club")),
            F.when(F.col("away_club_name").isNotNull() & F.col("away_club_id").isNull(),
                   F.lit("away club not found in dim_club")),
            F.when(~F.col("season_start").isin(valid_seasons), F.lit("season not found in dim_season")),
            F.when(~F.col("competition").isin(valid_competitions), F.lit("competition not found in dim_competition")),
        ),
    )
)
save(match_checked.filter(F.col("reason") == "").select(*match_columns), "fact_uefa_match")
quarantine(match_checked.filter(F.col("reason") != "").select(*match_columns, "reason"), "fact_uefa_match")

# %% Cell 3: round_rank
# A rank (0 to 10) and a weight for every round that appears in the data. weight = rank * the
# competition's strength factor. The Final isn't ranked directly: every competition instead gets a
# "Winner" and a "Runner-up" row, because what a Final is worth depends on who won it.
matches = spark.table("fact_uefa_match")
competitions = spark.table("dim_competition")

general_rank = lookup(ref["round_ranks"])
exceptions = spark.createDataFrame(
    [(e["competition"], e["round"], e["rank"]) for e in ref["round_rank_exceptions"]],
    "ex_competition string, ex_round string, ex_rank int",
)

played = matches.filter(F.col("round") != "Final").select("competition", "round").distinct()
ranked = (
    played.join(
        exceptions,
        (F.col("competition") == F.col("ex_competition")) & (F.col("round") == F.col("ex_round")),
        "left",
    )
    # A competition-specific rank wins over the general one for that round label.
    .withColumn("rank", F.coalesce(F.col("ex_rank"), general_rank[F.col("round")]))
    .select("competition", "round", "rank")
)

# A round with no rank is a gap in the config, not a bad data row, so this stops the notebook.
unranked = ranked.filter(F.col("rank").isNull()).collect()
if unranked:
    raise ValueError(f"No rank for these rounds, add them to reference_data.json: {[tuple(r) for r in unranked]}")

final_rows = competitions.select(F.col("code").alias("competition")).crossJoin(
    spark.createDataFrame(
        [("Runner-up", ref["runner_up_rank"]), ("Winner", ref["winner_rank"])], "round string, rank int"
    )
)

round_rank = (
    ranked.unionByName(final_rows.select("competition", "round", "rank"))
    .join(competitions.select(F.col("code").alias("competition"), "strength_factor"), "competition")
    .withColumn("weight", F.round(F.col("rank") * F.col("strength_factor"), 2).cast("decimal(6,2)"))
    .select("competition", "round", "rank", "weight")
)
save(round_rank, "round_rank")

# %% Cell 4: season_index (E, the European-strength score)
# For each club-season: how far the OTHER Premier League clubs got in Europe, as a percentage of
# the most they could have earned. See data_pipeline/season_index.py for the reasoning.
round_weights = spark.table("round_rank").select(
    "competition", F.col("round").alias("round_name"), F.col("weight").cast("double").alias("weight")
)
team_seasons = spark.table("fact_team_season")


def side(club_column: str, own_goals: str, other_goals: str, name_column: str):
    """One row per match per side, saying whether that side won. Only meaningful for a Final."""
    return matches.select(
        "season_start", "competition", "round", "home_goals", "away_goals", "shootout_winner",
        F.col(club_column).alias("club_id"),
        (
            (F.col(own_goals) > F.col(other_goals))
            | ((F.col("home_goals") == F.col("away_goals")) & (F.col("shootout_winner") == F.col(name_column)))
        ).alias("won_final"),
    )


sides = side("home_club_id", "home_goals", "away_goals", "home_name").unionByName(
    side("away_club_id", "away_goals", "home_goals", "away_name")
).filter(F.col("club_id").isNotNull())  # English sides only

# A Final that finished level must have a shootout winner, or we can't say who won it.
if sides.filter((F.col("round") == "Final") & (F.col("home_goals") == F.col("away_goals"))
                & F.col("shootout_winner").isNull()).count() > 0:
    raise ValueError("A Final finished level with no shootout_winner: add it to shootout_winners in the config")

# The Final becomes "Winner" or "Runner-up"; every other round keeps its name.
sides = sides.withColumn(
    "round_name",
    F.when(F.col("round") == "Final", F.when(F.col("won_final"), "Winner").otherwise("Runner-up"))
    .otherwise(F.col("round")),
).join(round_weights, ["competition", "round_name"], "left")
if sides.filter(F.col("weight").isNull()).count() > 0:
    raise ValueError("Some rounds have no weight in round_rank")

# The furthest round each club reached in each competition, for clubs that played in the Premier League.
results = (
    sides.groupBy("season_start", "club_id", "competition").agg(F.max("weight").alias("weight"))
    .join(team_seasons.select("club_id").distinct(), "club_id", "left_semi")  # drops Millwall, Wrexham
)

# best_possible = what winning that competition is worth; share = how close the club got, 0 to 1.
winner_weight = round_weights.filter(F.col("round_name") == "Winner").select(
    "competition", F.col("weight").alias("best_possible")
)
results = results.join(winner_weight, "competition").withColumn(
    "share", F.round(F.col("weight") / F.col("best_possible"), 6)  # rounded so equal shares tie
)

# A club in two competitions in one season is judged on the one where it got closest to the top;
# a tie goes to the run that earned more. One row per club per season is left.
best_run = Window.partitionBy("season_start", "club_id").orderBy(
    F.desc("share"), F.desc("weight"), "competition"
)
clubs = (
    results.withColumn("rank_in_season", F.row_number().over(best_run))
    .filter(F.col("rank_in_season") == 1)
    .select("season_start", "club_id", F.col("weight").alias("earned"), "best_possible")
)

# Season totals across every Premier League club that played in Europe.
totals = clubs.groupBy("season_start").agg(
    F.sum("earned").alias("total_earned"), F.sum("best_possible").alias("total_possible")
)

# Season total minus the club's own contribution (if it was in Europe at all) = the other clubs' score.
scored = (
    team_seasons.select("season_start", "club_id")
    .join(clubs, ["season_start", "club_id"], "left")
    .join(totals, "season_start", "left")
    .withColumn("raw", F.coalesce("total_earned", F.lit(0.0)) - F.coalesce("earned", F.lit(0.0)))
    .withColumn("ceil", F.coalesce("total_possible", F.lit(0.0)) - F.coalesce("best_possible", F.lit(0.0)))
)
season_index = scored.select(
    "season_start",
    "club_id",
    F.round("raw", 2).cast("decimal(8,2)").alias("raw_score"),
    F.round("ceil", 2).cast("decimal(8,2)").alias("ceiling"),
    # A percentage of the ceiling. With no other club in Europe the ceiling is 0, so there is no score.
    F.when(F.round("ceil", 2) > 0, F.round(100 * F.col("raw") / F.col("ceil"), 2))
    .cast("decimal(6,2)").alias("weighted_score"),
    F.lit(MODEL_VERSION).alias("model_version"),
)
save(season_index, "season_index")

# %% Cell 5: dominance_score (D, C, L and the HDI), then a summary
# D = domestic dominance, E = European strength (from season_index), C = domestic parity,
# L = 75% E + 25% C, HDI = D scaled by L. See data_pipeline/dominance_score.py.
season = Window.partitionBy("season_start")
whole_table = Window.partitionBy()  # one window over every club-season


def scale_0_to_100(column: str):
    """Lowest value in the whole table becomes 0, highest becomes 100."""
    c = F.col(column)
    low, high = F.min(c).over(whole_table), F.max(c).over(whole_table)
    return 100 * (c - low) / (high - low)


# ---- D: domestic dominance ------------------------------------------------------------------------
# Everything is per game, so the 42-game seasons are comparable with the 38-game ones.
df = (
    team_seasons.select("season_start", "club_id", "played", "won", "goal_difference", "points")
    .withColumn("points_per_game", F.col("points") / F.col("played"))
    .withColumn("win_rate", F.col("won") / F.col("played"))
    .withColumn("goal_diff_per_game", F.col("goal_difference") / F.col("played"))
)

# Title margin: points per game ahead of the strongest rival. For the best club that is the gap to
# the runner-up; for everyone else it is the (negative) gap to the best club.
best = F.max("points_per_game").over(season)
runner_up = F.element_at(F.sort_array(F.collect_list("points_per_game").over(season), asc=False), 2)
rival = F.when(F.col("points_per_game") < best, best).otherwise(runner_up)
df = df.withColumn("title_margin", F.col("points_per_game") - rival)

# Scale each measure from 0 to 100 across all club-seasons, then take the weighted average.
df = df.withColumn(
    "d_score",
    reduce(lambda a, b: a + b, [F.lit(w) * scale_0_to_100(m) for m, w in ref["d_weights"].items()]),
)

# ---- C: domestic parity ---------------------------------------------------------------------------
# The spread of the OTHER clubs' points per game: season totals minus the club's own contribution.
ppg = F.col("points_per_game")
others_n = F.count("*").over(season) - 1
others_mean = (F.sum(ppg).over(season) - ppg) / others_n
others_variance = (F.sum(ppg * ppg).over(season) - ppg * ppg) / others_n - others_mean * others_mean
df = df.withColumn("rest_of_league_ppg_sd", F.sqrt(F.greatest(others_variance, F.lit(0.0))))
df = df.withColumn("c_score", 100 - scale_0_to_100("rest_of_league_ppg_sd"))  # smallest spread = 100

# ---- E, L and the HDI -----------------------------------------------------------------------------
e_scores = spark.table("season_index").select(
    "season_start", "club_id", F.col("weighted_score").cast("double").alias("e_score")
)
df = (
    df.join(e_scores, ["season_start", "club_id"], "left")
    .withColumn("l_score", ref["l_europe_weight"] * F.col("e_score") + ref["l_parity_weight"] * F.col("c_score"))
    .withColumn("hdi", F.col("d_score") * (ref["hdi_base"] + ref["hdi_league_share"] * F.col("l_score") / 100))
)

four_decimals = ["points_per_game", "win_rate", "goal_diff_per_game", "title_margin", "rest_of_league_ppg_sd"]
two_decimals = ["d_score", "e_score", "c_score", "l_score", "hdi"]
dominance_score = df.select(
    "season_start",
    "club_id",
    *[F.round(c, 4).cast("decimal(6,4)").alias(c) for c in four_decimals],
    *[F.round(c, 2).cast("decimal(6,2)").alias(c) for c in two_decimals],
    F.lit(MODEL_VERSION).alias("model_version"),
)
save(dominance_score, "dominance_score")

# ---- Summary --------------------------------------------------------------------------------------
print("\nQuarantined while building Gold:", gold_quarantined)
print("\nTop three club-seasons by HDI (the pandas pipeline gives Manchester City 2017/18 first, at 89.68):")
(
    spark.table("dominance_score")
    .join(spark.table("dim_club"), "club_id")
    .orderBy(F.desc("hdi"))
    .select("name", "season_start", "d_score", "e_score", "c_score", "hdi")
    .show(3)
)
