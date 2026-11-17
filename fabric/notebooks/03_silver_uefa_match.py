# Silver for UEFA matches: read the latest Bronze batch, keep only the matches the project covers,
# give every column its proper type, conform the English club names, check each row, and write the
# clean rows to silver_uefa_match. Rows that fail a check go to silver_quarantine. This is the Spark
# version of english_uefa_matches() and clean_uefa_matches() in data_pipeline/transform.py.
#
# Run 02_silver_team_season first: the club-name check reads silver_team_season.
# Paste each "# %%" block into its own cell, with prem_lakehouse as the default lakehouse.

# %% [parameters]
config_path = "Files/config/sources.json"
reference_path = "Files/config/reference_data.json"
source_name = "uefa_match_results"  # which entry of sources.json this notebook processes
silver_table = "silver_uefa_match"
team_silver_table = "silver_team_season"
quarantine_table = "silver_quarantine"

# %%
import json
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# %% Load the two config files
with open(f"/lakehouse/default/{config_path}", encoding="utf-8") as f:
    source = next(s for s in json.load(f)["sources"] if s["name"] == source_name)
with open(f"/lakehouse/default/{reference_path}", encoding="utf-8") as f:
    ref = json.load(f)

bronze_table = source["target_table"]
keys = source["keys"]  # Date, Cup, Round, Home, Away: the columns that should identify one match

# %% Read the latest Bronze batch only
# Bronze is append-only, so the newest batch is the one with the latest ingested_at.
bronze_all = spark.table(bronze_table)
latest_ingested_at = bronze_all.agg(F.max("ingested_at")).first()[0]
bronze = bronze_all.filter(F.col("ingested_at") == latest_ingested_at)
latest_batch_id = bronze.select("batch_id").first()[0]
rows_in = bronze.count()
print(f"{bronze_table}: {rows_in} rows in the latest batch {latest_batch_id}")

# %% Keep only the matches the project covers
# "Out of scope" is not the same as "failed": most of the file is other competitions, matches
# before 1992/93, or games with no English club. Those rows are left out and counted, not
# quarantined. A row whose Season can't be read stays in, so it is quarantined and not lost.
bronze = bronze.withColumn("season_start", F.expr("try_cast(substring(`Season`, 1, 4) AS int)"))
competition_codes = [c["code"] for c in ref["competitions"]]

in_scope = bronze.filter(
    F.col("Cup").isin(competition_codes)
    & ((F.col("HomeCountry") == "England") | (F.col("AwayCountry") == "England"))
    & ((F.col("season_start") >= ref["first_season_start"]) | F.col("season_start").isNull())
)
rows_in_scope = in_scope.count()
rows_out_of_scope = rows_in - rows_in_scope
print(f"{rows_out_of_scope} rows out of scope, {rows_in_scope} in scope")

# %% Prepare: keep the raw row for the quarantine, and flag duplicates
# raw_json is the row exactly as Bronze has it, kept because a failed conversion below turns the
# typed value into null. A row is a duplicate when another in-scope row has the same keys.
prepared = in_scope.withColumn("raw_json", F.to_json(F.struct(*in_scope.columns))).withColumn(
    "is_duplicate", F.count("*").over(Window.partitionBy(*keys)) > 1
)


# %% Give every column its proper type, and conform the club names
def lookup(mapping: dict):
    """A Spark lookup built from a dictionary: lookup(d)[column] gives the mapped value, or null."""
    return F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])


club_lookup = lookup(ref["uefa_name_to_club"])
round_lookup = lookup(ref["round_spelling_fixes"])


def club_name(name_column: str, country_column: str):
    """The club's team-file name for an English side ("Tottenham Hotspur" -> "Tottenham"), else null."""
    return F.when(
        F.col(country_column) == "England",
        F.coalesce(club_lookup[F.col(name_column)], F.col(name_column)),
    )


def to_int(name: str):
    return F.expr(f"try_cast(`{name}` AS int)")


walkover_flag = F.expr("try_cast(`IsWalkover` AS int)")

typed = prepared.select(
    F.expr("try_cast(substring(`Date`, 1, 10) AS date)").alias("match_date"),  # "1999-05-05 00:00:00.000"
    "season_start",
    F.col("Cup").alias("competition"),
    F.coalesce(round_lookup[F.col("Round")], F.col("Round")).alias("round"),
    F.col("Home").alias("home_name"),  # the UEFA file's spelling, as given
    F.col("Away").alias("away_name"),
    club_name("Home", "HomeCountry").alias("home_club_name"),  # null for foreign clubs
    club_name("Away", "AwayCountry").alias("away_club_name"),
    to_int("gh").alias("home_goals"),
    to_int("ga").alias("away_goals"),
    F.col("HomeCountry").alias("home_country"),
    F.col("AwayCountry").alias("away_country"),
    to_int("gh1h").alias("home_goals_ht"),
    to_int("ga1h").alias("away_goals_ht"),
    F.when(walkover_flag == 1, True).when(walkover_flag == 0, False).alias("is_walkover"),
    "ingested_at", "source_file", "batch_id",  # audit columns, carried forward from Bronze
    "raw_json", "is_duplicate",
)

# %% Fill in the winner of the six finals decided on penalties
# The UEFA file has no penalties column, so the winners come from reference_data.json.
shootouts = spark.createDataFrame(
    [(s["season_start"], s["competition"], s["winner"]) for s in ref["shootout_winners"]],
    "so_season int, so_competition string, shootout_winner string",
)
typed = typed.join(
    F.broadcast(shootouts),
    (F.col("season_start") == F.col("so_season"))
    & (F.col("competition") == F.col("so_competition"))
    & (F.col("round") == "Final"),
    "left",
).drop("so_season", "so_competition")

# %% Check every row
# Each failing rule adds a reason; a clean row ends up with an empty reason.
required = [  # the columns schema.sql marks NOT NULL
    "match_date", "season_start", "competition", "round", "home_name", "away_name",
    "home_goals", "away_goals", "home_country", "away_country", "is_walkover",
]
any_required_missing = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in required])

# An English side must be a club from the team file, or one of the English clubs that never played
# in the Premier League. This is the check inside build_clubs() in transform.py.
known_clubs = {r["club_name"] for r in spark.table(team_silver_table).select("club_name").distinct().collect()}
known_clubs |= set(ref["english_clubs_not_in_premier_league"])
known_clubs = list(known_clubs)
unknown_english_club = (
    (F.col("home_club_name").isNotNull() & ~F.col("home_club_name").isin(known_clubs))
    | (F.col("away_club_name").isNotNull() & ~F.col("away_club_name").isin(known_clubs))
)

checked = typed.withColumn(
    "reason",
    F.concat_ws(
        "; ",
        F.when(any_required_missing, F.lit("required value missing or not a valid type")),
        F.when(unknown_english_club, F.lit("English club not found in the team file")),
        F.when(F.col("is_duplicate"), F.lit(f"duplicate on keys: {', '.join(keys)}")),
    ),
)

good = checked.filter(F.col("reason") == "").drop("reason", "raw_json", "is_duplicate")
bad = checked.filter(F.col("reason") != "")

# %% Write Silver (overwrite) and the quarantine (append)
# Silver is rebuilt from the latest Bronze batch on every run, so rerunning gives the same table.
good.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

# The quarantine is a log, so it appends. To keep a rerun from logging the same rows twice, first
# remove this source's rows for this batch.
if spark.catalog.tableExists(quarantine_table):
    spark.sql(
        f"DELETE FROM {quarantine_table} "
        f"WHERE source_table = '{bronze_table}' AND batch_id = '{latest_batch_id}'"
    )

rows_bad = bad.count()
if rows_bad > 0:
    (
        bad.select(
            F.lit(bronze_table).alias("source_table"),
            "batch_id",
            "reason",
            F.col("raw_json").alias("row_json"),
            F.current_timestamp().alias("quarantined_at"),
        )
        .write.format("delta").mode("append").saveAsTable(quarantine_table)
    )

# %% Report, and check that no row was lost
rows_good = spark.table(silver_table).count()
print(f"{silver_table}: {rows_good} rows")
print(f"{quarantine_table}: {rows_bad} rows from this batch")
if rows_out_of_scope + rows_good + rows_bad != rows_in:
    raise ValueError(
        f"{rows_in} rows came in but {rows_out_of_scope} + {rows_good} + {rows_bad} came out"
    )
