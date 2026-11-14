# Silver for team seasons: read the latest Bronze batch, give every column its proper type, check
# each row, and write the clean rows to silver_team_season. Rows that fail a check go to
# silver_quarantine with the reason, instead of stopping the pipeline. This is the Spark version of
# clean_team_seasons() in data_pipeline/transform.py.
#
# Paste each "# %%" block into its own cell, with prem_lakehouse as the default lakehouse.

# %% [parameters]
config_path = "Files/config/sources.json"
source_name = "team_season_records"  # which entry of sources.json this notebook processes
silver_table = "silver_team_season"
quarantine_table = "silver_quarantine"

# %%
import json
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# %% Find this source in the config
with open(f"/lakehouse/default/{config_path}", encoding="utf-8") as f:
    source = next(s for s in json.load(f)["sources"] if s["name"] == source_name)
bronze_table = source["target_table"]
keys = source["keys"]  # the columns that should identify one row: season and team

# %% Read the latest Bronze batch only
# Bronze is append-only, so it holds every batch ever loaded. batch_id is a random UUID and can't
# be sorted, so the newest batch is the one with the latest ingested_at.
bronze_all = spark.table(bronze_table)
latest_ingested_at = bronze_all.agg(F.max("ingested_at")).first()[0]
bronze = bronze_all.filter(F.col("ingested_at") == latest_ingested_at)
latest_batch_id = bronze.select("batch_id").first()[0]
rows_in = bronze.count()
print(f"{bronze_table}: {rows_in} rows in the latest batch {latest_batch_id}")

# %% Prepare: keep the raw row for the quarantine, and flag duplicates
# raw_json is the row exactly as Bronze has it. If a value fails to convert to a number below, the
# typed column becomes null and the bad value would be lost, so the quarantine stores this copy.
# A row is a duplicate when another row in the batch has the same keys (season and team).
bronze = bronze.withColumn("raw_json", F.to_json(F.struct(*bronze.columns))).withColumn(
    "is_duplicate", F.count("*").over(Window.partitionBy(*keys)) > 1
)

# %% Give every column its proper type
# try_cast returns null when a value can't be converted, instead of raising an error, so a bad
# value is caught by the "required value missing" check below.
INT_COLUMNS = [
    "position", "played", "won", "drawn", "lost",
    "goals_for", "goals_against", "goal_difference", "points",
]


def to_int(name: str):
    return F.expr(f"try_cast(`{name}` AS int)")


def to_bool(name: str):
    # "true" -> True, "false" -> False, anything else -> null (caught by the same check).
    lowered = F.lower(F.col(name))
    return F.when(lowered == "true", True).when(lowered == "false", False)


typed = bronze.select( # 'select' builds a new dataframe
    F.expr("try_cast(substring(season, 1, 4) AS int)").alias("season_start"),  # "1992-93" -> 1992
    F.col("team").alias("club_name"),
    *[to_int(c).alias(c) for c in INT_COLUMNS], # for each column convert the string of integers into integers
    to_bool("champion").alias("is_champion"),
    to_bool("relegated").alias("is_relegated"),
    "ingested_at", "source_file", "batch_id",  # audit columns, carried forward from Bronze
    "raw_json", "is_duplicate",
)

# %% Check every row
# Each failing rule adds a reason. concat_ws skips the rules a row passed, so a clean row ends up
# with an empty reason.
required = ["season_start", "club_name", *INT_COLUMNS, "is_champion", "is_relegated"]
any_required_missing = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in required]) # checks if any required column has a null value with an or statement.

checked = typed.withColumn(
    "reason",
    F.concat_ws(
        "; ",
        F.when(any_required_missing, F.lit("required value missing or not a valid type")),
        F.when(
            F.col("won") + F.col("drawn") + F.col("lost") != F.col("played"),
            F.lit("won + drawn + lost does not equal played"),
        ),
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
if rows_good + rows_bad != rows_in:
    raise ValueError(f"{rows_in} rows came in but {rows_good} + {rows_bad} came out")
