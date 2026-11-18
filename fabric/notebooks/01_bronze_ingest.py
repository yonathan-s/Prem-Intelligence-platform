# Bronze ingestion: copy every source listed in sources.json into a Bronze Delta table, untouched,
# plus three audit columns. Paste each "# %%" block into its own cell in a Fabric notebook that has
# prem_lakehouse attached as the default lakehouse.
#
# Bronze is append-only: every run adds a new batch and nothing is overwritten or deleted. Running
# this twice therefore gives every row twice, once per batch_id. Silver reads only the latest batch.

# %% [parameters]
# In Fabric, mark this cell with "Toggle parameter cell" so a pipeline can override these values.
config_path = "Files/config/sources.json"  # where sources.json was uploaded in the Lakehouse
batch_id = ""  # the pipeline passes its run id here; left empty, the notebook makes its own

# %%
import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

# %% Load the config
# The default lakehouse is mounted as a normal folder at /lakehouse/default, so plain open() works.
with open(f"/lakehouse/default/{config_path}", encoding="utf-8") as f:
    sources = json.load(f)["sources"]

print(f"Found {len(sources)} sources: {[s['name'] for s in sources]}")

# %% Set up this run
# One batch_id and one timestamp for the whole run, so every row from every source shares them.
batch_id = batch_id or str(uuid.uuid4())
ingested_at = datetime.now(timezone.utc)
print(f"batch_id: {batch_id}")
print(f"ingested_at: {ingested_at.isoformat()}")

# %% Ingest each source into its Bronze table
for source in sources:
    # Read the file exactly as it is. With infer_schema false in the config, every column is text.
    raw = (
        spark.read.format(source["format"])
        .option("header", str(source["header"]).lower())
        .option("inferSchema", str(source["infer_schema"]).lower())
        .load(source["source_path"])
    )
    rows_read = raw.count()

    # Audit columns: which run, when, and from which file. They are added here and carried through
    # Silver and Gold, so any Gold row can be traced back to its Bronze row.
    bronze = (
        raw.withColumn("ingested_at", F.lit(ingested_at))
        .withColumn("source_file", F.lit(source["source_path"]))
        .withColumn("batch_id", F.lit(batch_id))
    )

    # Append, so earlier batches are kept. The table is created on the first run.
    bronze.write.format("delta").mode("append").saveAsTable(source["target_table"])

    # Check: the rows in this batch must equal the rows read from the file.
    rows_written = spark.table(source["target_table"]).filter(F.col("batch_id") == batch_id).count()
    if rows_written != rows_read:
        raise ValueError(f"{source['name']}: read {rows_read} rows but wrote {rows_written}")
    print(f"{source['target_table']}: {rows_written} rows in batch {batch_id}")
