"""Database side of the pipeline: connect, create the tables, empty them, fill them."""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Connection, Engine, create_engine, text

SCHEMA_FILE = Path(__file__).with_name("schema.sql")

# Child tables first, so nothing is emptied while another table still points at it.
TABLES_TO_EMPTY = ["uefa_match", "team_season", "club", "competition", "season"]


def get_engine() -> Engine:
    """Connect using DATABASE_URL from .env."""
    load_dotenv()
    url = os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+psycopg://", 1)
    # Supabase's pooler on port 6543 doesn't support prepared statements, which psycopg uses by default.
    return create_engine(url, connect_args={"prepare_threshold": None})


def run_schema(conn: Connection) -> None:
    """Create any missing tables."""
    conn.execute(text(SCHEMA_FILE.read_text(encoding="utf-8")))


def empty_tables(conn: Connection) -> None:
    """Remove all rows, so rerunning the pipeline reloads from scratch instead of duplicating."""
    conn.execute(text(f"TRUNCATE {', '.join(TABLES_TO_EMPTY)} RESTART IDENTITY"))


def load_table(conn: Connection, table: str, df: pd.DataFrame) -> None:
    """Insert a DataFrame into a table whose columns match the DataFrame's."""
    df.to_sql(table, conn, if_exists="append", index=False, method="multi", chunksize=1000)
    print(f"{table}: {len(df)} rows")
