# Premier League Intelligence Platform

A full-stack football analytics platform that compares Premier League teams and players across different eras to find out which were the most dominant, while accounting for how strong their competition was.

The platform goes beyond raw statistics to answer questions such as:

- Was Arsenal's unbeaten 2003/04 season more dominant than Manchester City's 100-point 2017/18 season?
- Was Thierry Henry more impressive relative to his era than Mohamed Salah?
- Does winning the Premier League mean more when other English clubs are performing strongly in Europe?
- Which Premier League team achieved the greatest dominance relative to its competition?

> **Status:** the data pipeline and the Historical Dominance Index are built and loaded into a Supabase Postgres database. The API and frontend are next. See the [roadmap](#roadmap).

---

## Core features

| Feature | Description |
|---|---|
| Historical team comparison | Compare team-seasons using points per game, win rate, goal difference, possession and other available statistics |
| Historical player comparison | Compare goals, assists and per-90 statistics, plus performance relative to players of the same era |
| Dominance ranking engine | Rank Premier League teams with a custom **Historical Dominance Index (HDI)** |
| League strength analysis | Measure the strength of domestic competition using European results and league competitiveness |
| Interactive dashboard | Historical rankings, charts, comparisons and an explanation of every score |
| Customisable rankings | Adjust metric weightings and see how the rankings change |

### Fairness between eras

Comparing raw totals across eras is misleading, so the platform adjusts for differences such as:

- **Season length.** 1992/93 to 1994/95 had 42 matches per team; later seasons have 38. All metrics use per-game rates.
- **Data availability.** Possession, shots and xG are only available for later seasons. Scores are computed from the metrics a season actually has, and missing data is shown rather than hidden.
- **Era context.** Player and team performance is also measured relative to the rest of the same season, not only in absolute terms.

---

## Historical Dominance Index (HDI)

The HDI combines three independent measures, each normalised to 0–100:

| Component | Measures |
|---|---|
| **D**: domestic dominance | Points per game, win rate, goal difference per game, title margin |
| **E**: European strength | How well *other* English clubs performed in Europe, relative to their domestic standing. The team being scored is excluded so it isn't rewarded twice for its own success. |
| **C**: domestic parity | How closely matched the rest of the league was |

$$
L = 0.75E + 0.25C
$$

$$
HDI = D \times \left(0.80 + 0.20 \cdot \frac{L}{100}\right)
$$

The formula and weights are provisional. Every weight lives in [data_pipeline/reference_data.py](data_pipeline/reference_data.py), and each score row records the model version that produced it.

### How each part is calculated

- **D (domestic dominance):** a weighted average of points per game (30%), win rate (20%), goal difference per game (25%) and title margin (25%). The title margin is points per game ahead of the strongest rival: the gap to the runner-up for the champion, and the (negative) gap to the champion for everyone else. Each measure is scaled from 0 to 100 across all 686 club-seasons first, and everything is per game, so the 42-game seasons are comparable with the 38-game ones.
- **E (European strength):** for each club-season, how far the other Premier League clubs got in Europe. Each competition round has a rank (0 for qualifying up to 10 for winning the competition) multiplied by a strength factor for the competition (Champions League 1.0, Europa League and UEFA Cup 0.6, Cup Winners' Cup 0.5, Conference League 0.4). Each club is judged on the competition where it got closest to the top. E is the other clubs' points as a percentage of the most they could have earned, so a season with more European places is not scored higher just for having more clubs, and a Champions League club counts for more than a Europa League one.
- **C (domestic parity):** the spread (standard deviation) of the other clubs' points per game that season, scaled so the most evenly matched league scores 100.

Known limits: points deductions are not applied, so positions in the four seasons that had them (Middlesbrough 1996/97, Portsmouth 2009/10, Everton 2023/24, Nottingham Forest 2023/24) follow raw points; the 1992/93 Champions League pre-group rounds are scored approximately.

---

## Architecture

This is the target end-state architecture. It is being built in stages (see the [roadmap](#roadmap)), and the MVP runs entirely locally.

```mermaid
flowchart TD
    A[Data sources<br/>Results CSVs · European results · Player stats] --> B[Ingestion & orchestration<br/>Python · Apache Airflow]
    B --> C[Bronze: raw data lake<br/>AWS S3 · original CSV / JSON]
    C --> D[Silver: processing<br/>Databricks · PySpark · Delta Lake]
    D --> E[Gold: analytical warehouse<br/>Snowflake · dbt]
    E --> F[Comparison engine<br/>Python · era normalisation · HDI · model versioning]
    F --> G[Serving layer<br/>PostgreSQL · Redis cache]
    G --> H[Backend API<br/>FastAPI]
    H --> I[Frontend<br/>React · TypeScript]
```

Cross-cutting concerns: **Docker**, **Terraform**, **AWS**, **GitHub Actions** (CI/CD), **monitoring** (logs, metrics, alerts) and **data quality** (tests, validation, lineage).

---

## Tech stack

The MVP is a small Python pipeline writing to a hosted Postgres database. Its layout mirrors the end-state platform (raw data, cleaning, scoring, serving), so later stages swap the tools without rewriting the logic.

| Area | MVP | End state |
|---|---|---|
| Language | Python 3.11+, TypeScript | Same |
| Ingestion | Python (pandas, SQLAlchemy) from CSV files in `raw_data/` | Python, **Apache Airflow** (scheduling, retries, backfills) |
| Raw storage | Local CSV files | **AWS S3** (Bronze) |
| Processing | pandas | **Databricks**, **PySpark**, **Delta Lake** (Silver) |
| Transformation | pandas scoring scripts | **dbt** on **Snowflake** (Gold) |
| Warehouse | **Supabase** (hosted PostgreSQL) | **Snowflake** |
| Data quality | Validation checks in the pipeline | dbt tests, **Great Expectations**, lineage via dbt docs |
| Scoring engine | Python (pandas, NumPy) | Python, versioned model configurations |
| Serving database | **Supabase** (hosted PostgreSQL) | Supabase or **AWS RDS PostgreSQL**, **Redis** cache |
| API | **FastAPI**, Pydantic | Same, containerised |
| Frontend | React, Vite, TanStack Query, Recharts, Tailwind | Same, hosted on S3 + CloudFront |
| Containers | — | **Docker**, **AWS ECS Fargate** |
| Infrastructure as code | — | **Terraform** |
| CI/CD | — | **GitHub Actions** (tests, dbt build, image publish, Terraform plan/apply) |
| Observability | Printed row counts | **CloudWatch** or **Prometheus + Grafana**, alerting |

---

## Data model

Defined in [data_pipeline/schema.sql](data_pipeline/schema.sql):

| Table | Purpose |
|---|---|
| `season` | One row per season from 1992/93: number of clubs and games per team |
| `club` | One row per English club, with the UEFA file's spelling where it differs from the league file's |
| `competition` | The five European competitions, with each one's strength factor |
| `team_season` | Premier League table for every club and season (686 rows) |
| `uefa_match` | Every European match involving an English club since 1992/93 (2,244 rows), with the winner of any penalty-decided final |
| `round_rank` | How far each round of each competition is worth: a rank from 0 to 10 and a weight |
| `season_index` | The European-strength score (E) for each club-season |
| `dominance_score` | D, E, C, L and the HDI for each club-season, with the measures behind them |

Later stages add players and player seasons.

---

## Roadmap

### Stage 1: MVP, historical team comparison
- [x] Postgres database on Supabase, created by `schema.sql`
- [x] Load Premier League tables and European results from CSV, with validation
- [x] Domestic dominance score (D) from per-game measures
- [ ] FastAPI endpoints for seasons, league tables, team-season comparison and rankings
- [ ] React frontend: rankings, side-by-side comparison, season view
- [ ] Tests and GitHub Actions running them

### Stage 2: League strength and HDI
- [x] European competition results loaded and scored (E)
- [x] Domestic-parity metric (C)
- [x] Full HDI, with a model version on every score
- [ ] User-adjustable weightings in the UI

### Stage 3: Data engineering platform
- [ ] Raw data landed in AWS S3 (Bronze)
- [ ] Databricks / PySpark cleaning into Delta tables (Silver)
- [ ] dbt models running on Snowflake (Gold)
- [ ] Airflow DAGs orchestrating ingestion, processing and transformation
- [ ] Data quality checks and lineage

### Stage 4: Players and production deployment
- [ ] Historical player statistics with era-adjusted, per-90 comparisons
- [ ] AWS infrastructure provisioned with Terraform
- [ ] Containerised services deployed on ECS
- [ ] Full CI/CD pipeline
- [ ] Monitoring, metrics and alerting

---

## Getting started

Requires Python 3.11+ and a Postgres database (the project uses a Supabase project).

```bash
pip install pandas sqlalchemy "psycopg[binary]" python-dotenv

# .env in the repo root (never committed):
#   DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/postgres

python data_pipeline/run_pipeline.py      # create tables, load the CSVs, build round_rank
python data_pipeline/season_index.py      # European-strength score (E)
python data_pipeline/dominance_score.py   # D, C, L and the HDI
```

`run_pipeline.py` empties and reloads its tables in a single transaction, so it is safe to rerun. The two scoring scripts replace their own tables, so to try different weights, edit `reference_data.py` and rerun them (rerun `run_pipeline.py` first if you changed a round rank or strength factor).

**Inputs** are two CSV files in `raw_data/`: the Premier League tables for 1992-93 to 2025-26, and European match results. The results file has no penalty data, so the winners of the six English finals that finished level are written into `reference_data.py`.

The pipeline turns on row-level security for every table, so Supabase's public API keys cannot read or write them; the scripts connect directly with the database URL.
