# NBA Ingest — ETL pipeline (Postgres)

A small, focused ETL that pulls NBA data and lands it in PostgreSQL on a schedule (cron). This repo contains the code I built to fetch the NBA data, normalize/flatten it, and upsert into Postgres tables so downstream tools (DBeaver, pgAdmin, BI tools) can query it.

Below is a ready-to-use README.md you can drop into your repo — it documents what the project does, how to set it up, how to run it manually, and how to schedule it with a cron job.

## Features

- Fetches NBA data from the public API endpoints (games / leagues / teams / box scores — whatever your ingest scripts request).
- Normalizes JSON into relational tables and stores raw JSON in jsonb columns for flexibility.
- Upserts data (idempotent) so repeated runs don’t duplicate rows.
- Simple cron-based scheduling for periodic runs
- Minimal dependencies — plain Python, requests, SQLAlchemy / psycopg2.

## Architecture (High Level)

[ NBA API ]  --->  [ ingest script (Python) ]  --->  [ PostgreSQL ]
