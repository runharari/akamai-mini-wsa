# Mini WSA: a mini security analytics pipeline

A Spring Boot service that ingests web-security events (DLRs), classifies and scores them, stores them in ClickHouse, and serves statistics and samples over a REST API.

> Work in progress. Sections are filled in as each milestone lands.

## Quick start

```bash
docker compose up --build          # app on :8080, ClickHouse on :8123
# load 10,000 generated events
# TODO (v0.5-generator)
```

Requirements: Docker. To build without Docker: Java 25 and `./mvnw verify`.

## API

### Ingest
TODO (v0.1-ingestion): curl examples for a single event, a batch, and a 400 response.

### Stats summary
TODO (v0.3-stats)

### Samples
TODO (v0.4-samples)

### Time series (bonus)
TODO (v0.6-timeseries)

## Threat score

| Rule | Points |
|---|---|
| Severity CRITICAL / HIGH / MEDIUM / LOW | 40 / 30 / 20 / 10 |
| Action DENY / ALERT / MONITOR | +20 / +10 / +0 |
| Path contains `/admin` or `/login` | +15 |
| More than 5 events from the same IP in the last 10 minutes (event time) | +15 |

Capped at 100. Note: the highest reachable score with these rules is 90.

## Architecture

TODO: diagram + write and read paths.

## Why ClickHouse

TODO

## Design decisions

TODO

## What I'd improve with more time

TODO

## What was challenging

TODO

## One thing I learned

TODO

## Milestones

`v0.1-ingestion` · `v0.2-enrichment` · `v0.3-stats` · `v0.4-samples` · `v0.5-generator` · `v0.6-timeseries` · `v1.0`
