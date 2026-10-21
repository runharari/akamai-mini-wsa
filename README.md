# Mini WSA: a mini security analytics pipeline

A Spring Boot service that ingests web-security events (DLRs), classifies and scores them, stores them in ClickHouse, and serves statistics and samples over a REST API.

> Work in progress. Sections are filled in as each milestone lands.

## Quick start

Requirements: Docker (about 3 GB of memory for the whole stack).

```bash
docker compose up --build -d --wait   # Kafka, ClickHouse and the app (4 roles); a few minutes the first time
curl -s localhost:8080/actuator/health
```

- Ingest API: `localhost:8080` (the `ingest` container)
- Query APIs: `localhost:8081` (the `query` container)
- ClickHouse HTTP: `localhost:8123` (user `wsa`, password `wsa`)

Build and test the app without Docker: Java 25, then `./mvnw test` (unit tests, seconds) or `./mvnw verify` (adds the integration tests, which need Docker for Testcontainers).

## How it works, in three lines

1. `POST /v1/events/ingest` validates the events and writes them to the Kafka topic `events.raw`, keyed by `clientIp`. It answers **201** only after Kafka has stored them.
2. A Kafka Streams processor enriches each event (attack type, threat score) and writes it to `events.enriched`.
3. A batch consumer reads `events.enriched` and inserts each batch into ClickHouse with one `INSERT`. The query APIs read from ClickHouse.

Rows become queryable about a second after the 201.

## API

### Ingest

`POST /v1/events/ingest` takes one event or an array of up to 5,000 events.

```bash
curl -s -X POST localhost:8080/v1/events/ingest -H 'Content-Type: application/json' -d '{
  "eventId": "evt-00132", "timestamp": "2026-05-20T14:32:10Z", "configId": 14227,
  "policyId": "pol_web1", "clientIp": "203.0.113.42", "hostname": "www.example.com",
  "path": "/api/v1/login", "method": "POST", "statusCode": 403,
  "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
  "rule": { "id": "950001", "name": "SQL_INJECTION", "message": "SQL Injection Attack Detected",
            "severity": "CRITICAL", "category": "INJECTION" },
  "action": "DENY", "geoLocation": { "country": "CN", "city": "Beijing" },
  "requestSize": 1024, "responseSize": 256 }'
```
```json
{"ingested":1}
```

An array works the same way: `-d '[{...}, {...}]'` returns `{"ingested":2}`.

**Validation.** Required: `eventId`, `timestamp`, `configId`, `clientIp`, `path`, `method`, `rule.id`, `rule.severity`, `rule.category`, `action`. Enums are upper case. `timestamp` is ISO-8601 with a zone. If any event is invalid, nothing is stored, and the response lists the problems by event index and field:

```bash
curl -s -X POST localhost:8080/v1/events/ingest -H 'Content-Type: application/json' -d '[
  {"eventId":"a","timestamp":"yesterday","configId":1,"clientIp":"1.1.1.1","path":"/","method":"GET",
   "rule":{"id":"1","severity":"LOW","category":"BOT"},"action":"DENY"},
  {"eventId":"b","timestamp":"2026-05-20T14:32:10Z","configId":1,"clientIp":"1.1.1.1","path":"/","method":"GET",
   "rule":{"id":"1","severity":"LOW","category":"BOT"},"action":"BLOCK"}]'
```
```json
{"errors":[
  {"index":0,"field":"timestamp","message":"must be an ISO-8601 timestamp with a zone, e.g. 2026-05-20T14:32:10Z"},
  {"index":1,"field":"action","message":"must be one of DENY, ALERT, MONITOR"}]}
```

Status codes: **201** stored in Kafka · **400** invalid request · **503** Kafka didn't confirm in time (with `Retry-After`; resend the whole request, duplicates are harmless).

`receivedAt` is always set by the server. Every response carries an `X-Request-Id` header, which also appears in the logs.

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

Capped at 100. Note: the highest reachable score with these rules is 90; the cap guards future rules.

Each event also gets an `attackType` from its `rule.category` (`INJECTION` → "SQL/Command Injection", `XSS` → "Cross-Site Scripting", `PROTOCOL_VIOLATION` → "Protocol Anomaly", `DATA_LEAKAGE` → "Data Exfiltration", `BOT` → "Bot Activity", `DOS` → "Denial of Service", `RATE_LIMIT` → "Rate Limiting").

**Details:**
- The path rule is case-insensitive (`/Admin/users` counts).
- **Repeat offender:** the event's IP had more than 5 *earlier* events within the 10 minutes before this event. The event itself doesn't count, and an event exactly 10 minutes older still does.
- **Event time, not arrival time:** the window uses the events' own `timestamp`. Loading 6 hours of generated events in a few seconds doesn't turn every IP into a repeat offender.

**Where the window lives:**
- In the Kafka Streams processor, as a state store (`ip-window`) keyed by IP. Events are keyed by `clientIp` in Kafka, so all events of one IP reach the same processor task, in order. The counts stay correct with any number of processor instances, with no locks and no Redis.
- The store is local (RocksDB) and backed up to a Kafka changelog topic, so the processor remembers IPs across restarts.
- **Bounded memory:** only the newest 6 events per IP are kept, which is enough to answer "more than 5?". A flooding IP costs the same memory as any other. IPs idle for 10 minutes are removed.
- **Trade-off:** an event that arrives very late (older than the 6 kept events) can be under-counted.

Example: 7 `CRITICAL`/`DENY` events from one IP on `/admin/users`, one minute apart. The first six score 75, and the seventh scores 90.

## Deployment

One codebase and one Docker image. `docker compose` runs it as 4 containers, one Spring profile (role) each: `ingest`, `processor`, `sink` and `query`. Each role loads only its own module, so the roles scale and fail independently. Heavy queries never slow down ingest. With no profile set (tests, IDE), all roles run in one process.

## Monitoring

Each container exposes `/actuator/health` and `/actuator/prometheus`. The pipeline's own metrics are prefixed `wsa_`:

```bash
curl -s localhost:8080/actuator/prometheus | grep '^wsa_'
```

TODO (v1.0): what each metric tells you.

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
