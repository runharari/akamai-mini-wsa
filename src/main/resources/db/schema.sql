-- One wide table of enriched security events.
-- ReplacingMergeTree: the sink is at-least-once, so a re-delivered batch can insert the same
-- eventId twice; rows with the same sort key are merged in the background.
-- Sort key starts with configId + timestamp because every query filters on them.
CREATE TABLE IF NOT EXISTS events
(
    eventId      String,
    timestamp    DateTime64(3, 'UTC'),
    receivedAt   DateTime64(3, 'UTC'),
    configId     UInt32,
    policyId     String,
    clientIp     String,
    hostname     String,
    path         String,
    method       LowCardinality(String),
    statusCode   Nullable(UInt16),
    userAgent    String,
    ruleId       String,
    ruleName     String,
    ruleMessage  String,
    severity     LowCardinality(String),
    category     LowCardinality(String),
    action       LowCardinality(String),
    country      LowCardinality(String),
    city         String,
    requestSize  Nullable(UInt32),
    responseSize Nullable(UInt32),
    attackType   LowCardinality(String),
    threatScore  UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (configId, timestamp, eventId);
