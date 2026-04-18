-- Debezium CDC raw layer: envelope with before/after as STRING (schema-less JSON).
-- Downstream (e.g. src_leads) parses `after` / `before` with JSON_VALUE.
CREATE TABLE IF NOT EXISTS raw_leads (
  `m_id`         STRING NOT NULL COMMENT 'Source primary key (from Debezium key)',
  `before`       STRING COMMENT 'Row state before change (JSON); null for INSERT',
  `after`        STRING COMMENT 'Row state after change (JSON); null for DELETE',
  `op`           STRING COMMENT 'Debezium op: c=create, u=update, d=delete, r=read/snapshot',
  `source_ts_ms` BIGINT COMMENT 'Source event timestamp (ms)',
  PRIMARY KEY (`m_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`m_id`) INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
