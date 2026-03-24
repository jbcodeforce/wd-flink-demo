-- Mirrors producers.models.Lead: core profile, custom attributes, timestamps.
-- m_id: int -> INT; email/first_name/last_name/status -> VARCHAR; lead_score -> INT;
-- attributes: Dict[str, Any] -> MAP<STRING, STRING> (values as strings);
-- created_at: datetime -> TIMESTAMP(3)

CREATE TABLE IF NOT EXISTS raw_leads (
  `m_id` INT NOT NULL,
  `email` VARCHAR(2147483647) NOT NULL,
  `first_name` VARCHAR(2147483647),
  `last_name` VARCHAR(2147483647),
  `lead_score` INT NOT NULL,
  `status` VARCHAR(2147483647) NOT NULL,
  `attributes` MAP<VARCHAR(2147483647), VARCHAR(2147483647)> NOT NULL,
  `created_at` TIMESTAMP(3) NOT NULL,
  PRIMARY KEY (`m_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`m_id`) INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
