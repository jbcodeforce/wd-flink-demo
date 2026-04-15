CREATE TABLE IF NOT EXISTS src_leads (
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
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);