-- Mirrors producers.models.MarketingProgram: program identity, channel, status, workspace.

CREATE TABLE IF NOT EXISTS raw_mkt_pgm (
  `program_id` INT NOT NULL,
  `name` VARCHAR(2147483647) NOT NULL,
  `channel` VARCHAR(2147483647) NOT NULL,
  `status` VARCHAR(2147483647) NOT NULL,
  `workspace` VARCHAR(2147483647) NOT NULL,
  PRIMARY KEY (`program_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`program_id`) INTO 3 BUCKETS
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
