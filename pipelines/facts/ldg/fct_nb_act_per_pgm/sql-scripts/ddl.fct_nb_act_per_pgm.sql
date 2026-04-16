CREATE TABLE IF NOT EXISTS fct_nb_act_per_pgm (
  `window_start` TIMESTAMP(3) NOT NULL,
  `window_end` TIMESTAMP(3) NOT NULL,
  `program_id` INT NOT NULL,
  `program_name` VARCHAR(2147483647) NOT NULL,
  `nb_activities` BIGINT NOT NULL,
  PRIMARY KEY(`window_start`, `program_name`) NOT ENFORCED
) DISTRIBUTED BY HASH(`window_start`,`program_name`) INTO 1 BUCKETS
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
