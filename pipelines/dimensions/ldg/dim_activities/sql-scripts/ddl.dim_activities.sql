CREATE TABLE IF NOT EXISTS dim_activities (
  `activity_id` INT NOT NULL,
  `lead_id` INT NOT NULL,
  `activity_type` VARCHAR(2147483647),
  `primary_attribute_value` VARCHAR(2147483647),
  `activity_ts` TIMESTAMP(3),
  `lead_email` VARCHAR(2147483647),
  `lead_first_name` VARCHAR(2147483647),
  `lead_last_name` VARCHAR(2147483647),
  `lead_score` INT,
  `lead_status` VARCHAR(2147483647),
  `program_name` VARCHAR(2147483647),
  `program_channel` VARCHAR(2147483647),
  `program_status` VARCHAR(2147483647),
  WATERMARK FOR `activity_ts` AS `activity_ts` - INTERVAL '5' SECOND,
  PRIMARY KEY(`activity_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`activity_id`) INTO 1 BUCKETS
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