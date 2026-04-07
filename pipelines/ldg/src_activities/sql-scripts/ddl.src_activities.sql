CREATE TABLE IF NOT EXISTS src_activities (
  `activity_id` INT NOT NULL,
  `lead_id` INT NOT NULL,
  `activity_type` VARCHAR(2147483647) NOT NULL,
  `primary_attribute_value` VARCHAR(2147483647) NOT NULL,
  `timestamp` TIMESTAMP(3) NOT NULL,
  `metadata` MAP<VARCHAR(2147483647), VARCHAR(2147483647)> NOT NULL,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND,
  PRIMARY KEY (`activity_id`) NOT ENFORCED
) DISTRIBUTED BY HASH(`activity_id`) INTO 3 BUCKETS
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