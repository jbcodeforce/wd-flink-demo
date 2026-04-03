-- Mirrors producers.models.MarketingProgram: program identity, channel, status, workspace.

CREATE TABLE IF NOT EXISTS `raw_mkt_pgm` (
  `payload` VARCHAR(2147483647) NOT NULL
) 
WITH (
  'changelog.mode' = 'append',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset'
);

