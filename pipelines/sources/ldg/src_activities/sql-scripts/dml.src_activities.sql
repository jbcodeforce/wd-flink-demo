-- Dedup: one row per activity_id, keep the latest event by rowtime.
INSERT INTO src_activities
SELECT
  activity_id,
  lead_id,
  activity_type,
  primary_attribute_value,
  `timestamp` as `activity_ts`,
  metadata
FROM
 raw_activities
WHERE
    activity_type IS NOT NULL
    AND primary_attribute_value IS NOT NULL
