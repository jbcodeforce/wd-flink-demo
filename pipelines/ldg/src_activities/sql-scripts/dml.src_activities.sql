-- Dedup: one row per activity_id, keep the latest event by timestamp.
-- Quality (3 predicates): non-empty activity_type, non-null timestamp, non-empty primary_attribute_value.

INSERT INTO src_activities
SELECT
  activity_id,
  lead_id,
  activity_type,
  primary_attribute_value,
  timestamp,
  metadata
FROM
  SELECT
    activity_id,
    lead_id,
    activity_type,
    primary_attribute_value,
    timestamp,
    metadata,
    ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY timestamp DESC) AS rn
  FROM raw_activities
  WHERE
    activity_type IS NOT NULL
    AND timestamp IS NOT NULL
    AND primary_attribute_value IS NOT NULL
) AS deduped
WHERE rn = 1;