-- Dedup: one row per m_id, keep the latest event by created_at.
-- Quality (5 predicates):  plausible email, score in 0..100, non-empty status, non-null created_at.
-- Requires src_table to expose the same columns as raw_leads; define a watermark on
-- created_at on the source if the planner requires an event-time attribute for ROW_NUMBER.

INSERT INTO src_leads
SELECT
  m_id,
  email,
  first_name,
  last_name,
  lead_score,
  status,
  attributes,
  created_at
FROM (
  SELECT
    m_id,
    email,
    first_name,
    last_name,
    lead_score,
    status,
    attributes,
    created_at,
    ROW_NUMBER() OVER (PARTITION BY m_id ORDER BY created_at DESC) AS rn
  FROM raw_leads
  WHERE
    COALESCE(TRIM(email), '') LIKE '%@%'
    AND lead_score BETWEEN 0 AND 100
    AND COALESCE(TRIM(status), '') <> ''
    AND created_at IS NOT NULL
) AS deduped
WHERE rn = 1;