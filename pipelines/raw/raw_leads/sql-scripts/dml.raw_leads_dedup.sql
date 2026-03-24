-- Pipeline: dedupe from src_table into raw_leads (not seed data).
-- Dedup: one row per m_id, keep the latest event by created_at.
-- Quality (5 predicates): plausible email, score in 0..100, non-empty status, non-null created_at.

INSERT INTO raw_leads
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
  FROM src_table
  WHERE m_id > 0
    AND COALESCE(TRIM(email), '') LIKE '%@%'
    AND lead_score BETWEEN 0 AND 100
    AND COALESCE(TRIM(status), '') <> ''
    AND created_at IS NOT NULL
) AS deduped
WHERE rn = 1;
