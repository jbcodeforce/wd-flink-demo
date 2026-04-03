-- Dedupe typed src_table rows and write Debezium-style envelopes into raw_leads.
-- Synthetic op = c; after = JSON of lead; before null; source_ts_ms from event time.

INSERT INTO raw_leads
SELECT
  CAST(m_id AS STRING),
  CAST(NULL AS STRING),
  JSON_OBJECT(
    'm_id' VALUE m_id,
    'email' VALUE email,
    'first_name' VALUE first_name,
    'last_name' VALUE last_name,
    'lead_score' VALUE lead_score,
    'status' VALUE status,
    'created_at' VALUE DATE_FORMAT(created_at, 'yyyy-MM-dd HH:mm:ss.SSS')
  ),
  'c',
  CAST(UNIX_TIMESTAMP(created_at) * 1000 AS BIGINT)
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
