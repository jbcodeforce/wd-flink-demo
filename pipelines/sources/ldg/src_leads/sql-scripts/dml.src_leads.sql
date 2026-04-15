-- Parse Debezium envelope on raw_leads (JSON in after/before), then dedupe by latest source_ts_ms.
-- Quality filters on typed fields after parse.

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
    ROW_NUMBER() OVER (PARTITION BY m_id ORDER BY source_ts_ms DESC) AS rn
  FROM (
    SELECT
      CAST(COALESCE(JSON_VALUE(after, '$.m_id'), JSON_VALUE(before, '$.m_id'), '0') AS INT) AS m_id,
      COALESCE(JSON_VALUE(after, '$.email'), JSON_VALUE(before, '$.email'), '') AS email,
      COALESCE(JSON_VALUE(after, '$.first_name'), JSON_VALUE(before, '$.first_name'), '') AS first_name,
      COALESCE(JSON_VALUE(after, '$.last_name'), JSON_VALUE(before, '$.last_name'), '') AS last_name,
      CAST(COALESCE(JSON_VALUE(after, '$.lead_score'), JSON_VALUE(before, '$.lead_score'), '0') AS INT) AS lead_score,
      COALESCE(JSON_VALUE(after, '$.status'), JSON_VALUE(before, '$.status'), '') AS status,
      MAP[
        'company', COALESCE(JSON_VALUE(COALESCE(after, before), '$.attributes.company'), ''),
        'job_title', COALESCE(JSON_VALUE(COALESCE(after, before), '$.attributes.job_title'), ''),
        'industry', COALESCE(JSON_VALUE(COALESCE(after, before), '$.attributes.industry'), ''),
        'region', COALESCE(JSON_VALUE(COALESCE(after, before), '$.attributes.region'), '')
      ] AS attributes,
      COALESCE(
        TO_TIMESTAMP(
          COALESCE(JSON_VALUE(after, '$.created_at'), JSON_VALUE(before, '$.created_at')),
          'yyyy-MM-dd HH:mm:ss.SSS'
        ),
        TIMESTAMP '1970-01-01 00:00:00'
      ) AS created_at,
      source_ts_ms
    FROM raw_leads
  ) AS parsed
) AS deduped
WHERE rn = 1
  AND COALESCE(TRIM(email), '') LIKE '%@%'
  AND lead_score BETWEEN 0 AND 100
  AND COALESCE(TRIM(status), '') <> ''
  AND created_at IS NOT NULL;
