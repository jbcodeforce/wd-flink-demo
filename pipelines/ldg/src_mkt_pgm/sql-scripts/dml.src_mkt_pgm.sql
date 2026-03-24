-- Dedup: one row per program_id, keep the latest event by created_at.
-- Quality (3 predicates): non-empty name, non-null channel, non-empty status.

INSERT INTO src_mkt_pgm
SELECT
  program_id,
  name,
  channel,
  status,
  workspace
FROM
  SELECT
    program_id,
    name,
    channel,
    status,
    workspace,
    ROW_NUMBER() OVER (PARTITION BY program_id ORDER BY created_at DESC) AS rn
  FROM raw_mkt_pgm
  WHERE
    COALESCE(TRIM(name), '') <> ''
    AND COALESCE(TRIM(channel), '') <> ''
    AND COALESCE(TRIM(status), '') <> ''
) AS deduped
WHERE rn = 1;