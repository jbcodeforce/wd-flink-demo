
INSERT INTO src_mkt_pgm
with extracted_payload as (
  select
    json_value(payload, '$.program_id') as program_id,
    json_value(payload, '$.name') as name,
    json_value(payload, '$.channel') as channel,
    json_value(payload, '$.status') as status,
    json_value(payload, '$.workspace') as workspace,
    `$rowtime` as event_ts
  from raw_mkt_pgm where payload IS NOT NULL
)
SELECT
  program_id,
  name,
  channel,
  status,
  workspace
FROM
  (SELECT
    program_id,
    name,
    channel,
    status,
    workspace,
    ROW_NUMBER() OVER (PARTITION BY program_id ORDER BY event_ts DESC) AS rn
  FROM extracted_payload
  WHERE
    COALESCE(TRIM(name), '') <> ''
    AND COALESCE(TRIM(channel), '') <> ''
    AND COALESCE(TRIM(status), '') <> ''
    AND status <> 'Draft'
) AS deduped
WHERE rn = 1;