-- Daily tumbling windows (event time on activity_ts): distinct activities per program per day.
set 'sql.tables.scan.idle-timeout' = '1s';
INSERT INTO fct_nb_act_per_pgm
SELECT
  window_start,
  window_end,
  COALESCE(program_name, 'UNKNOWN') AS program_name,
  COUNT(DISTINCT activity_id) AS nb_activities
FROM TABLE(
  TUMBLE(
    TABLE dim_activities,
    DESCRIPTOR(activity_ts),
    INTERVAL '4' HOURS
  )
)
GROUP BY
  window_start,
  window_end,
  COALESCE(program_name, 'UNKNOWN');
