-- Daily tumbling windows (event time on activity_ts): distinct activities per program per day.
INSERT INTO fct_nb_act_per_pgm
SELECT
  window_start,
  window_end,
  COALESCE(program_name, 'UNKNOWN') AS program_name,
  COUNT(DISTINCT activity_id) AS nb_activities
FROM TABLE(
  TUMBLE(
    TABLE (
      SELECT *
      FROM dim_activities
      WHERE activity_ts IS NOT NULL
    ),
    DESCRIPTOR(activity_ts),
    INTERVAL '1' DAY
  )
)
GROUP BY
  window_start,
  window_end,
  COALESCE(program_name, 'UNKNOWN');
