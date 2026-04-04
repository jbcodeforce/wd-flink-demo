  -- Test scenario 1: Open Email for Re-engagement Winback — to test the fact: number of open emails for a program over time
  INSERT INTO raw_activities VALUES
  (3011, 1008, 'Open Email', 'Re-engagement Winback — We miss you',
   TIMESTAMP '2026-03-20 07:05:00',
   MAP['program_id', '2010', 'batch', 'winback-03', 'channel', 'Email']),
   (3012, 1009, 'Open Email', 'Re-engagement Winback — We miss you',
   TIMESTAMP '2026-03-21 10:05:00',
   MAP['program_id', '2010', 'batch', 'winback-03', 'channel', 'Email']);