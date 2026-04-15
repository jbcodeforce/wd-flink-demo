-- Seed: 10 programs (program_id 2001–2010). Referenced by raw_activities.metadata program_id.
-- Seed: same 10 programs as before; one row = one JSON string in `payload` (unique per program_id).
INSERT INTO raw_mkt_pgm (`payload`) VALUES
  ('{"program_id":2001,"name":"Q1 Product Update Newsletter","channel":"Email","status":"Active","workspace":"Default"}'),
  ('{"program_id":2002,"name":"Demo Request 2026 Landing","channel":"Web","status":"Active","workspace":"Default"}'),
  ('{"program_id":2003,"name":"HR Summit Webinar Series","channel":"Webinar","status":"Active","workspace":"Default"}'),
  ('{"program_id":2004,"name":"LinkedIn Retargeting Q1","channel":"Social Media","status":"Active","workspace":"Default"}'),
  ('{"program_id":2005,"name":"Nurture Stream A — Product","channel":"Email","status":"Active","workspace":"Default"}'),
  ('{"program_id":2006,"name":"Partner Co-Marketing FY26","channel":"Email","status":"Completed","workspace":"Default"}'),
  ('{"program_id":2007,"name":"Summer Roadshow APAC","channel":"Event","status":"Active","workspace":"APAC"}'),
  ('{"program_id":2008,"name":"Paid Search — Brand","channel":"Paid Search","status":"Active","workspace":"Default"}'),
  ('{"program_id":2009,"name":"Customer Stories Digest","channel":"Email","status":"Active","workspace":"Default"}'),
  ('{"program_id":2010,"name":"Re-engagement Winback","channel":"Email","status":"Draft","workspace":"Default"}');