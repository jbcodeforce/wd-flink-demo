-- Seed: marketing activities aligned to 4-hour event-time tumbling windows (see fct_nb_act_per_pgm).
-- Watermark: activity_ts - 5 seconds. A window [start, end) emits only after watermark >= end,
-- i.e. some row must have activity_ts >= end + 5 seconds. Rows marked "watermark anchor" sit just
-- after each 4h boundary so prior windows can complete while staying realistic (batch sync, late click).
-- metadata program_id must match raw_mkt_pgm (2001-2009 Active/Completed; avoid 2010 Draft).
INSERT INTO raw_activities VALUES
  -- 2026-03-18 — paid search (window [08:00,12:00) needs later data; closed by downstream days)
  (3009, 1009, 'Paid Click', 'brand-search-workday',
   TIMESTAMP '2026-03-18 11:12:00',
   MAP['program_id', '2008', 'keyword', 'workday hcm', 'channel', 'Paid Search']),
  -- 2026-03-19 — webinar prep + partner content; anchor closes [12:00,16:00) on this day
  (3007, 1007, 'Register Webinar', 'HR Summit Webinar — March',
   TIMESTAMP '2026-03-19 15:30:00',
   MAP['program_id', '2003', 'session_id', 'HR-MAR-01', 'channel', 'Webinar']),
  (3006, 1006, 'Visit Webpage', '/resources/whitepaper-analytics',
   TIMESTAMP '2026-03-19 17:00:00',
   MAP['program_id', '2006', 'partner', 'CoBrand-A', 'channel', 'Email']),
  (3012, 1010, 'CRM Sync Touch', 'batch_score_refresh',
   TIMESTAMP '2026-03-19 20:00:06',
   MAP['program_id', '2001', 'source', 'nightly_sync', 'channel', 'System']),
  -- 2026-03-20 — full day: morning block [08:00,12:00)
  (3008, 1008, 'Open Email', 'Customer Stories Digest — March',
   TIMESTAMP '2026-03-20 09:45:00',
   MAP['program_id', '2009', 'story_slug', 'acme-rollout', 'channel', 'Email']),
  (3013, 1004, 'Click Link', 'customer-stories/acme',
   TIMESTAMP '2026-03-20 10:22:00',
   MAP['program_id', '2009', 'utm_campaign', 'stories_mar', 'channel', 'Email']),
  (3014, 1006, 'Visit Webpage', '/products/hcm/overview',
   TIMESTAMP '2026-03-20 11:40:00',
   MAP['program_id', '2002', 'page_section', 'hero', 'channel', 'Web']),
  -- Anchor: advances watermark past 12:00 so [08:00,12:00) on 2026-03-20 can emit
  (3015, 1001, 'Open Email', 'Q1_Product_Update_Newsletter',
   TIMESTAMP '2026-03-20 12:00:06',
   MAP['program_id', '2001', 'campaignId', '5543', 'channel', 'Email']),
  -- Afternoon [12:00,16:00)
  (3005, 1005, 'Open Email', 'Nurture Stream A — Product / Episode 3',
   TIMESTAMP '2026-03-20 12:18:00',
   MAP['program_id', '2005', 'sequence_step', '3', 'channel', 'Email']),
  (3004, 1004, 'Fill Out Form', 'demo_request_2026',
   TIMESTAMP '2026-03-20 13:50:00',
   MAP['program_id', '2002', 'webpageUrl', 'https://www.workday.com/en-us/demo.html', 'referrer', 'google']),
  (3003, 1003, 'Visit Webpage', 'https://www.workday.com/en-us/demo.html',
   TIMESTAMP '2026-03-20 14:30:00',
   MAP['program_id', '2002', 'form_context', 'demo_request', 'channel', 'Web']),
  (3002, 1002, 'Click Link', 'linkedin.com/campaign/q1-hr',
   TIMESTAMP '2026-03-20 14:55:00',
   MAP['program_id', '2004', 'utm_source', 'linkedin', 'channel', 'Social Media']),
  (3001, 1001, 'Open Email', 'Q1_Product_Update_Newsletter',
   TIMESTAMP '2026-03-20 15:04:22',
   MAP['program_id', '2001', 'campaignId', '5543', 'channel', 'Email']),
  -- Anchor: advances watermark past 16:00 so [12:00,16:00) on 2026-03-20 can emit
  (3016, 1007, 'Attended Webinar', 'HR Summit — follow-up resource',
   TIMESTAMP '2026-03-20 16:00:06',
   MAP['program_id', '2003', 'asset', 'recording', 'channel', 'Webinar']),
  -- Evening [16:00,20:00)
  (3017, 1005, 'Open Email', 'Post-webinar nurture — Episode 1',
   TIMESTAMP '2026-03-20 17:35:00',
   MAP['program_id', '2005', 'sequence_step', '1', 'channel', 'Email']),
  (3018, 1002, 'Visit Webpage', '/events/roadshow-sf',
   TIMESTAMP '2026-03-20 18:50:00',
   MAP['program_id', '2004', 'utm_source', 'email', 'channel', 'Social Media']),
  -- Anchor: advances watermark past 20:00 so [16:00,20:00) on 2026-03-20 can emit
  (3019, 1008, 'Open Email', 'Customer Stories Digest — March',
   TIMESTAMP '2026-03-20 20:00:06',
   MAP['program_id', '2009', 'story_slug', 'evening_send', 'channel', 'Email']),
  -- Late evening [20:00,24:00)
  (3020, 1003, 'Paid Click', 'remarketing-hcm',
   TIMESTAMP '2026-03-20 21:15:00',
   MAP['program_id', '2008', 'keyword', 'workday payroll', 'channel', 'Paid Search']),
  -- Anchor: closes 2026-03-20 [20:00,24:00)
  (3021, 1004, 'Form Abandon', 'demo_request_2026',
   TIMESTAMP '2026-03-21 00:00:06',
   MAP['program_id', '2002', 'step', 'partial', 'channel', 'Web']),
  -- 2026-03-21 — repeat engagement same newsletter (distinct activities for COUNT DISTINCT)
  (3010, 1002, 'Open Email', 'Q1_Product_Update_Newsletter',
   TIMESTAMP '2026-03-21 12:04:22',
   MAP['program_id', '2001', 'campaignId', '5543', 'channel', 'Email']),
  (3011, 1003, 'Open Email', 'Q1_Product_Update_Newsletter',
   TIMESTAMP '2026-03-21 12:04:22',
   MAP['program_id', '2001', 'campaignId', '5543', 'channel', 'Email']),
  -- Anchor: closes [08:00,12:00) and [12:00,16:00) on 2026-03-21 as new events arrive
  (3022, 1001, 'Click Link', 'Q1_Product_Update_Newsletter',
   TIMESTAMP '2026-03-21 16:00:06',
   MAP['program_id', '2001', 'link', 'footer_unsubscribe', 'channel', 'Email']);
