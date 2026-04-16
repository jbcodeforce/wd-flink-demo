INSERT INTO dim_activities
with activities_leads as (
    SELECT
    sa.activity_id,
    sa.lead_id,
    sa.activity_type,
    sa.primary_attribute_value,
    sa.activity_ts as activity_ts,
    cast(sa.metadata['program_id'] as int) as program_id,
    src_leads.email as lead_email,
    src_leads.first_name as `lead_first_name`,
    src_leads.last_name as `lead_last_name`,
    src_leads.lead_score as `lead_score`,
    src_leads.status as `lead_status`
    FROM src_activities sa
    LEFT JOIN src_leads ON sa.lead_id = src_leads.m_id
)
SELECT
  al.activity_id,
  al.lead_id,
  al.activity_type,
  al.primary_attribute_value,
  al.activity_ts,
  al.lead_email as `lead_email`,
  al.lead_first_name,
  al.lead_last_name as `lead_last_name`,
  al.lead_score as `lead_score`,
  al.lead_status as `lead_status`,
  al.program_id,
  p.name as `program_name`, 
  p.channel as `program_channel`,
  p.status as `program_status`
FROM activities_leads al
  left JOIN src_mkt_pgm p ON al.program_id = p.program_id
WHERE p.status <> 'Draft'

