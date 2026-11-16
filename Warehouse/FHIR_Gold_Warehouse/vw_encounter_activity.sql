CREATE OR ALTER VIEW dbo.vw_encounter_activity
AS
SELECT
    encounter_id,
    patient_id,
    status,
    class_code,
    class_display,
    period_start,
    period_end,
    source_last_updated
FROM dbo.fact_encounter
WHERE is_current = 1;