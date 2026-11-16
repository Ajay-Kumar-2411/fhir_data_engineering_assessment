CREATE OR ALTER VIEW dbo.vw_condition_summary
AS
SELECT
    condition_id,
    patient_id,
    clinical_status,
    verification_status,
    code_system,
    code,
    code_display,
    onset_date,
    source_last_updated
FROM dbo.fact_condition
WHERE is_current = 1;