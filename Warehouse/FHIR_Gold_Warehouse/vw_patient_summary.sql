CREATE OR ALTER VIEW dbo.vw_patient_summary
AS
SELECT
    patient_id,
    active,
    gender,
    birth_date,
    deceased,
    source_last_updated,
    effective_from,
    effective_to,
    is_current
FROM dbo.dim_patient
WHERE is_current = 1;