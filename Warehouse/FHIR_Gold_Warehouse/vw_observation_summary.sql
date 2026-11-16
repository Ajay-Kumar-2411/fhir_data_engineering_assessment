CREATE OR ALTER VIEW dbo.vw_observation_summary
AS
SELECT
    observation_id,
    patient_id,
    status,
    code_system,
    code,
    code_display,
    value_quantity,
    value_unit,
    effective_datetime,
    source_last_updated
FROM dbo.fact_observation
WHERE is_current = 1;