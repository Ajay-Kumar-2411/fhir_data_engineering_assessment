CREATE TABLE dbo.fact_encounter
(
    encounter_id        VARCHAR(255),
    patient_id          VARCHAR(255),
    status              VARCHAR(100),
    class_code          VARCHAR(100),
    class_display       VARCHAR(255),
    period_start        DATETIME2,
    period_end          DATETIME2,
    source_last_updated DATETIME2,
    effective_from      DATETIME2,
    effective_to        DATETIME2,
    is_current          BIT
);