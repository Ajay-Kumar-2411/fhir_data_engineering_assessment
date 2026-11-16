CREATE TABLE dbo.fact_condition
(
    condition_id         VARCHAR(255),
    patient_id            VARCHAR(255),
    clinical_status       VARCHAR(100),
    verification_status   VARCHAR(100),
    code_system           VARCHAR(500),
    code                  VARCHAR(255),
    code_display          VARCHAR(1000),
    onset_date            DATE,
    source_last_updated   DATETIME2,
    effective_from        DATETIME2,
    effective_to          DATETIME2,
    is_current            BIT
);