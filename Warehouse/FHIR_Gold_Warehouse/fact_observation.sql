CREATE TABLE dbo.fact_observation
(
    observation_id      VARCHAR(255),
    patient_id          VARCHAR(255),
    status               VARCHAR(100),
    code_system          VARCHAR(500),
    code                 VARCHAR(255),
    code_display         VARCHAR(1000),
    value_quantity       FLOAT,
    value_unit           VARCHAR(255),
    effective_datetime   DATETIME2,
    source_last_updated  DATETIME2,
    effective_from       DATETIME2,
    effective_to         DATETIME2,
    is_current           BIT
);