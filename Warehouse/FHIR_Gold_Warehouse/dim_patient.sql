CREATE TABLE dbo.dim_patient
(
    patient_id          VARCHAR(255),
    active              BIT,
    gender              VARCHAR(50),
    birth_date          DATE,
    deceased            BIT,
    source_last_updated DATETIME2,
    effective_from      DATETIME2,
    effective_to        DATETIME2,
    is_current          BIT,
    source_file         VARCHAR(1000)
);