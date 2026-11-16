# FHIR API Data Ingestion & Analytics

## 1. Project Overview

This project implements an end-to-end healthcare data ingestion and analytics pipeline using **Microsoft Fabric** and the public **HAPI FHIR R4 API**.

The solution ingests four FHIR resources:

- Patient
- Encounter
- Observation
- Condition

The pipeline follows a **Medallion Architecture**:

**FHIR API → Raw → Bronze → Silver → Gold → Reporting**

The implementation covers incremental ingestion, API pagination, raw response preservation, metadata capture, Delta-based processing, cleansing and deduplication, SCD Type 2 versioning, analytical modeling, Power BI reporting, and Fabric Data Pipeline orchestration.

The implementation is intentionally modular so that the same ingestion and transformation patterns can be reused across the supported FHIR resources.

---

## 2. Business / Engineering Objective

The objective is to build a repeatable data engineering workflow that:

1. Extracts FHIR data incrementally rather than performing an unrestricted full load.
2. Preserves the original API responses for traceability and replay.
3. Converts the source data into structured Delta tables.
4. Cleans and deduplicates the data in the Silver layer.
5. Detects changes to resources and maintains historical versions using SCD Type 2.
6. Publishes analytics-ready data through a Fabric Warehouse.
7. Publishes analytics-ready Gold Warehouse tables for reporting and BI consumption.
8. Orchestrates the complete workflow through a Fabric Data Pipeline.

---

## 3. Technology Stack

| Component | Technology |
|---|---|
| Cloud data platform | Microsoft Fabric |
| Source API | HAPI FHIR R4 |
| Source format | FHIR JSON |
| Processing engine | Apache Spark / PySpark |
| Raw storage | Fabric Lakehouse Files |
| Structured storage | Delta Lake |
| Analytical serving | Fabric Warehouse |
| Orchestration | Fabric Data Pipeline |
| Historical versioning | SCD Type 2 |
| Source control | Not configured in the default Fabric workspace |
| Reporting | Power BI using Gold Warehouse tables |

---

## 4. Microsoft Fabric Architecture

The assignment is implemented in one Fabric workspace containing:

### Lakehouses

- `FHIR_Raw_Lakehouse`
- `FHIR_Bronze_Lakehouse`
- `FHIR_Silver_Lakehouse`

### Warehouse

- `FHIR_Gold_Warehouse`

### Spark Notebooks

1. `01_fhir_ingestion`
2. `02_raw_to_bronze`
3. `03_bronze_to_silver`
4. `04_silver_to_gold`

### Pipeline

- `FHIR_Ingestion_Pipeline`

The workspace arrangement is appropriate for the assignment environment. The default workspace does not provide Git integration, so the final solution is packaged and submitted as a ZIP file. In a larger production platform, individual layers or domains may be separated into multiple workspaces depending on security, ownership, deployment and lifecycle requirements.

---

# 5. High-Level Architecture

```text
                         HAPI FHIR R4 API
                               │
                               │ Incremental _lastUpdated
                               │ + Pagination
                               ▼
                    ┌──────────────────────┐
                    │ 01_fhir_ingestion    │
                    │ Spark / PySpark       │
                    │ Retry + Metadata     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ FHIR_Raw_Lakehouse   │
                    │                      │
                    │ JSON responses as-is │
                    │ Date partitioned     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ 02_raw_to_bronze     │
                    │ Extract FHIR entries │
                    │ Add metadata         │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ FHIR_Bronze_Lakehouse│
                    │ Delta tables         │
                    │ Source + load metadata│
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ 03_bronze_to_silver  │
                    │ Clean / Deduplicate  │
                    │ Hash + SCD Type 2    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ FHIR_Silver_Lakehouse│
                    │ Curated Delta tables │
                    │ Historical versions  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ 04_silver_to_gold    │
                    │ Analytical modeling  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ FHIR_Gold_Warehouse  │
                    │ Dimensions / Facts   │
                    └──────────┬───────────┘
                               │
                               ▼
                         Power BI Dashboard
```

A rendered architecture diagram is included separately in the project documentation package.

---

# 6. FHIR Source

The source system is the public HAPI FHIR R4 server:

```text
https://hapi.fhir.org/baseR4
```

The implementation uses the FHIR search API for:

- `Patient`
- `Encounter`
- `Observation`
- `Condition`

The incremental filter uses `_lastUpdated`.

Conceptually:

```text
<Resource>?_count=100
&_lastUpdated=ge<start_timestamp>
&_lastUpdated=lt<end_timestamp>
```

The API returns a FHIR `Bundle`. Each Bundle contains resource entries and, when additional pages are available, pagination links.

---

# 7. Notebook 01 — FHIR Ingestion

### Responsibility

`01_fhir_ingestion` is responsible only for extracting data from the external FHIR API and storing the original responses in the Raw layer.

### Processing flow

For each configured daily window and each FHIR resource:

1. Construct the `_lastUpdated` filter.
2. Call the FHIR endpoint.
3. Capture the API response.
4. Record extraction metadata.
5. Write the complete response as JSON.
6. Read the FHIR `next` pagination link.
7. Continue until no next page exists.
8. Record the data-save timestamp.

### Resource processing order

The pipeline follows the required resource order:

```text
Patient
   ↓
Encounter
   ↓
Observation
   ↓
Condition
```

### Pagination

Pagination is driven by the `next` URL supplied by the FHIR Bundle.

This is preferable to assuming a fixed number of pages because the number of returned records can change between executions.

### Retry handling

Transient API failures are handled using retries with exponential backoff.

Transient HTTP statuses include:

- 429
- 500
- 502
- 503
- 504

A request timeout is also configured.

---

# 8. Raw Layer

## Storage

Raw data is stored in:

`FHIR_Raw_Lakehouse`

under:

```text
Files/raw/
```

The folder structure is:

```text
Files/raw/
├── patient/
│   ├── ingestion_date=2026-11-12/
│   │   ├── page_001.json
│   │   └── page_002.json
│   ├── ingestion_date=2026-11-13/
│   └── ingestion_date=2026-11-14/
│
├── encounter/
├── observation/
└── condition/
```

### Raw design principles

The Raw layer intentionally stores the API response without flattening the FHIR structure.

This provides:

- Source preservation
- Replay capability
- Traceability
- Auditability
- Separation between extraction and transformation

### Idempotency strategy

The Raw layout uses deterministic resource/date partitions.

When an affected date is re-ingested successfully, the corresponding date partition can be replaced instead of creating execution-specific directories.


---

# 9. Raw Metadata

In addition to the raw JSON responses, the ingestion process records operational metadata such as:

| Metadata | Purpose |
|---|---|
| `resource_type` | FHIR resource being ingested |
| `window_date` | Incremental extraction date |
| `page_number` | API pagination sequence |
| `api_url_or_params` | Request URL / query parameters |
| `extraction_timestamp` | Time API response was obtained |
| `data_saved_timestamp` | Time response was persisted |
| `records_extracted` | Number of resources in the page |
| `raw_file_path` | Location of persisted response |
| `status` | Extraction status |

This provides operational lineage from the API request to the stored response.

---

# 10. Notebook 02 — Raw to Bronze

### Responsibility

`02_raw_to_bronze` converts the raw FHIR Bundles into structured Delta tables.

The notebook does not perform the main business-level cleansing or SCD Type 2 logic. Those responsibilities belong to Silver.

### Processing

1. Read Raw JSON.
2. Parse the FHIR Bundle.
3. Explode `entry[]`.
4. Extract `entry.resource`.
5. Preserve the original source file.
6. Join/carry extraction and save metadata.
7. Derive ingestion date.
8. Write one Delta table per resource.

### Bronze tables

```text
bronze_patient
bronze_encounter
bronze_observation
bronze_condition
```

### Bronze role

Bronze is a structured representation of the source data while retaining source lineage and ingestion metadata.

It is intentionally closer to the source than the Silver analytical model.

---

# 11. Notebook 03 — Bronze to Silver

### Responsibility

`03_bronze_to_silver` creates curated Silver datasets.

Main responsibilities:

- Cleaning
- Standardization
- Deduplication
- Record hashing
- SCD Type 2 versioning

### Silver tables

```text
silver_patient
silver_encounter
silver_observation
silver_condition
```

### Record identity

Each resource retains its FHIR resource identifier.

Examples:

```text
patient_id
encounter_id
observation_id
condition_id
```

### Record hash

A record hash is used as a comparison mechanism for identifying changes to a logical resource.

Conceptually:

```text
Same resource ID
        │
        ├── Same record_hash
        │       → no content change
        │
        └── Different record_hash
                → resource changed
                → create new SCD2 version
```

---

# 12. SCD Type 2

SCD Type 2 is implemented in Silver to retain historical resource versions.

Important columns:

| Column | Description |
|---|---|
| `effective_from` | Start of version validity |
| `effective_to` | End of version validity |
| `is_current` | Indicates the active version |
| `record_hash` | Detects content changes |

### Initial version

For the first/current version:

```text
effective_from = extraction/change timestamp
effective_to   = NULL
is_current     = true
```

Therefore, having `effective_to = NULL` for all records after the initial load is expected when no historical version has yet been superseded.

### Changed resource

When a resource changes in a later load:

```text
Previous version
effective_to = new effective_from
is_current   = false

New version
effective_to = NULL
is_current   = true
```

This preserves historical state instead of overwriting the previous version.

---

# 13. Notebook 04 — Silver to Gold

### Responsibility

`04_silver_to_gold` converts curated Silver resources into analytics-friendly Warehouse tables.

Gold does not read directly from the FHIR API or Raw layer.

It reads from Silver.

### Analytical model

```text
Silver
   │
   ├── Patient       → dim_patient
   ├── Encounter     → fact_encounter
   ├── Observation   → fact_observation
   └── Condition     → fact_condition
```

### Patient dimension

`dim_patient` exposes commonly used patient attributes such as:

- Patient identifier
- Active status
- Gender
- Birth date
- Deceased status
- Source last-updated timestamp
- SCD Type 2 columns
- Source file

### Encounter fact

`fact_encounter` extracts analytical fields such as:

- Encounter identifier
- Patient identifier
- Encounter status
- Encounter class
- Encounter period

### Observation fact

`fact_observation` extracts fields such as:

- Observation identifier
- Patient identifier
- Observation status
- Coding system
- Code
- Code display
- Quantity value
- Quantity unit
- Effective timestamp

### Condition fact

`fact_condition` extracts fields such as:

- Condition identifier
- Patient identifier
- Clinical status
- Verification status
- Coding system
- Diagnosis code
- Code display
- Onset date

---

# 14. Gold Warehouse

The analytical serving layer is:

`FHIR_Gold_Warehouse`

The Warehouse contains the physical Gold tables:

```text
dbo.dim_patient
dbo.fact_encounter
dbo.fact_observation
dbo.fact_condition
```

The Gold layer is designed for consumption by SQL/reporting workloads rather than for preserving the full nested FHIR representation.

---

# 15. Reporting and Power BI

The reporting layer uses the physical Gold Warehouse tables directly rather than relying on SQL views.

The Power BI report is built on:

```text
dbo.dim_patient
dbo.fact_encounter
dbo.fact_observation
dbo.fact_condition
```

### Power BI Dashboard

A one-page Power BI dashboard has been created to provide a concise analytical view of the FHIR data.

The dashboard includes:

- Total Patients
- Active Patients
- Total Encounters
- Total Observations
- Total Conditions
- Patients by Gender
- Encounters by Status

### Power BI Semantic Model

The Power BI model follows a simple **star-schema design**.

`dim_patient` is the central dimension table and the three clinical tables are fact tables.

```text
                         dim_patient
                        [patient_id]
                              │
             ┌────────────────┼────────────────┐
             │                │                │
            1│               1│               1│
             │                │                │
             ▼                ▼                ▼
      fact_encounter   fact_observation   fact_condition
       [patient_id]      [patient_id]      [patient_id]
             *                *                *
```

### Relationships

The following relationships are configured in Power BI:

| From table | From column | Cardinality | To table | To column | Cross-filter direction |
|---|---|---|---|---|---|
| `dim_patient` | `patient_id` | 1 → * | `fact_encounter` | `patient_id` | Single |
| `dim_patient` | `patient_id` | 1 → * | `fact_observation` | `patient_id` | Single |
| `dim_patient` | `patient_id` | 1 → * | `fact_condition` | `patient_id` | Single |

The relationship key is the FHIR patient identifier. This allows patient-level filtering from `dim_patient` to the related clinical facts.

The model intentionally uses a **single filter direction from the patient dimension to the fact tables**. This keeps the model predictable and avoids unnecessary bidirectional filtering.

### Table Roles

| Table | Model role | Purpose |
|---|---|---|
| `dim_patient` | Dimension | Patient-level descriptive attributes |
| `fact_encounter` | Fact | Encounter activity and status |
| `fact_observation` | Fact | Clinical observations and measurements |
| `fact_condition` | Fact | Patient conditions/diagnoses |

### Technical / SCD2 Columns

The Gold tables retain historical/versioning fields needed for traceability and SCD Type 2 processing:

- `effective_from`
- `effective_to`
- `is_current`
- `source_last_updated`

These are technical fields rather than primary dashboard attributes and are hidden from the report-facing view where appropriate.

`is_current` is used by the Power BI measures to restrict reporting metrics to the current SCD Type 2 version of each logical resource.

### Power BI Measures

The dashboard uses measures for current-record reporting rather than counting historical SCD2 versions as separate business entities.

```DAX
Current Patients =
CALCULATE(
    DISTINCTCOUNT(dim_patient[patient_id]),
    dim_patient[is_current] = TRUE()
)

Active Patients =
CALCULATE(
    [Current Patients],
    dim_patient[active] = TRUE()
)

Current Encounters =
CALCULATE(
    DISTINCTCOUNT(fact_encounter[encounter_id]),
    fact_encounter[is_current] = TRUE()
)

Current Observations =
CALCULATE(
    DISTINCTCOUNT(fact_observation[observation_id]),
    fact_observation[is_current] = TRUE()
)

Current Conditions =
CALCULATE(
    DISTINCTCOUNT(fact_condition[condition_id]),
    fact_condition[is_current] = TRUE()
)
```

### Dashboard Visuals and Field Mapping

| Visual | Type | Data / measure |
|---|---|---|
| Total Patients | Card | `[Current Patients]` |
| Active Patients | Card | `[Active Patients]` |
| Total Encounters | Card | `[Current Encounters]` |
| Total Observations | Card | `[Current Observations]` |
| Total Conditions | Card | `[Current Conditions]` |
| Patients by Gender | Donut chart | `dim_patient[gender]` + `[Current Patients]` |
| Encounters by Status | Column chart | `fact_encounter[status]` + `[Current Encounters]` |

### Reporting Flow

The complete reporting flow is:

```text
FHIR API
   │
   ▼
Raw
   │
   ▼
Bronze
   │
   ▼
Silver
   │
   ▼
Gold Warehouse
   │
   ├── dim_patient
   ├── fact_encounter
   ├── fact_observation
   └── fact_condition
           │
           ▼
   Power BI Semantic Model
           │
           ├── 1 → * fact_encounter
           ├── 1 → * fact_observation
           └── 1 → * fact_condition
           │
           ▼
   Power BI Dashboard
```

The Power BI report does not use the reporting views as its data source.

# 16. Orchestration

The complete workflow is orchestrated through:

`FHIR_Ingestion_Pipeline`

Execution order:

```text
01_FHIR_Ingestion
        │
        ▼
02_Raw_to_Bronze
        │
        ▼
03_Bronze_to_Silver
        │
        ▼
04_Silver_to_Gold
        │
```

Dependencies are configured sequentially using **On Success**.

This ensures that:

- Bronze does not execute before Raw ingestion completes.
- Silver does not execute before Bronze succeeds.
- Gold does not execute before Silver succeeds.
- The Gold Warehouse is populated before Power BI reporting.

---

# 17. Data Lineage

The end-to-end lineage is:

```text
HAPI FHIR API
      │
      ▼
Raw JSON Bundle
      │
      ▼
Bronze Delta Resource
      │
      ▼
Cleaned / Deduplicated Silver
      │
      ▼
SCD Type 2 Historical Silver
      │
      ▼
Gold Analytical Tables
      │
      ▼
Power BI Dashboard
      │
      ▼
Analytics / BI
```

Metadata provides additional lineage from the API request through the storage and transformation layers.

---

# 18. Data Quality / Validation

No separate data quality or data validation notebook is included. Validation is performed within the existing ingestion/transformation workflow and through final pipeline and table checks.

Validation should cover:

### Raw

- All four resource folders exist.
- Daily ingestion partitions exist.
- Paginated responses are present where the API returned multiple pages.
- Raw responses remain valid JSON/FHIR Bundles.

### Bronze

- Four Delta tables exist.
- Tables contain records.
- Source and ingestion metadata are populated.

### Silver

- Four curated tables exist.
- Resource identifiers are populated.
- Duplicate logical records are removed.
- `record_hash` is populated.
- SCD2 columns are populated correctly.

### Gold

- Four analytical Warehouse tables exist.
- Tables contain records.
- Gold tables contain data for Power BI reporting.

### Pipeline

All four core notebook activities complete successfully in the intended order.

---

# 19. Current Execution Result

The final pipeline execution has completed successfully through all four core notebooks:

```text
01 FHIR Ingestion           SUCCESS
02 Raw → Bronze             SUCCESS
03 Bronze → Silver          SUCCESS
04 Silver → Gold            SUCCESS
```

The Power BI dashboard has also been created using the Gold Warehouse tables.

The initial Silver load having `effective_to = NULL` for current records is expected. A non-NULL `effective_to` appears when an existing resource version is superseded by a later changed version.

---

# 20. Reprocessing and Idempotency

The design avoids execution-level duplication in Raw by using deterministic date/resource partitions.

For a date that must be reprocessed:

1. Re-ingest the affected API window.
2. Replace the corresponding Raw date partition.
3. Rebuild the affected Bronze data.
4. Re-run Silver change detection and SCD Type 2 processing.
5. Refresh Gold.
6. Refresh the Power BI dataset/report.

The objective is to make reruns predictable without accumulating duplicate execution directories.

---

# 22. Security and Configuration Considerations

The source API used by this assignment is public.

For production implementation, the following should be externalized:

- API credentials, if required
- Secrets
- Endpoint configuration
- Environment-specific Lakehouse/Warehouse identifiers
- Pipeline parameters
- Retry and timeout settings

Secrets should not be stored directly in notebooks or source control.

---

# 23. Submission Package Structure

Git source control is not configured because the default Fabric workspace does not allow Git integration. The final solution will therefore be submitted as a ZIP file.

Recommended ZIP structure:

```text
FHIR_Data_Engineering_Assignment/
│
├── README.md
├── notebooks/
│   ├── 01_fhir_ingestion.ipynb
│   ├── 02_raw_to_bronze.ipynb
│   ├── 03_bronze_to_silver.ipynb
│   └── 04_silver_to_gold.ipynb
│
├── pipeline/
│   └── FHIR_Ingestion_Pipeline/
│
├── warehouse/
│   └── FHIR_Gold_Warehouse/
│
├── powerbi/
│   └── FHIR_Analytics_Dashboard.pbix
│
├── architecture/
│   └── fhir_architecture.png
│
└── screenshots/
    └── validation/
```

Only the final versions of the notebooks and supporting artifacts should be included in the ZIP.

---

# 24. Execution Instructions

### Step 1 — Start the pipeline

Run:

```text
FHIR_Ingestion_Pipeline
```

### Step 2 — Monitor notebook activities

Expected order:

```text
01 → 02 → 03 → 04
```

### Step 3 — Validate Raw

Check the resource/date folders and page files.

### Step 4 — Validate Bronze

Check the four Bronze Delta tables.

### Step 5 — Validate Silver

Check cleansing, deduplication, `record_hash`, and SCD Type 2 columns.

### Step 6 — Validate Gold

Check the four Warehouse tables.

### Step 7 — Validate reporting

Open the Power BI dashboard and confirm that the Gold Warehouse tables populate the report.

---

# 25. Potential Production Enhancements

The current implementation satisfies the assignment scope. A production implementation could additionally introduce:

- Centralized pipeline execution logging
- Watermark/control tables
- Automated data quality rules
- Schema evolution management
- Alerting and failure notifications
- Environment-specific configuration
- CI/CD deployment between Fabric environments
- Row-level security where applicable
- API rate-limit monitoring
- Incremental Gold processing instead of rebuilding complete analytical tables

These are considered extensions rather than requirements for this assignment.

---

# 26. Assignment Requirement Coverage

| Assignment Requirement | Status |
|---|---|
| Microsoft Fabric implementation | Complete |
| HAPI FHIR R4 API | Complete |
| Patient resource | Complete |
| Encounter resource | Complete |
| Observation resource | Complete |
| Condition resource | Complete |
| Incremental 2–3 day ingestion | Complete |
| API pagination | Complete |
| Raw JSON preservation | Complete |
| Bronze Delta layer | Complete |
| Silver cleansing | Complete |
| Deduplication | Complete |
| API/load metadata | Complete |
| Daily change detection | Complete |
| SCD Type 2 | Complete |
| Gold Warehouse | Complete |
| Power BI dashboard | Complete |
| Fabric Data Pipeline | Complete |
| Modular/reusable implementation | Complete |
| Pipeline/table documentation | Complete |

### Optional extensions

| Extension | Status |
|---|---|
| XML ingestion | Not implemented |

---

# 27. Conclusion

The project implements a complete FHIR data engineering workflow on Microsoft Fabric.

The architecture separates source extraction, structured storage, data refinement, historical versioning and analytical serving into clear layers. This provides traceability at the Raw/Bronze levels, controlled transformation in Silver, and a reporting-oriented model in Gold.

The final solution is designed to be repeatable, modular and suitable for extension beyond the assignment scope.
