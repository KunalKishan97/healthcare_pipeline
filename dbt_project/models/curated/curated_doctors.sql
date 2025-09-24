{{ config(materialized='incremental', unique_key='DoctorID') }}

WITH dedup AS (
    SELECT
        DoctorID,
        DoctorName,
        Specialization,
        ingestion_date,
        ROW_NUMBER() OVER (PARTITION BY DoctorID ORDER BY ingestion_date DESC) AS rn
    FROM {{ ref('raw_doctors') }}
)
SELECT
    DoctorID,
    DoctorName,
    Specialization,
    ingestion_date
FROM dedup
WHERE rn = 1
