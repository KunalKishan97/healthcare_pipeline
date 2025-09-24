{{ config(materialized='incremental', unique_key='PatientID') }}

WITH dedup AS (
    SELECT
        PatientID,
        firstname,
        lastname,
        email,
        ingestion_date,
        ROW_NUMBER() OVER (PARTITION BY PatientID ORDER BY ingestion_date DESC) AS rn
    FROM {{ ref('raw_patients') }}
)
SELECT
    PatientID,
    firstname,
    lastname,
    email,
    ingestion_date
FROM dedup
WHERE rn = 1
