{{ config(materialized='incremental', unique_key='AppointmentID') }}

WITH dedup AS (
    SELECT
        AppointmentID,
        PatientID,
        DoctorID,
        Date,
        ingestion_date,
        ROW_NUMBER() OVER (PARTITION BY AppointmentID ORDER BY ingestion_date DESC) AS rn
    FROM {{ ref('raw_appointments') }}
)
SELECT
    AppointmentID,
    PatientID,
    DoctorID,
    Date,
    ingestion_date
FROM dedup
WHERE rn = 1
