{{ config(materialized='incremental', unique_key='ProcedureID') }}

WITH dedup AS (
    SELECT
        AppointmentID,
        ProcedureID,
        ProcedureName,
        ingestion_date,
        ROW_NUMBER() OVER (
            PARTITION BY AppointmentID, ProcedureID
            ORDER BY ingestion_date DESC
        ) AS rn
    FROM {{ ref('raw_medicalprocedure') }}
)
SELECT
    AppointmentID,
    ProcedureID,
    ProcedureName,
    ingestion_date
FROM dedup
WHERE rn = 1
