SELECT *
FROM {{ source('raw', 'patients') }}
