{{ config(materialized='view') }}

SELECT
    abn,
    TRIM(company_name) AS company_name,
    entity_type,
    state,
    postcode,
    registration_date
FROM {{ source('raw', 'abr_raw') }}
WHERE LENGTH(abn) = 11