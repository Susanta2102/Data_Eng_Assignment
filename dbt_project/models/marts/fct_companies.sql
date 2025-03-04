{{ config(
    materialized='table',
    unique_key='url'
) }}

WITH cc AS (
    SELECT 
        *,
        LOWER(REGEXP_REPLACE(company_name, '[^a-zA-Z0-9]', '')) AS clean_name 
    FROM {{ ref('stg_common_crawl') }}
),

abr AS (
    SELECT 
        *,
        LOWER(REGEXP_REPLACE(company_name, '[^a-zA-Z0-9]', '')) AS clean_name 
    FROM {{ ref('stg_abr') }}
)

SELECT
    cc.url,
    cc.company_name,
    cc.industry,
    abr.abn,
    abr.entity_type,
    abr.state,
    abr.postcode,
    abr.registration_date
FROM cc
LEFT JOIN abr USING (clean_name)