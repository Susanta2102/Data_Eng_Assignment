{{ config(materialized='view') }}

SELECT
    url,
    COALESCE(NULLIF(company_name, ''), 'Unknown') AS company_name,
    CASE 
        WHEN industry IN ('Technology', 'Retail') THEN industry
        ELSE 'Other' 
    END AS industry
FROM {{ source('raw', 'common_crawl_raw') }}
WHERE url IS NOT NULL