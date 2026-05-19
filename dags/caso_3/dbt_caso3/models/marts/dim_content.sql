WITH unified_content AS (
    SELECT * FROM {{ ref('int_unified_content') }}
),

dim_content AS (
    SELECT 
        contentId AS content_id,
        title,
        contentType AS content_type,
        releaseYear AS release_year,
        sourcePlatform AS source_platform
    FROM unified_content
)

SELECT * FROM dim_content
