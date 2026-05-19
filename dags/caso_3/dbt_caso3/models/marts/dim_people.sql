WITH distinct_people AS (
    SELECT DISTINCT person_name
    FROM {{ ref('int_content_people_split') }}
),

dim_people AS (
    SELECT 
        MD5(person_name) AS person_id,
        person_name
    FROM distinct_people
)

SELECT * FROM dim_people
