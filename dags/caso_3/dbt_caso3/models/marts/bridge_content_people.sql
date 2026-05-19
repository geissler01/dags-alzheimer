WITH split_people AS (
    SELECT * FROM {{ ref('int_content_people_split') }}
),

people_dim AS (
    SELECT * FROM {{ ref('dim_people') }}
),

bridge_content_people AS (
    SELECT 
        MD5(s.contentId || s.person_name || s.role) AS bridge_id,
        s.contentId AS content_id,
        d.person_id,
        s.role
    FROM split_people s
    INNER JOIN people_dim d ON s.person_name = d.person_name
)

SELECT * FROM bridge_content_people
