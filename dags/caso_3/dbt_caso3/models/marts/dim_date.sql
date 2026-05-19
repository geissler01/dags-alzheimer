WITH date_series AS (
    SELECT 
        CAST(d AS DATE) AS date_id
    FROM GENERATE_SERIES(
        '1900-01-01'::DATE, 
        '2030-12-31'::DATE, 
        '1 day'::INTERVAL
    ) d
),

dim_date AS (
    SELECT 
        date_id,
        EXTRACT(YEAR FROM date_id) AS year,
        EXTRACT(MONTH FROM date_id) AS month,
        EXTRACT(DAY FROM date_id) AS day,
        EXTRACT(QUARTER FROM date_id) AS quarter,
        EXTRACT(ISODOW FROM date_id) AS day_of_week,
        TO_CHAR(date_id, 'Month') AS month_name,
        TO_CHAR(date_id, 'Day') AS day_name,
        CASE WHEN EXTRACT(ISODOW FROM date_id) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
    FROM date_series
)

SELECT * FROM dim_date
