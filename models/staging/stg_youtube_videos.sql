-- Staging model: Clean and transform raw YouTube videos

{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_youtube_videos') }}
),

cleaned AS (
    SELECT
        video_id,
        TRIM(title) AS title_clean,
        TRIM(description) AS description_clean,
        TRIM(channel_title) AS channel_title_clean,
        published_at,
        TO_DATE(published_at) AS published_date,
        YEAR(published_at) AS year,
        QUARTER(published_at) AS quarter,
        MONTH(published_at) AS month,
        MONTHNAME(published_at) AS month_name,
        DAYOFWEEK(published_at) AS day_of_week,
        url,
        -- Split comma-separated foods into array
        SPLIT(mentioned_foods, ',') AS extracted_foods,
        year AS year_int,
        loaded_at
    FROM source
    WHERE title IS NOT NULL
      AND published_at IS NOT NULL
)

SELECT * FROM cleaned
