-- Unnest the foods array to create one row per video-food pair and using qualify to ensure no duplicates are present 

WITH source AS (
    SELECT * FROM {{ ref('stg_youtube_videos') }}
),

unnested AS (
    SELECT
        s.video_id,
        s.title_clean,
        s.published_date,
        s.year,
        s.month,
        -- Clean the value directly from the flattened result
        CASE 
            WHEN LOWER(TRIM(f.value)) IN ('carb','carbs','carbohydrate','carbohydrates') 
                THEN 'carbohydrates'
            WHEN LOWER(TRIM(f.value)) = 'protein'  
                THEN 'protein'
            WHEN LOWER(TRIM(f.value)) = 'fat'
                THEN 'fat'
            WHEN LOWER(TRIM(f.value)) IN ('protein bar','bars','bar','energy bar') 
                THEN 'protein bar'
            WHEN LOWER(TRIM(f.value)) IN ('gel','gels','energy gel','energy gels') 
                THEN 'energy gel'
            WHEN LOWER(TRIM(f.value)) = 'sports drink'
                THEN 'sports drink'
            ELSE LOWER(TRIM(f.value))
        END AS food_name
    FROM source s,
    LATERAL FLATTEN(input => s.extracted_foods) f
    WHERE f.value IS NOT NULL 
      AND TRIM(f.value) != ''
)

SELECT
    video_id,
    title_clean,
    published_date,
    year,
    month,
    food_name
FROM unnested
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY video_id, food_name
    ORDER BY published_date DESC
) = 1