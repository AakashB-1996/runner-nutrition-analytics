-- models/marts/mart_food_analysis.sql
-- Analytics mart: Food analysis with USDA data
-- Built on dimensional model (star schema)

{{
    config(
        materialized='table'
    )
}}

WITH fact_mentions AS (
    SELECT * FROM {{ ref('fact_video_food_mentions') }}
),

dim_foods AS (
    SELECT * FROM {{ ref('dim_foods') }}
),

dim_date AS (
    SELECT * FROM {{ source('raw', 'dim_date') }}
),

aggregated AS (
    SELECT
        f.food_key,
        df.food_name,
        df.food_name_normalized,
        df.is_macronutrient,
        df.is_sports_nutrition,
        df.is_whole_food,
        
        -- Aggregated metrics
        COUNT(DISTINCT f.video_id) AS video_mention_count,
        COUNT(*) AS total_mentions,
        MIN(d.full_date) AS first_mentioned,
        MAX(d.full_date) AS last_mentioned,
        DATEDIFF(day, MIN(d.full_date), MAX(d.full_date)) AS days_active,
        
        -- USDA nutrition data
        df.protein_g AS usda_protein_g,
        df.carbs_g AS usda_carbs_g,
        df.fat_g AS usda_fat_g,
        df.calories AS usda_calories,
        
        -- Rankings
        ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT f.video_id) DESC) AS mention_rank,
        ROW_NUMBER() OVER (PARTITION BY df.is_macronutrient ORDER BY COUNT(DISTINCT f.video_id) DESC) AS rank_within_category
        
    FROM fact_mentions f
    INNER JOIN dim_foods df ON f.food_key = df.food_key
    INNER JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY 
        f.food_key,
        df.food_name,
        df.food_name_normalized,
        df.is_macronutrient,
        df.is_sports_nutrition,
        df.is_whole_food,
        df.protein_g,
        df.carbs_g,
        df.fat_g,
        df.calories
)

SELECT * FROM aggregated
ORDER BY mention_rank