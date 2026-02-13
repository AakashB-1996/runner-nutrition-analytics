-- models/dimensions/dim_foods.sql
-- Foods dimension with USDA nutritional data

{{
    config(
        materialized='incremental',
        unique_key='food_name_normalized',
        on_schema_change='append_new_columns'
    )
}}

WITH usda_foods AS (
    SELECT * FROM {{ source('raw', 'raw_usda_foods') }}
),

extracted_foods AS (
    -- Get all unique foods from int_food_mentions
    SELECT DISTINCT
        food_name,
        LOWER(TRIM(food_name)) AS food_name_normalized,
        CASE 
            WHEN food_name IN ('carbohydrates', 'protein', 'fat') THEN TRUE
            ELSE FALSE
        END AS is_macronutrient,
        CASE 
            WHEN food_name IN ('protein bar', 'energy gel', 'energy bar', 'sports drink') THEN TRUE
            ELSE FALSE
        END AS is_sports_nutrition,
        CASE 
            WHEN food_name IN ('egg', 'banana', 'rice', 'oatmeal', 'pasta', 'chicken', 
                              'yogurt', 'milk', 'coffee', 'fruit') THEN TRUE
            ELSE FALSE
        END AS is_whole_food
    FROM {{ ref('int_food_mentions') }}
    
    {% if is_incremental() %}
    -- Only process new foods
    WHERE food_name NOT IN (SELECT food_name FROM {{ this }})
    {% endif %}
),

enriched AS (
    SELECT
        ef.food_name,
        ef.food_name_normalized,
        ef.is_macronutrient,
        ef.is_sports_nutrition,
        ef.is_whole_food,
        u.fdc_id AS usda_fdc_id,
        COALESCE(u.protein_g, 0) AS protein_g,
        COALESCE(u.carbs_g, 0) AS carbs_g,
        COALESCE(u.fat_g, 0) AS fat_g,
        COALESCE(u.calories, 0) AS calories
    FROM extracted_foods ef
    LEFT JOIN usda_foods u
        ON ef.food_name_normalized = LOWER(TRIM(u.food_name))
)

SELECT * FROM enriched