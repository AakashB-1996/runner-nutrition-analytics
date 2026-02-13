-- models/facts/fact_video_food_mentions.sql
-- Fact table: One row per video-food mention
-- Idempotent: Uses MERGE to prevent duplicates

{{
    config(
        materialized='incremental',
        unique_key='mention_key',
        on_schema_change='append_new_columns',
        cluster_by=['published_at']
    )
}}

WITH video_foods AS (
    SELECT * FROM {{ ref('int_food_mentions') }}
    
    {% if is_incremental() %}
    -- Only process new videos (idempotency)
    WHERE video_id NOT IN (
        SELECT DISTINCT video_id FROM {{ this }}
    )
    {% endif %}
),

dim_foods AS (
    SELECT * FROM {{ ref('dim_foods') }}
),

dim_channels AS (
    SELECT * FROM {{ ref('dim_channels') }}
),

dim_date AS (
    SELECT * FROM {{ source('raw', 'dim_date') }}
),

enriched AS (
    SELECT
        vf.video_id || '_' || df.food_key AS mention_key,
        vf.video_id,
        dd.date_key,
        df.food_key,
        dc.channel_key,
        vf.title_clean AS video_title,
        sv.published_at,                    -- ← get timestamp from stg, not int
        1 AS mention_count,
        CURRENT_TIMESTAMP() AS created_at
    FROM video_foods vf
    INNER JOIN dim_foods df
        ON LOWER(TRIM(vf.food_name)) = LOWER(TRIM(df.food_name_normalized)) and df.food_key IS NOT NULL
    INNER JOIN {{ ref('stg_youtube_videos') }} sv
        ON vf.video_id = sv.video_id
    INNER JOIN dim_channels dc
        ON LOWER(TRIM(sv.channel_title_clean)) = dc.channel_title_normalized
    INNER JOIN dim_date dd
        ON vf.published_date = dd.full_date
   
)

SELECT * FROM enriched