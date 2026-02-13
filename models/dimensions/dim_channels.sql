-- models/dimensions/dim_channels.sql
-- YouTube channels dimension

{{
    config(
        materialized='incremental',
        unique_key='channel_title_normalized',
        on_schema_change='append_new_columns'
    )
}}

WITH source_channels AS (
    SELECT 
        channel_title_clean AS channel_title,
        LOWER(TRIM(channel_title_clean)) AS channel_title_normalized,
        MIN(published_date) AS first_video_date,
        COUNT(DISTINCT video_id) AS total_videos
    FROM {{ ref('stg_youtube_videos') }}
    
    {% if is_incremental() %}
    -- Only process new channels
    WHERE LOWER(TRIM(channel_title_clean)) NOT IN (
        SELECT channel_title_normalized FROM {{ this }}
    )
    {% endif %}
    
    GROUP BY channel_title, channel_title_normalized
),

final AS (
    SELECT
        channel_title,
        channel_title_normalized,
        first_video_date,
        total_videos,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    FROM source_channels
)

SELECT * FROM final