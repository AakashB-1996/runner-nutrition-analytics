"""
Runner Nutrition Analytics Pipeline
====================================
Idempotent daily pipeline extracting YouTube runner nutrition videos,
loading to Snowflake, transforming with dbt, and generating insights.

Schedule: Daily at 2:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import logging

# =====================================================================
# DEFAULT ARGS
# =====================================================================

default_args = {
    'owner': 'aakash',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

# =====================================================================
# DAG DEFINITION
# =====================================================================

dag = DAG(
    'runner_nutrition_pipeline',
    default_args=default_args,
    description='Daily runner nutrition analytics pipeline',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['nutrition', 'youtube', 'snowflake', 'dbt'],
)

# =====================================================================
# CONFIG
# =====================================================================

YOUTUBE_API_KEY  = os.environ.get('YOUTUBE_API_KEY')
USDA_API_KEY     = os.environ.get('USDA_API_KEY')
DBT_PROJECT_DIR  = os.environ.get('DBT_PROJECT_DIR',  '/opt/airflow/dbt/runner_nutrition')
DBT_PROFILES_DIR = os.environ.get('DBT_PROFILES_DIR', '/opt/airflow/dbt')

FOOD_KEYWORDS = [
    'carb', 'carbs', 'carbohydrate', 'protein', 'fat', 'banana',
    'oatmeal', 'pasta', 'rice', 'egg', 'chicken', 'gel', 'energy gel',
    'bar', 'protein bar', 'coffee', 'sports drink', 'hydration',
    'yogurt', 'milk', 'sweet potato', 'quinoa', 'salmon', 'avocado',
    'nuts', 'berries', 'bread', 'bagel', 'smoothie', 'fruit'
]

# =====================================================================
# SNOWFLAKE CONNECTION HELPER (PEM KEY AUTH)
# =====================================================================

def get_snowflake_connection():
    """
    Create Snowflake connection using PEM private key (no password).
    Key is mounted into Docker container at /opt/airflow/secrets/snowflake_key.pem
    """
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    key_path = os.environ.get('PRIVATE_KEY_PATH', '/opt/airflow/secrets/snowflake_key.pem')

    with open(key_path, 'rb') as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None  # Key is not encrypted
        )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        user=os.environ.get('SNOWFLAKE_USER'),
        private_key=private_key_bytes,
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'DATAEXPERT_STUDENT'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA', 'BOLISETTYAAKASH693240'),
    )


# =====================================================================
# TASK 1: EXTRACT YOUTUBE VIDEOS
# =====================================================================

def extract_youtube(**context):
    """
    Extract YouTube videos published since last run.
    Uses MAX(published_at) from Snowflake for incremental extraction.
    """
    from googleapiclient.discovery import build

    logging.info("Starting YouTube extraction...")

    # Get last processed date for incremental loading
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COALESCE(
            MAX(published_at),
            DATEADD(day, -7, CURRENT_TIMESTAMP())
        )
        FROM raw_youtube_videos
    """)
    last_date = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    published_after = last_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    logging.info(f"Fetching videos published after: {published_after}")

    # YouTube API
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

    SEARCH_QUERIES = [
        'runner nutrition food',
        'marathon nutrition',
        'running diet',
        'ultramarathon fueling',
        'what runners eat'
    ]

    all_videos = []

    for query in SEARCH_QUERIES:
        try:
            response = youtube.search().list(
                part='snippet',
                q=query,
                type='video',
                publishedAfter=published_after,
                maxResults=50,
                relevanceLanguage='en'
            ).execute()

            for item in response.get('items', []):
                title       = item['snippet']['title'].lower()
                description = item['snippet'].get('description', '').lower()
                text        = title + ' ' + description

                mentioned_foods = [f for f in FOOD_KEYWORDS if f in text]

                if mentioned_foods:
                    all_videos.append({
                        'video_id':        item['id']['videoId'],
                        'title':           item['snippet']['title'],
                        'description':     item['snippet'].get('description', ''),
                        'channel_title':   item['snippet']['channelTitle'],
                        'published_at':    item['snippet']['publishedAt'][:19].replace('T', ' '),
                        'url':             f"https://youtube.com/watch?v={item['id']['videoId']}",
                        'mentioned_foods': ','.join(mentioned_foods),
                        'year':            int(item['snippet']['publishedAt'][:4])
                    })

        except Exception as e:
            logging.error(f"Error fetching query '{query}': {e}")
            continue

    # Deduplicate by video_id
    seen = set()
    unique_videos = []
    for v in all_videos:
        if v['video_id'] not in seen:
            seen.add(v['video_id'])
            unique_videos.append(v)

    logging.info(f"Found {len(unique_videos)} new unique videos")

    context['ti'].xcom_push(key='videos',      value=unique_videos)
    context['ti'].xcom_push(key='video_count', value=len(unique_videos))

    return len(unique_videos)


# =====================================================================
# TASK 2: LOAD BRONZE (IDEMPOTENT MERGE)
# =====================================================================

def load_bronze(**context):
    """
    Idempotent load to raw_youtube_videos using MERGE.
    Skips videos already in stg_processed_videos.
    """
    videos = context['ti'].xcom_pull(key='videos', task_ids='extract_youtube')

    if not videos:
        logging.info("No new videos to load. Skipping.")
        return 0

    logging.info(f"Loading {len(videos)} videos to Snowflake...")

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    # Idempotency check
    video_ids    = [v['video_id'] for v in videos]
    placeholders = ','.join(['%s'] * len(video_ids))
    cursor.execute(
        f"SELECT video_id FROM stg_processed_videos WHERE video_id IN ({placeholders})",
        video_ids
    )
    existing_ids = {row[0] for row in cursor.fetchall()}
    new_videos   = [v for v in videos if v['video_id'] not in existing_ids]

    if not new_videos:
        logging.info("All videos already processed. Idempotency check passed.")
        conn.close()
        return 0

    logging.info(f"Inserting {len(new_videos)} new videos...")

    for video in new_videos:
        # MERGE into raw_youtube_videos
        cursor.execute("""
            MERGE INTO raw_youtube_videos AS target
            USING (SELECT %s AS video_id) AS source
            ON target.video_id = source.video_id
            WHEN NOT MATCHED THEN
                INSERT (video_id, title, description, channel_title,
                        published_at, url, mentioned_foods, year)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            video['video_id'],
            video['video_id'],
            video['title'][:500],
            video['description'][:5000],
            video['channel_title'][:200],
            video['published_at'],
            video['url'],
            video['mentioned_foods'],
            video['year']
        ))

        # Track as processed
        cursor.execute("""
            INSERT INTO stg_processed_videos (video_id, processing_status)
            VALUES (%s, 'SUCCESS')
        """, (video['video_id'],))

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"Successfully loaded {len(new_videos)} videos")
    return len(new_videos)


# =====================================================================
# TASKS 3-8: DBT TRANSFORMATIONS
# =====================================================================


dbt_stg = BashOperator(
    task_id='dbt_stg',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --profiles-dir {DBT_PROFILES_DIR} \
            --select stg_youtube_videos \
            --no-version-check
    """,
    dag=dag,
)

dbt_int = BashOperator(
    task_id='dbt_int',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --profiles-dir {DBT_PROFILES_DIR} \
            --select int_food_mentions \
            --no-version-check
    """,
    dag=dag,
)

dbt_dimensions = BashOperator(
    task_id='dbt_dimensions',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --profiles-dir {DBT_PROFILES_DIR} \
            --select dim_foods dim_channels \
            --no-version-check
    """,
    dag=dag,
)

dbt_fact = BashOperator(
    task_id='dbt_fact',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --profiles-dir {DBT_PROFILES_DIR} \
            --select fact_video_food_mentions \
            --no-version-check
    """,
    dag=dag,
)

dbt_mart = BashOperator(
    task_id='dbt_mart',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --profiles-dir {DBT_PROFILES_DIR} \
            --select mart_food_analysis \
            --no-version-check
    """,
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test \
            --profiles-dir {DBT_PROFILES_DIR} \
            --no-version-check
    """,
    dag=dag,
)


# =====================================================================
# TASK 7: GENERATE INSIGHTS
# =====================================================================

def generate_insights(**context):
    """
    Run analytical queries and log key insights.
    """
    logging.info("Generating insights...")

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    # Top 5 foods
    cursor.execute("""
        SELECT food_name, video_mention_count, usda_protein_g, usda_calories
        FROM DATAEXPERT_STUDENT.BOLISETTYAAKASH693240.mart_food_analysis
        ORDER BY mention_rank
        LIMIT 5
    """)
    top_foods = cursor.fetchall()

    logging.info("=== TOP 5 RUNNER NUTRITION FOODS ===")
    for rank, row in enumerate(top_foods, 1):
        logging.info(
            f"#{rank} {row[0]}: {row[1]} videos | "
            f"Protein: {row[2]}g | Calories: {row[3]}"
        )

    # Pipeline stats
    cursor.execute("SELECT COUNT(*) FROM raw_youtube_videos")
    total_videos = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM fact_video_food_mentions")
    total_mentions = cursor.fetchone()[0]

    logging.info("=== PIPELINE STATS ===")
    logging.info(f"Total videos:   {total_videos}")
    logging.info(f"Total mentions: {total_mentions}")

    cursor.close()
    conn.close()

    context['ti'].xcom_push(key='total_videos', value=total_videos)
    context['ti'].xcom_push(key='top_foods',    value=[r[0] for r in top_foods])

    return total_videos


# =====================================================================
# TASK 8: PIPELINE SUMMARY
# =====================================================================

def pipeline_summary(**context):
    """
    Log final pipeline run summary.
    """
    video_count  = context['ti'].xcom_pull(key='video_count',  task_ids='extract_youtube') or 0
    total_videos = context['ti'].xcom_pull(key='total_videos', task_ids='generate_insights') or 0
    top_foods    = context['ti'].xcom_pull(key='top_foods',    task_ids='generate_insights') or []

    logging.info("=" * 50)
    logging.info("RUNNER NUTRITION PIPELINE COMPLETE")
    logging.info("=" * 50)
    logging.info(f"New videos today:   {video_count}")
    logging.info(f"Total videos in DB: {total_videos}")
    logging.info(f"Top foods:          {', '.join(top_foods)}")
    logging.info("=" * 50)


# =====================================================================
# TASK INSTANTIATION
# =====================================================================

t1_extract = PythonOperator(
    task_id='extract_youtube',
    python_callable=extract_youtube,
    dag=dag,
)

t2_load = PythonOperator(
    task_id='load_bronze',
    python_callable=load_bronze,
    dag=dag,
)

t7_insights = PythonOperator(
    task_id='generate_insights',
    python_callable=generate_insights,
    dag=dag,
)

t8_summary = PythonOperator(
    task_id='pipeline_summary',
    python_callable=pipeline_summary,
    dag=dag,
)

# =====================================================================
# TASK DEPENDENCIES
# =====================================================================
#
# extract_youtube
#       ↓
# load_bronze
#       ↓
# dbt_stg
#       ↓
# dbt_int
#       ↓
# dbt_dimensions
#       ↓
# dbt_fact
#       ↓
# dbt_mart
#       ↓
# dbt_test
#       ↓
# generate_insights
#       ↓
# pipeline_summary

t1_extract >> t2_load >> dbt_stg >> dbt_int >> dbt_dimensions >> dbt_fact >> dbt_mart >> dbt_test >> t7_insights >> t8_summary