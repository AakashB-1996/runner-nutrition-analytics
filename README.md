# 🏃 Runner Nutrition Analytics

A production-grade data pipeline analyzing YouTube runner nutrition content, enriched with USDA nutritional data, built on a dimensional model (star schema).

![Pipeline](https://img.shields.io/badge/Pipeline-Airflow-green) ![Warehouse](https://img.shields.io/badge/Warehouse-Snowflake-blue) ![Transform](https://img.shields.io/badge/Transform-dbt-orange) ![Status](https://img.shields.io/badge/Status-Production-brightgreen)

---

## 📊 Project Overview

Runners consume nutrition advice from YouTube influencers, but this content is rarely grounded in scientific data. This pipeline:

- Ingests **560+ YouTube videos** (2020-2026) discussing runner nutrition
- Extracts **35+ food mentions** from video titles and descriptions
- Enriches with **USDA FoodData Central** nutritional data
- Loads into a **Snowflake star schema** data warehouse
- Runs **automatically every day at 2:00 AM UTC** via Apache Airflow

### Key Insights
- Runners mention **"protein"** 123x but it's just a category - not a real food
- **Protein bars** are marketed as protein food but contain more carbs (38.4g) than protein (30.3g)
- **Turkey** has 56% protein calories but is mentioned only once
- **Energy gels** are the fastest-growing nutrition topic (2020-2026)

---

## 🏗️ Architecture

```
YouTube API          USDA API
     │                   │
     ▼                   ▼
┌─────────────────────────────┐
│     Bronze Layer            │
│  raw_youtube_videos         │
│  raw_usda_foods             │
│  stg_processed_videos       │
└─────────────────────────────┘
             │
             ▼
┌─────────────────────────────┐
│     Silver Layer (dbt)      │
│  stg_youtube_videos         │
│  int_food_mentions          │
└─────────────────────────────┘
             │
             ▼
┌─────────────────────────────┐
│  Gold Layer - Star Schema   │
│                             │
│  dim_date (2,557 rows)      │
│  dim_foods (31 foods)       │
│  dim_channels (409 channels)│
│         │                   │
│  fact_video_food_mentions   │
│         (645 rows)          │
└─────────────────────────────┘
             │
             ▼
┌─────────────────────────────┐
│     Mart Layer (dbt)        │
│  mart_food_analysis         │
└─────────────────────────────┘
```

### Airflow DAG (Daily @ 2AM UTC)
```
extract_youtube → load_bronze → dbt_dimensions → dbt_fact → dbt_mart → dbt_test → generate_insights → pipeline_summary
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Airflow | Orchestration & scheduling |
| Snowflake | Cloud data warehouse |
| dbt Core | SQL transformations & testing |
| YouTube Data API v3 | Video metadata extraction |
| USDA FoodData Central | Nutritional data enrichment |
| Python 3.10+ | Extraction scripts |
| Docker | Airflow containerization |

---

## 📁 Project Structure

```
runner_nutrition/
├── airflow/
│   ├── dags/
│   │   └── runner_nutrition_dag.py   # 8-task Airflow DAG
│   └── docker-compose.yml            # Airflow + Postgres setup
├── models/
│   ├── staging/
│   │   ├── stg_youtube_videos.sql
│   │   └── sources.yml
│   ├── intermediate/
│   │   └── int_food_mentions.sql
│   ├── dimensions/
│   │   ├── dim_foods.sql
│   │   ├── dim_foods.yml             # Unit tests
│   │   ├── dim_channels.sql
│   │   └── dim_channels.yml          # Unit tests
│   ├── facts/
│   │   ├── fact_video_food_mentions.sql
│   │   └── fact_video_food_mentions.yml  # Unit tests
│   └── marts/
│       ├── mart_food_analysis.sql
│       └── mart_food_analysis.yml
├── tests/
│   └── analytical_queries.sql        # 6 analytical queries
├── dbt_project.yml
├── packages.yml
└── README.md
```

---

## 🚀 Setup Instructions

### Prerequisites
- Snowflake account
- YouTube Data API key
- USDA API key
- Docker Desktop
- Python 3.10+
- dbt Core (`pip install dbt-snowflake`)

### 1. Clone the Repository
```bash
git clone git@github.com:AakashB-1996/runner-nutrition-analytics.git
cd runner-nutrition-analytics
```

### 2. Configure Snowflake
```bash
# Set up profiles.yml
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << EOF
runner_nutrition:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_user
      private_key_path: "{{ env_var('PRIVATE_KEY_PATH') }}"
      role: ALL_USERS_ROLE
      database: DATAEXPERT_STUDENT
      warehouse: COMPUTE_WH
      schema: your_schema
      threads: 4
EOF

export PRIVATE_KEY_PATH=/path/to/snowflake_key.pem
```

### 3. Initialize Snowflake Tables
```sql
-- Run in Snowflake worksheet
-- Creates dim_date, dim_foods, dim_channels, fact_video_food_mentions, stg_processed_videos
-- dim_date is pre-populated with 2,557 days (2020-2026)
```
> See `dimensional_model_ddl_fixed.sql` in the repo root

### 4. Run dbt
```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run tests (66 data tests + 8 unit tests)
dbt test
```

### 5. Set Up Airflow
```bash
cd airflow

# Create .env file
cat > .env << EOF
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
PRIVATE_KEY_PATH=/opt/airflow/secrets/snowflake_key.pem
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
YOUTUBE_API_KEY=your_key
USDA_API_KEY=your_key
EOF

# Start Airflow (port 8081)
docker-compose up -d

# Open UI
open http://localhost:8081
# Login: admin / admin
```

### 6. Trigger the Pipeline
- Open **http://localhost:8081**
- Find `runner_nutrition_pipeline`
- Click **▶️ Trigger DAG**
- Watch all 8 tasks turn green!

---

## 📐 Dimensional Model

### Star Schema Design

```
         dim_date
         (2,557 rows)
              │
              │ date_key
              │
dim_foods ────┼──── fact_video_food_mentions ────┬──── dim_channels
(31 rows)     │     (645 rows)                   │     (409 rows)
         food_key     Grain: video-food mention   channel_key
```

### Tables

| Table | Type | Rows | Description |
|-------|------|------|-------------|
| dim_date | Dimension | 2,557 | Pre-populated 2020-2026 |
| dim_foods | Dimension (Type 1 SCD) | 31 | Foods + USDA nutrition data |
| dim_channels | Dimension (Type 1 SCD) | 409 | YouTube channels |
| fact_video_food_mentions | Fact | 645 | One row per video-food mention |
| mart_food_analysis | Mart | 31 | Aggregated food analytics |

---

## 🔬 Analytical Questions

| # | Question | Key Finding |
|---|----------|-------------|
| Q1 | What macronutrients do runners discuss most? | Carbohydrates (159) > Protein (123) > Fat (18) |
| Q2 | What actual foods are most mentioned? | Protein bar (61) > Hydration (48) > Energy gel (43) |
| Q3 | What high-value foods are runners overlooking? | Turkey (56% protein calories) - only 1 mention |
| Q4 | Which channels produce the most nutrition content? | Top 15 channels with 3+ nutrition videos |
| Q5 | How has nutrition discussion evolved 2020-2026? | Energy gel mentions growing YoY |
| Q6 | The protein myth - mentions vs actual content? | Protein bar has more carbs than protein! |

---

## ✅ Data Quality

- **66 schema tests** (unique, not_null, relationships, accepted_values)
- **8 unit tests** (initial run + incremental run per dimension/fact)
- **Idempotency verified** - re-running inserts 0 duplicates
- **Freshness tests** - data loaded within 48 hours

---

## 🔄 Idempotency

The pipeline is fully idempotent:
1. `stg_processed_videos` tracks all loaded video_ids
2. MERGE statements prevent duplicate inserts
3. dbt incremental models filter existing records
4. Running the DAG twice produces identical results

---

## 📈 Future Enhancements

- Type 2 SCDs for full historical tracking
- Video transcription for deeper food extraction
- Sentiment analysis on nutrition discussions
- Expand to Reddit, Instagram, TikTok
- Machine learning for automatic food classification
- Tableau/PowerBI dashboards
