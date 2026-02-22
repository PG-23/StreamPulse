# 📊 StreamPulse

**Real-time Twitch chat analytics and sentiment pipeline**

StreamPulse is an end-to-end data pipeline that ingests live Twitch chat messages, analyzes sentiment, and surfaces insights through an interactive dashboard. Built to demonstrate core data engineering skills including data ingestion, transformation, orchestration, and visualization.

---

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Twitch IRC     │────▶│  JSON Files  │────▶│   PostgreSQL    │
│  (Live Chat)     │     │  (data/raw/) │     │  (raw_messages) │
└─────────────────┘     └──────────────┘     └────────┬────────┘
                                                       │
                                              ┌────────▼────────┐
                                              │    Sentiment     │
                                              │    Analysis      │
                                              │   (TextBlob)     │
                                              └────────┬────────┘
                                                       │
                                              ┌────────▼────────┐
                                              │  dbt Transforms  │
                                              │  staging → int   │
                                              │  → marts         │
                                              └────────┬────────┘
                                                       │
                                              ┌────────▼────────┐
                                              │   Streamlit      │
                                              │   Dashboard      │
                                              └─────────────────┘

                    ┌─────────────────────────────────────┐
                    │         Apache Airflow               │
                    │  Orchestrates: Load → Sentiment →    │
                    │  dbt Run → dbt Test (every 5 min)    │
                    └─────────────────────────────────────┘
```

## Tech Stack

| Layer            | Technology                        |
|------------------|-----------------------------------|
| Ingestion        | Python, Twitch IRC (raw sockets)  |
| Storage          | PostgreSQL 16 (Docker)            |
| Transformation   | dbt-core with dbt-postgres        |
| Sentiment        | TextBlob (NLP)                    |
| Orchestration    | Apache Airflow 2.9                |
| Visualization    | Streamlit + Plotly                |
| Containerization | Docker & Docker Compose           |

## Features

- **Live chat ingestion** — Connects to Twitch IRC and captures messages with full metadata (emotes, badges, subscriber status, timestamps)
- **Batched file storage** — Messages are written as JSON batches to disk, then loaded into PostgreSQL for durability and replayability
- **Sentiment analysis** — Each message is scored for polarity (-1 to +1) and subjectivity using TextBlob NLP
- **dbt transformation pipeline** — Three-layer model architecture (staging → intermediate → marts) with schema tests and documentation
- **Automated orchestration** — Airflow DAG runs the full pipeline every 5 minutes: load → sentiment → dbt run → dbt test
- **Interactive dashboard** — Streamlit app with time-series charts, sentiment distribution, behavior signals, and filterable channel comparison

## Data Models

### Staging
- `stg_messages` — Cleaned and standardized raw messages

### Intermediate
- `int_messages_enriched` — Messages enriched with time buckets, message characteristics (all caps detection, emote detection, word count), and sentiment flags

### Marts
- `chat_activity` — Per-minute metrics by channel: message volume, unique chatters, sentiment averages, behavior ratios
- `channel_summary` — Aggregated channel-level statistics: total messages, top chatters, peak activity, sentiment breakdown

## Sample Insights

Data collected from three distinct Twitch communities over 10-minute sessions:

| Channel     | Content Type       | Messages | Chatters | Avg Sentiment | Positive | Negative |
|-------------|--------------------|----------|----------|---------------|----------|----------|
| hasanabi    | Political commentary | 5,957  | 2,175    | 0.013 😐      | 1,056    | 841      |
| loltyler1   | Competitive gaming  | 440    | 196      | 0.026 😐      | 50       | 35       |
| n3on        | IRL / Social        | 1,069  | 270      | 0.064 😊      | 142      | 54       |

**Key findings:**
- Political commentary streams show near-neutral sentiment with the most even positive/negative split, reflecting active debate
- IRL/social streams with celebrity interactions generate the highest positive sentiment (3:1 positive-to-negative ratio)
- Competitive gaming chat volume varies significantly based on in-game moments

## Getting Started

### Prerequisites
- Python 3.11+
- Docker Desktop
- A free Twitch account

### 1. Clone and set up the environment
```bash
git clone https://github.com/YOUR_USERNAME/StreamPulse.git
cd StreamPulse
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure credentials
Copy `.env.example` to `.env` and fill in your Twitch credentials:
```
TWITCH_TOKEN=oauth:your_token_here
TWITCH_NICKNAME=your_twitch_username
TWITCH_CHANNELS=#channel1,#channel2
```

Generate a Twitch OAuth token by registering an app at [dev.twitch.tv/console](https://dev.twitch.tv/console).

### 3. Start the infrastructure
```bash
docker compose up -d
```
This starts PostgreSQL and Apache Airflow.

### 4. Collect chat data
```bash
cd ingestion
python twitch_chat_listener.py
```
Let it run for 5-10 minutes on an active channel, then stop with `Ctrl+C`.

### 5. Run the pipeline manually (first time)
```bash
cd ingestion
python load_to_postgres.py
python sentiment_analysis.py
cd ../dbt_transforms
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 6. Launch the dashboard
```bash
cd dashboards
streamlit run app.py
```
Open `http://localhost:8501` to view the dashboard.

### 7. Enable automated pipeline
Open Airflow at `http://localhost:8080` (login: admin/admin) and toggle the `streampulse_pipeline` DAG on. It will automatically process new data every 5 minutes.

## Project Structure

```
StreamPulse/
├── ingestion/              # Data ingestion scripts
│   ├── config.py           # Environment configuration
│   ├── twitch_chat_listener.py  # IRC chat collector
│   ├── load_to_postgres.py      # JSON → PostgreSQL loader
│   └── sentiment_analysis.py    # TextBlob sentiment scorer
├── database/
│   └── schema.sql          # PostgreSQL table definitions
├── dbt_transforms/         # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/        # Data cleaning
│       ├── intermediate/   # Enrichment
│       └── marts/          # Analytics-ready tables
├── dags/
│   └── streampulse_pipeline.py  # Airflow DAG
├── dashboards/
│   └── app.py              # Streamlit dashboard
├── airflow/
│   └── init.sh             # Airflow initialization script
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Future Improvements

- Add Kafka for real-time streaming ingestion instead of file-based batching
- Implement a more sophisticated NLP model (e.g., fine-tuned transformer) for better Twitch chat sentiment accuracy
- Add emote-specific sentiment mapping (e.g., LUL = positive, BabyRage = negative)
- Deploy to cloud infrastructure (AWS/GCP) with Terraform
- Add data quality checks with Great Expectations
- Build a real-time dashboard with WebSocket updates