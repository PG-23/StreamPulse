#!/bin/bash
set -e

echo "=== Installing dependencies ==="
pip install psycopg2-binary textblob dbt-postgres python-dotenv --quiet

echo "=== Downloading TextBlob corpora ==="
python -m textblob.download_corpora --quiet || true

echo "=== Initializing Airflow database ==="
airflow db init

echo "=== Creating admin user ==="
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@streampulse.local || true

echo "=== Starting Airflow webserver ==="
airflow webserver --port 8080 &

echo "=== Starting Airflow scheduler ==="
exec airflow scheduler