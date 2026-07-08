# Real-Time AQI Lakehouse Pipeline with Kafka and Spark

## Project Overview

This project builds a local real-time-style data engineering pipeline for air-quality analytics.

It ingests air-quality data from the Open-Meteo API, publishes records to a Kafka topic, reads the Kafka stream with Spark Structured Streaming, and writes the data through Bronze, Silver, and Gold analytical layers.

The goal of this project is to demonstrate practical data engineering skills using Python API ingestion, Apache Kafka, Spark Structured Streaming, PySpark transformations, Parquet-based lakehouse layers, data validation, deduplication, and Gold analytics tables for BI/reporting.

## Architecture

```text
Open-Meteo Air Quality API
        ↓
Python Producer
        ↓
Kafka Topic: air_quality_raw
        ↓
Spark Structured Streaming
        ↓
Bronze Layer: raw parsed AQI records
        ↓
Silver Layer: cleaned, validated, deduplicated AQI records
        ↓
Gold Layer: analytics-ready reporting tables
```

## Tech Stack

| Area | Tools |
|---|---|
| Language | Python |
| Streaming broker | Apache Kafka |
| Stream processing | Spark Structured Streaming / PySpark |
| Storage format | Parquet |
| Local runtime | WSL, Docker, Docker Compose |
| Version control | Git / GitHub |

## Pipeline Layers

### Producer

The producer fetches air-quality data from the Open-Meteo API and publishes each AQI event as a JSON message to Kafka.

Kafka topic:

```text
air_quality_raw
```

Each event contains source, city metadata, measurement timestamp, AQI and pollutant values, and ingestion timestamp.

### Bronze Layer

The Bronze layer reads JSON messages from Kafka using Spark Structured Streaming.

Purpose:

- read from Kafka
- cast Kafka binary value to string
- parse JSON into structured columns
- write raw parsed records to Parquet

Output:

```text
data/bronze/air_quality_raw/
```

Checkpoint:

```text
data/checkpoints/bronze_air_quality_raw/
```

### Silver Layer

The Silver layer cleans and validates Bronze data.

Transformations:

- cast timestamp strings to timestamp type
- filter malformed/non-Open-Meteo records
- add validation flags
- remove invalid pollutant/AQI records
- remove future forecast records
- deduplicate by city and measurement timestamp
- keep latest ingestion timestamp when duplicates exist

Output:

```text
data/silver/air_quality/
```

### Gold Layer

The Gold layer creates analytics-ready tables for reporting.

Outputs:

```text
data/gold/daily_city_aqi/
data/gold/daily_city_ranking/
data/gold/data_freshness/
data/gold/data_completeness/
```

Gold tables:

| Table | Purpose |
|---|---|
| daily_city_aqi | Daily AQI and pollutant metrics by city |
| daily_city_ranking | Daily ranking of cities by AQI |
| data_freshness | Latest ingestion timestamp per city |
| data_completeness | Actual vs expected record counts per city/date |

## How to Run Locally

### 1. Create and activate virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Kafka

```bash
docker compose -f kafka/docker-compose.yml up -d
```

### 3. Create Kafka topic

```bash
docker exec -it local-kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic air_quality_raw \
  --bootstrap-server localhost:9092
```

If the topic already exists, continue.

### 4. Run the API producer

```bash
python -m producer.produce_openmeteo_to_kafka
```

### 5. Run Kafka to Bronze streaming job

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
  spark/kafka_to_bronze.py
```

Stop the streaming job with:

```text
CTRL + C
```

### 6. Run Bronze to Silver job

```bash
python spark/bronze_to_silver.py
```

### 7. Run Silver to Gold job

```bash
python spark/silver_to_gold.py
```

### 8. Stop Kafka

```bash
docker compose -f kafka/docker-compose.yml down
```

## Data Quality Checks

The Silver layer adds validation flags for:

- valid European AQI
- valid PM10
- valid PM2.5
- valid nitrogen dioxide
- non-future measurement timestamps

Deduplication keeps one record per:

```text
city_id + measurement_timestamp
```

When duplicates exist, the record with the latest `ingestion_timestamp_utc` is kept.

## Current Status

Completed:

- API ingestion script
- Kafka producer
- Local Kafka setup with Docker Compose
- Spark Structured Streaming Kafka reader
- Bronze Parquet output
- Silver cleaned Parquet output
- Gold analytics tables

Next planned stages:

- Power BI dashboard using Gold outputs
- Azure integration with Event Hubs / Azure Databricks / ADLS Gen2
- Delta Lake version of Bronze/Silver/Gold layers
- Optional orchestration and monitoring improvements

## Notes

Generated pipeline outputs are ignored by Git:

```text
data/bronze/
data/silver/
data/gold/
data/checkpoints/
```

Only small sample data may be committed under:

```text
data/samples/
```
