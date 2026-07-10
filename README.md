# Real-Time AQI Lakehouse Pipeline with Kafka and Spark

## Project Overview

This project builds a local real-time-style data engineering pipeline for air-quality analytics.

It ingests air-quality data from the Open-Meteo API, publishes records to a Kafka topic, reads the Kafka stream with Spark Structured Streaming, and writes the data through Bronze, Silver, and Gold analytical layers.

The goal of this project is to demonstrate practical data engineering skills using Python API ingestion, Apache Kafka, Spark Structured Streaming, PySpark transformations, Parquet-based lakehouse layers, data validation, deduplication, and Gold analytics tables for BI/reporting.

## Local Architecture

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
| Streaming broker | Apache Kafka, Azure Event Hubs |
| Stream processing | Spark Structured Streaming / PySpark, Azure Databricks Serverless |
| Storage format | Parquet | Delta Lake |
| Storage | Local filesystem, ADLS Gen2 |
| Cloud | Azure Event Hubs, Azure Databricks, ADLS Gen2 |
| Local runtime | WSL, Docker, Docker Compose |
| Governance/Security | Unity Catalog External Location, Databricks Secrets |
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
- Azure migration completed through Event Hubs, Databricks Serverless, and ADLS Gen2.
- Bronze, Silver, and Gold layers are available both locally and in Azure.
- Gold Delta outputs were registered as external Unity Catalog tables for Spark SQL / Databricks SQL access.

Next planned stages:

- Power BI dashboard using Gold outputs
- Optional orchestration and monitoring improvements

## Azure Migration

The project was migrated from a local Kafka/Spark/Parquet pipeline to an Azure lakehouse-style architecture using Event Hubs, Databricks Serverless, Unity Catalog external locations, and ADLS Gen2.

### Azure Architecture

```text
Open-Meteo Air Quality API
        ↓
Python Producer
        ↓
Azure Event Hubs
        ↓
Azure Databricks Serverless
        ↓
ADLS Gen2 Bronze Layer
        ↓
ADLS Gen2 Silver Layer
        ↓
ADLS Gen2 Gold Layer
```

### Azure Components
| Component                       | Purpose                                  |
| ------------------------------- | ---------------------------------------- |
| Azure Event Hubs                | Kafka-compatible streaming ingestion     |
| Azure Databricks Serverless     | Spark processing and notebook execution  |
| ADLS Gen2                       | Cloud lakehouse storage                  |
| Unity Catalog External Location | Governed access to ADLS paths            |
| Databricks Secrets              | Secure storage of Event Hubs credentials |

### Completed Azure Migration Steps
- Published Open-Meteo AQI records to Azure Event Hubs using the Kafka-compatible endpoint.
- Configured Databricks secrets for Event Hubs credentials.
- Created an ADLS Gen2 lakehouse container with Bronze, Silver, Gold, and checkpoint folders.
- Configured Databricks access to ADLS using an Access Connector and Unity Catalog external location.
- Read Event Hubs messages from Databricks.
- Wrote raw JSON messages to the Bronze layer.
- Parsed Bronze JSON into structured records.
- Created Silver cleaned, validated, and deduplicated AQI records.
- Created Gold analytics tables for reporting.

### Azure Data Lake Layout
```text
lakehouse/
├── bronze/
│   ├── air_quality_raw/
│   └── air_quality_structured/
├── silver/
│   └── air_quality/
├── gold/
│   ├── daily_city_aqi/
│   ├── daily_city_ranking/
│   ├── data_freshness/
│   └── data_completeness/
└── checkpoints/
```

### Delta Lake Upgrade

The Azure version was upgraded from Parquet outputs to Delta Lake tables for the Bronze, Silver, and Gold layers.

Delta outputs:

```text
lakehouse/
├── bronze_delta/
│   ├── air_quality_raw/
│   └── air_quality_structured/
├── silver_delta/
│   └── air_quality/
├── gold_delta/
│   ├── daily_city_aqi/
│   ├── daily_city_ranking/
│   ├── data_freshness/
│   └── data_completeness/
└── checkpoints_delta/
```
### Databricks Gold Table Registration

The Gold Delta outputs were registered as external Delta tables in Unity Catalog, making the analytics layer queryable through Databricks SQL and Spark SQL.

Registered catalog and schema:

```text
Catalog: dbw_aqi_lakehouse_dev
Schema: aqi_lakehouse
```

Registered Gold tables:

| Table                | Description                                         |
| -------------------- | --------------------------------------------------- |
| `daily_city_aqi`     | Daily AQI and pollutant metrics by city             |
| `daily_city_ranking` | Daily city ranking based on average European AQI    |
| `data_freshness`     | Latest ingestion and measurement timestamps by city |
| `data_completeness`  | Actual vs expected daily record counts by city      |

This step registers table metadata over existing ADLS Gen2 Delta paths. The data remains stored in ADLS Gen2, while Databricks provides a table interface for querying and downstream BI/reporting.

Example query:

```sql
SELECT *
FROM dbw_aqi_lakehouse_dev.aqi_lakehouse.daily_city_aqi
ORDER BY measurement_date, city;
```

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