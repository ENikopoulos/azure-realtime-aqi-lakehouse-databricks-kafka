# Local Pipeline Runbook

This runbook contains the command sequence for running the local AQI pipeline.

## Prerequisites

- WSL/Linux shell
- Docker running
- Python virtual environment activated
- Project dependencies installed
- Kafka Docker Compose file available under `kafka/docker-compose.yml`

## 1. Start Kafka

```bash
docker compose -f kafka/docker-compose.yml up -d
```

Check that Kafka is running:

```bash
docker ps
```

Expected container:

```text
local-kafka
```

## 2. Create Kafka Topic

```bash
docker exec -it local-kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic air_quality_raw \
  --bootstrap-server localhost:9092
```

List topics:

```bash
docker exec -it local-kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Expected topic:

```text
air_quality_raw
```

## 3. Produce Open-Meteo AQI Records to Kafka

```bash
python -m producer.produce_openmeteo_to_kafka
```

## 4. Run Kafka to Bronze Streaming Job

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
  spark/kafka_to_bronze.py
```

This job reads Kafka messages and writes parsed AQI records to:

```text
data/bronze/air_quality_raw/
```

Checkpoint location:

```text
data/checkpoints/bronze_air_quality_raw/
```

Stop the streaming job manually with `CTRL + C`.

## 5. Run Bronze to Silver Job

```bash
python spark/bronze_to_silver.py
```

This job writes cleaned and deduplicated data to:

```text
data/silver/air_quality/
```

## 6. Run Silver to Gold Job

```bash
python spark/silver_to_gold.py
```

This job writes analytics-ready tables to:

```text
data/gold/
```

Expected Gold folders:

```text
daily_city_aqi/
daily_city_ranking/
data_freshness/
data_completeness/
```

## 7. Stop Kafka

```bash
docker compose -f kafka/docker-compose.yml down
```

## Troubleshooting

### Topic does not exist

Error example:

```text
UnknownTopicOrPartitionException
```

Fix:

```bash
docker exec -it local-kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic air_quality_raw \
  --bootstrap-server localhost:9092
```

### Spark shows Kafka value as bytes

Kafka values are binary by default. Cast the `value` column to string before parsing JSON.

### Spark logs hide console output

Set Spark log level to WARN:

```python
spark.sparkContext.setLogLevel("WARN")
```

### Generated data appears in Git status

Make sure `.gitignore` includes:

```gitignore
data/bronze/
data/silver/
data/gold/
data/checkpoints/
```
