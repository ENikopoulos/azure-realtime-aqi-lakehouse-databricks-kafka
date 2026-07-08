# Azure Integration Roadmap

This project currently runs locally. Azure integration should be added after the local pipeline is documented, committed, and reproducible.

## Why Not Add Azure Immediately?

The local version proves the core engineering logic first:

```text
API → Kafka → Spark Structured Streaming → Bronze → Silver → Gold
```

Azure should be added after the local pipeline is stable so that the cloud migration is a clear platform migration, not a debugging exercise.

## Recommended Azure Phases

## Phase 1: Replace Local Kafka with Azure Event Hubs

Current local component:

```text
Docker Kafka topic: air_quality_raw
```

Azure replacement:

```text
Azure Event Hubs with Kafka-compatible endpoint
```

Goal:

- keep the Python producer logic mostly similar
- change Kafka bootstrap/security configuration
- send records to Event Hubs instead of local Kafka

## Phase 2: Move Spark Jobs to Azure Databricks

Current local component:

```text
spark/*.py scripts running locally
```

Azure replacement:

```text
Azure Databricks notebooks or jobs
```

Goal:

- run Kafka/Event Hubs to Bronze in Databricks
- use Spark Structured Streaming in Databricks
- keep Bronze/Silver/Gold logic conceptually the same

## Phase 3: Replace Local Parquet Folders with ADLS Gen2

Current local storage:

```text
data/bronze/
data/silver/
data/gold/
```

Azure replacement:

```text
Azure Data Lake Storage Gen2
```

Recommended future layout:

```text
abfss://lakehouse@<storage-account>.dfs.core.windows.net/bronze/air_quality/
abfss://lakehouse@<storage-account>.dfs.core.windows.net/silver/air_quality/
abfss://lakehouse@<storage-account>.dfs.core.windows.net/gold/daily_city_aqi/
```

## Phase 4: Use Delta Lake

Current format:

```text
Parquet
```

Azure/Databricks target format:

```text
Delta Lake
```

Goal:

- write Bronze/Silver/Gold as Delta tables
- use checkpointing for streaming reliability
- later register tables for SQL/BI access

## Phase 5: Connect Power BI

Power BI should connect to the Gold layer, not Bronze.

Recommended reporting tables:

- daily_city_aqi
- daily_city_ranking
- data_freshness
- data_completeness

## Suggested Timing

Add Azure integration after these are complete:

- local Bronze/Silver/Gold pipeline committed
- README/runbook/data dictionary committed
- Gold tables validated
- basic Power BI dashboard or mock dashboard plan created

The first Azure step should be:

```text
Azure Event Hubs as the cloud Kafka-compatible streaming source
```

Then:

```text
Azure Databricks + ADLS Gen2 + Delta Lake
```