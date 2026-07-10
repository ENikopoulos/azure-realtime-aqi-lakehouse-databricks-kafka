# Architecture

## Current Local Architecture

```text
+---------------------------+
| Open-Meteo AQI API        |
+-------------+-------------+
              |
              v
+---------------------------+
| Python Kafka Producer     |
| producer/                 |
+-------------+-------------+
              |
              v
+---------------------------+
| Local Kafka               |
| topic: air_quality_raw    |
+-------------+-------------+
              |
              v
+---------------------------+
| Spark Structured Streaming|
| kafka_to_bronze.py        |
+-------------+-------------+
              |
              v
+---------------------------+
| Bronze Parquet            |
| data/bronze/              |
+-------------+-------------+
              |
              v
+---------------------------+
| Silver Cleaning Job       |
| bronze_to_silver.py       |
+-------------+-------------+
              |
              v
+---------------------------+
| Silver Parquet            |
| data/silver/              |
+-------------+-------------+
              |
              v
+---------------------------+
| Gold Analytics Job        |
| silver_to_gold.py         |
+-------------+-------------+
              |
              v
+---------------------------+
| Gold Reporting Tables     |
| data/gold/                |
+---------------------------+
```

## Future Azure Architecture

### Azure Architecture
```text
Open-Meteo Air Quality API
        ↓
Python Producer
        ↓
Azure Event Hubs
        ↓
Azure Data Factory Schedule Trigger
        ↓
ADF Databricks Job Activity
        ↓
Azure Databricks Workflow
        ↓
Event Hubs → Bronze Delta
        ↓
Bronze Delta → Silver Delta
        ↓
Silver Delta → Gold Delta
        ↓
Register Gold tables in Unity Catalog
        ↓
ADLS Gen2 + Unity Catalog Gold Tables
```
