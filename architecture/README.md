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

```text
Open-Meteo AQI API
        ↓
Python Producer
        ↓
Azure Event Hubs Kafka endpoint
        ↓
Azure Databricks Structured Streaming
        ↓
Bronze Delta tables on ADLS Gen2
        ↓
Silver Delta tables on ADLS Gen2
        ↓
Gold Delta tables on ADLS Gen2
        ↓
Power BI
```
