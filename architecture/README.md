# Architecture

This repository contains a local implementation and a migrated Azure implementation of the AQI lakehouse pipeline.

## Local Architecture

```
+----------------------------+
| Open-Meteo AQI API         |
+-------------+--------------+
              |
              v
+----------------------------+
| Python Kafka Producer      |
| producer/                  |
+-------------+--------------+
              |
              v
+----------------------------+
| Local Apache Kafka         |
| topic: air_quality_raw     |
+-------------+--------------+
              |
              v
+----------------------------+
| Spark Structured Streaming |
| kafka_to_bronze.py         |
+-------------+--------------+
              |
              v
+----------------------------+
| Bronze Parquet             |
| data/bronze/               |
+-------------+--------------+
              |
              v
+----------------------------+
| Silver Cleaning Job        |
| bronze_to_silver.py        |
+-------------+--------------+
              |
              v
+----------------------------+
| Silver Parquet             |
| data/silver/               |
+-------------+--------------+
              |
              v
+----------------------------+
| Gold Analytics Job         |
| silver_to_gold.py          |
+-------------+--------------+
              |
              v
+----------------------------+
| Gold Reporting Tables      |
| data/gold/                 |
+----------------------------+
```

## Implemented Azure Architecture

The Azure implementation contains two separately initiated flows: data publication and processing orchestration.

### Data Publication Flow

```
+----------------------------+
| Open-Meteo AQI API         |
+-------------+--------------+
              |
              v
+----------------------------+
| Python Event Hubs Producer |
| executed separately        |
+-------------+--------------+
              |
              v
+----------------------------+
| Azure Event Hubs           |
| Kafka-compatible endpoint  |
+----------------------------+
```

The producer fetches Open-Meteo data and sends individual JSON AQI events to Azure Event Hubs.

The producer is implemented and was tested successfully, but it is not currently deployed or scheduled by Azure Data Factory.

### Processing Orchestration Flow

```
+----------------------------+
| ADF Schedule Trigger       |
| currently disabled         |
+-------------+--------------+
              |
              v
+----------------------------+
| ADF Databricks Job         |
| pl_aqi_lakehouse           |
+-------------+--------------+
              |
              v
+----------------------------+
| Databricks Workflow        |
| Serverless compute         |
+-------------+--------------+
              |
              v
+----------------------------+
| Event Hubs to Bronze Delta |
+-------------+--------------+
              |
              v
+----------------------------+
| Bronze to Silver Delta     |
| validation + deduplication |
+-------------+--------------+
              |
              v
+----------------------------+
| Silver to Gold Delta       |
| analytics aggregations     |
+-------------+--------------+
              |
              v
+----------------------------+
| Register External Tables   |
| Unity Catalog              |
+-------------+--------------+
              |
              v
+----------------------------+
| ADLS Gen2 Gold Delta Data  |
| Unity Catalog tables       |
+----------------------------+
```

## Azure Component Responsibilities

### Azure Event Hubs

Provides a Kafka-compatible endpoint for AQI event ingestion.

### Azure Databricks

Runs the Structured Streaming and batch transformation notebooks using Serverless compute.

### ADLS Gen2

Stores Bronze, Silver, Gold, and checkpoint data.

### Delta Lake

Provides the Azure storage format for the medallion layers.

### Unity Catalog

Provides governed external access to ADLS locations and registers the Gold Delta outputs as queryable tables.

### Databricks Secrets

Stores the Event Hubs connection information used by Databricks notebooks.

### Azure Data Factory

Schedules and monitors the Databricks Workflow.

ADF does not currently execute the Python producer.

## Execution Status

Successfully validated:

* Python to Event Hubs publication
* Event Hubs to Bronze ingestion
* Bronze to Silver processing
* Silver to Gold processing
* Gold external table registration
* Databricks Workflow dependencies
* ADF-triggered Databricks Workflow execution

Currently disabled or not implemented:

* ADF schedule trigger is disabled for cost control
* Event Hubs producer scheduling is not implemented
* Infrastructure as Code is not implemented
* Power BI connection to Azure Gold tables is not implemented
* Automated cloud integration tests are not implemented

## Current End-to-End Operating Procedure

To process newly published data with the current implementation:

1. Run the Python Event Hubs producer.
2. Confirm events were sent successfully.
3. Trigger the ADF pipeline or Databricks Workflow.
4. Verify the four Databricks tasks completed successfully.
5. Query the registered Unity Catalog Gold tables.

A future scheduled producer will remove the first manual step and make the cloud pipeline autonomous.
