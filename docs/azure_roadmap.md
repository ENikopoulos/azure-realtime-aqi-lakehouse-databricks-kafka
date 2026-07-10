# Azure Migration Status and Roadmap

## Overview

This document records the migration of the local AQI lakehouse pipeline to Azure and identifies the remaining work required for a more automated and reproducible cloud implementation.

The local architecture established the core pipeline:

```
Open-Meteo API
    → Python Producer
    → Apache Kafka
    → Spark Structured Streaming
    → Bronze
    → Silver
    → Gold
```

The Azure migration retained the same logical data flow while replacing local infrastructure with managed Azure and Databricks services.

## Migration Status

### Phase 1: Replace Local Kafka with Azure Event Hubs

**Status: Completed**

Implemented:

* Azure Event Hubs namespace and Event Hub
* Kafka-compatible Event Hubs producer
* SASL/SSL authentication using an Event Hubs connection string
* Environment-based producer configuration
* Successful AQI event publication to Event Hubs

Current limitation:

* The producer is currently executed separately and is not deployed as a scheduled Azure service.

### Phase 2: Move Spark Processing to Azure Databricks

**Status: Completed**

Implemented:

* Event Hubs ingestion using Spark Structured Streaming
* Databricks Serverless notebook execution
* Bronze, Silver, and Gold transformation notebooks
* A four-task Databricks Workflow
* Task dependencies that stop downstream processing after an upstream failure

### Phase 3: Replace Local Storage with ADLS Gen2

**Status: Completed**

Implemented:

* ADLS Gen2 lakehouse storage
* Bronze, Silver, Gold, and checkpoint locations
* Databricks Access Connector
* Unity Catalog storage credential and external location

### Phase 4: Upgrade Parquet Outputs to Delta Lake

**Status: Completed**

Implemented:

* Bronze Delta outputs
* Silver Delta outputs
* Gold Delta outputs
* Structured Streaming checkpoints
* External Gold table registration in Unity Catalog

### Phase 5: Add Azure Data Factory Orchestration

**Status: Completed for Databricks processing**

Implemented:

* Azure Data Factory pipeline
* Databricks linked service
* Databricks Job activity
* Hourly schedule trigger
* Successful triggered pipeline runs

Scope clarification:

ADF triggers the Databricks Workflow. It does not currently start the Python Event Hubs producer. The producer must publish new events separately before the processing workflow runs.

The ADF trigger was disabled after testing to avoid unnecessary costs.

### Phase 6: Add Basic Repository CI

**Status: Completed**

Implemented:

* Pinned Python dependencies
* Ruff linting
* Python syntax validation
* Unit tests for producer-side transformation logic
* Validation of exported ADF JSON files
* GitHub Actions execution on pushes and pull requests

CI does not execute live Kafka, Spark, Databricks, or Azure integration tests.

## Remaining Roadmap

### Next Priority 1: Automate the Producer

Deploy the Open-Meteo Event Hubs producer to scheduled Azure compute.

Possible implementation options:

* Azure Function with a timer trigger
* Azure Container Apps Job
* Another small scheduled container or serverless workload

Goal:

```
Scheduled producer
    → Event Hubs
    → ADF/Databricks processing
    → Updated Delta tables
```

This would make the cloud data flow autonomous rather than requiring a manual producer run.

### Next Priority 2: Expand Automated Data Tests

Add tests for:

* missing and negative AQI values
* future timestamps
* duplicate-record handling
* latest-ingestion selection
* daily aggregation calculations
* city-ranking calculations
* completeness and freshness outputs

Spark tests should use small local DataFrames and should not require Azure resources.

### Next Priority 3: Infrastructure as Code

Add Terraform or Bicep definitions for:

* Event Hubs
* ADLS Gen2
* Databricks access configuration
* Azure Data Factory
* identities and role assignments
* environment-specific parameters

Secrets should remain outside source control and should be supplied through secure deployment configuration.

### Next Priority 4: CI/CD for Cloud Assets

Extend GitHub Actions to validate and deploy:

* Databricks notebooks or bundles
* Databricks Workflow configuration
* ADF templates
* Infrastructure as Code

Cloud deployment should require explicit credentials and environment approval.

### Next Priority 5: Reporting

Connect Power BI to the registered Unity Catalog Gold tables.

Primary reporting tables:

* `daily_city_aqi`
* `daily_city_ranking`
* `data_freshness`
* `data_completeness`

### Next Priority 6: Monitoring and Reliability

Add:

* failed-record or quarantine storage
* producer execution logging
* pipeline failure notifications
* data-freshness alerts
* Azure cost alerts
* incremental Silver and Gold processing

## Current Cost State

The Azure implementation was validated through successful Event Hubs, Databricks, Unity Catalog, ADLS, and ADF runs.

The ADF schedule trigger is currently disabled. Azure resources should be reviewed or removed when they are no longer required to prevent additional charges.