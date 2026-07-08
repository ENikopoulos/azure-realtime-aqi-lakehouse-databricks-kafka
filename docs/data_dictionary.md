# Data Dictionary

## Source

Open-Meteo Air Quality API.

The pipeline collects hourly air-quality variables for selected cities.

## Core Fields

| Field | Type | Layer | Description |
|---|---:|---|---|
| source | string | Bronze/Silver/Gold | Source system name. Expected value: `openmeteo`. |
| latitude | double | Bronze/Silver | City latitude. |
| longitude | double | Bronze/Silver | City longitude. |
| city_id | integer | Bronze/Silver | Internal numeric city identifier. |
| city | string | Bronze/Silver/Gold | City name. |
| country | string | Bronze/Silver | Country name. |
| measurement_timestamp | timestamp | Silver | Timestamp of the AQI measurement. |
| ingestion_timestamp_utc | timestamp | Silver/Gold | Timestamp when the API response was ingested. |
| european_aqi | integer | Bronze/Silver/Gold | European AQI value. |
| pm10 | double | Bronze/Silver/Gold | PM10 pollutant value. |
| pm2_5 | double | Bronze/Silver/Gold | PM2.5 pollutant value. |
| nitrogen_dioxide | double | Bronze/Silver/Gold | Nitrogen dioxide pollutant value. |

## Silver Validation Flags

| Field | Type | Description |
|---|---:|---|
| is_valid_aqi | boolean | True when `european_aqi` is not null and non-negative. |
| is_valid_pm10 | boolean | True when `pm10` is not null and non-negative. |
| is_valid_pm2_5 | boolean | True when `pm2_5` is not null and non-negative. |
| is_valid_nitrogen_dioxide | boolean | True when `nitrogen_dioxide` is not null and non-negative. |
| is_not_future_measurement | boolean | True when `measurement_timestamp` is not later than the current timestamp. |

## Gold Tables

### daily_city_aqi

Grain:

```text
city + measurement_date
```

Metrics:

| Field | Description |
|---|---|
| avg_european_aqi | Average daily European AQI by city. |
| max_european_aqi | Maximum daily European AQI by city. |
| avg_pm10 | Average daily PM10 by city. |
| max_pm10 | Maximum daily PM10 by city. |
| avg_pm2_5 | Average daily PM2.5 by city. |
| max_pm2_5 | Maximum daily PM2.5 by city. |
| avg_nitrogen_dioxide | Average daily nitrogen dioxide by city. |
| max_nitrogen_dioxide | Maximum daily nitrogen dioxide by city. |

### daily_city_ranking

Ranks cities by daily average European AQI.

Rank meaning:

```text
aqi_rank = 1 means highest/worst average AQI for that date
```

### data_freshness

Tracks the latest ingestion timestamp per city.

### data_completeness

Counts actual records per city/date and compares them to expected records.

Current simple expectation:

```text
expected_records = 24
```

Note: Current partial days may naturally have fewer than 24 records.
