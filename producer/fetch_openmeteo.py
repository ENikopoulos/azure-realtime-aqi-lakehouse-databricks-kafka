# Imports

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REQUEST_TIMEOUT_SECONDS = 30
MAX_FETCH_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

HOURLY_VARIABLES = [
    "nitrogen_dioxide",
    "pm10",
    "pm2_5",
    "european_aqi",
]


CITIES = [
    {
        "city_id": 1,
        "city": "Athens",
        "country": "Greece",
        "latitude": 37.9838,
        "longitude": 23.7275,
    },
    {
        "city_id": 2,
        "city": "Thessaloniki",
        "country": "Greece",
        "latitude": 40.6401,
        "longitude": 22.9444,
    },
    {
        "city_id": 3,
        "city": "London",
        "country": "United Kingdom",
        "latitude": 51.5072,
        "longitude": -0.1276,
    },
    {
        "city_id": 4,
        "city": "Paris",
        "country": "France",
        "latitude": 48.8566,
        "longitude": 2.3522,
    },
    {
        "city_id": 5,
        "city": "Berlin",
        "country": "Germany",
        "latitude": 52.5200,
        "longitude": 13.4050,
    },
]


def build_url(city, hourly):
    """
    Build the Open-Meteo Air Quality API URL for one city.

    Parameters:
        city (dict): Dictionary containing city metadata such as latitude and longitude.
        hourly (list): List of hourly air-quality variables to request from the API.

    Returns:
        str: Fully encoded API URL with query parameters.
    """

    base_url = "https://air-quality-api.open-meteo.com/v1/air-quality"

    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(hourly),
        "timezone": "auto",
        "forecast_days": 1,
    }

    return base_url + "?" + urllib.parse.urlencode(params)

def validate_api_response(api_response):
    """Validate the hourly arrays required to build AQI event records."""

    hourly = api_response.get("hourly")

    if not isinstance(hourly, dict):
        raise ValueError("Open-Meteo response is missing the 'hourly' object.")

    required_fields = ["time", *HOURLY_VARIABLES]

    missing_fields = [
        field for field in required_fields if field not in hourly
    ]

    if missing_fields:
        raise ValueError(
            f"Open-Meteo response is missing hourly fields: {missing_fields}"
        )

    non_list_fields = [
        field
        for field in required_fields
        if not isinstance(hourly[field], list)
    ]

    if non_list_fields:
        raise ValueError(
            f"Expected hourly fields to be arrays: {non_list_fields}"
        )

    field_lengths = {
        field: len(hourly[field])
        for field in required_fields
    }

    if field_lengths["time"] == 0:
        raise ValueError("Open-Meteo response contains no hourly records.")

    if len(set(field_lengths.values())) != 1:
        raise ValueError(
            f"Inconsistent Open-Meteo hourly array lengths: {field_lengths}"
        )


def fetch_data(
    api_url,
    max_attempts=MAX_FETCH_ATTEMPTS,
    retry_backoff_seconds=RETRY_BACKOFF_SECONDS,
):
    """
    Fetch and validate Open-Meteo data.

    Retries temporary server, rate-limit, network, timeout, decoding,
    and response-validation failures using exponential backoff.
    """

    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(
                api_url,
                timeout=REQUEST_TIMEOUT_SECONDS,
            ) as response:
                response_body = response.read().decode("utf-8")

            api_response = json.loads(response_body)
            validate_api_response(api_response)

            ingestion_timestamp_utc = datetime.now(
                timezone.utc
            ).isoformat()

            return api_response, ingestion_timestamp_utc

        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode(
                "utf-8",
                errors="replace",
            )

            print(f"Open-Meteo HTTP error {exc.code}: {error_body}")

            retryable_http_error = (
                exc.code == 429 or 500 <= exc.code < 600
            )

            if not retryable_http_error or attempt == max_attempts:
                raise

            retry_reason = f"HTTP {exc.code}"

        except (
            urllib.error.URLError,
            TimeoutError,
            UnicodeDecodeError,
            ValueError,
        ) as exc:
            if attempt == max_attempts:
                raise

            retry_reason = str(exc)

        retry_delay = retry_backoff_seconds * (2 ** (attempt - 1))

        print(
            f"Open-Meteo request attempt {attempt} failed: "
            f"{retry_reason}. Retrying in {retry_delay} seconds."
        )

        time.sleep(retry_delay)

    raise RuntimeError("Open-Meteo request failed unexpectedly.")


def build_records(city, api_response, ingestion_timestamp_utc):
    """
    Convert one Open-Meteo API response into flat event records.

    The Open-Meteo response contains hourly arrays. This function transforms
    those arrays into a list of dictionaries, where each dictionary represents
    one city and one measurement timestamp.

    Parameters:
        city (dict): Dictionary containing city metadata.
        api_response (dict): Parsed API response from Open-Meteo.
        ingestion_timestamp_utc (str): UTC timestamp when the API response was fetched.

    Returns:
        list: List of event records ready to be saved locally or sent to Kafka.
    """
    validate_api_response(api_response)

    event_records = []

    for i in range(len(api_response["hourly"]["time"])):
        event_records.append(
            {
                "source": "openmeteo",
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "city_id": city["city_id"],
                "city": city["city"],
                "country": city["country"],
                "measurement_timestamp": api_response["hourly"]["time"][i],
                "european_aqi": api_response["hourly"]["european_aqi"][i],
                "pm10": api_response["hourly"]["pm10"][i],
                "pm2_5": api_response["hourly"]["pm2_5"][i],
                "nitrogen_dioxide": api_response["hourly"]["nitrogen_dioxide"][i],
                "ingestion_timestamp_utc": ingestion_timestamp_utc,
            }
        )

    return event_records


def main():
    """
    Run the local Open-Meteo ingestion workflow.

    For each configured city, this function builds the API URL, fetches the
    air-quality response, converts the response into flat event records, prints
    a small validation summary, and writes the records to a local sample JSON file.

    This is the entry point for testing the API-to-records step before adding Kafka.
    """

    records = []

    for city in CITIES:
        api_url = build_url(city, HOURLY_VARIABLES)
        api_response, ingestion_timestamp_utc = fetch_data(api_url)

        city_records = build_records(city, api_response, ingestion_timestamp_utc)
        records.extend(city_records)

    print(records[0])
    print(f"Total records: {len(records)}")

    output_path = Path("data/samples/sample_records.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


if __name__ == "__main__":
    main()