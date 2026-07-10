from urllib.parse import parse_qs, urlparse

import pytest

from producer.fetch_openmeteo import (
    HOURLY_VARIABLES,
    build_records,
    build_url,
    validate_api_response,
)


TEST_CITY = {
    "city_id": 1,
    "city": "Athens",
    "country": "Greece",
    "latitude": 37.9838,
    "longitude": 23.7275,
}


def make_api_response():
    return {
        "hourly": {
            "time": [
                "2026-07-10T00:00",
                "2026-07-10T01:00",
            ],
            "nitrogen_dioxide": [20.0, 21.0],
            "pm10": [30.0, 31.0],
            "pm2_5": [15.0, 16.0],
            "european_aqi": [40, 42],
        }
    }


def test_build_url_contains_expected_parameters():
    api_url = build_url(TEST_CITY, HOURLY_VARIABLES)
    query = parse_qs(urlparse(api_url).query)

    assert query["latitude"] == [str(TEST_CITY["latitude"])]
    assert query["longitude"] == [str(TEST_CITY["longitude"])]
    assert query["timezone"] == ["auto"]
    assert query["forecast_days"] == ["1"]

    requested_variables = query["hourly"][0].split(",")

    assert requested_variables == HOURLY_VARIABLES


def test_build_records_flattens_hourly_arrays():
    records = build_records(
        TEST_CITY,
        make_api_response(),
        "2026-07-10T10:00:00+00:00",
    )

    assert len(records) == 2

    assert records[0]["city_id"] == 1
    assert records[0]["city"] == "Athens"
    assert records[0]["measurement_timestamp"] == "2026-07-10T00:00"
    assert records[0]["european_aqi"] == 40
    assert records[0]["pm10"] == 30.0
    assert records[0]["source"] == "openmeteo"


def test_validation_rejects_inconsistent_array_lengths():
    api_response = make_api_response()
    api_response["hourly"]["pm10"] = [30.0]

    with pytest.raises(
        ValueError,
        match="Inconsistent Open-Meteo hourly array lengths",
    ):
        validate_api_response(api_response)


def test_validation_rejects_missing_hourly_field():
    api_response = make_api_response()
    del api_response["hourly"]["european_aqi"]

    with pytest.raises(
        ValueError,
        match="missing hourly fields",
    ):
        validate_api_response(api_response)