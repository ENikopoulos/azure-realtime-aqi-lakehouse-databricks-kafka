import os
import json
from kafka import KafkaProducer
from dotenv import load_dotenv

# API Functions

from producer.fetch_openmeteo import (
    CITIES,
    HOURLY_VARIABLES,
    build_url,
    fetch_data,
    build_records,
)


def create_eventhub_producer():
    bootstrap_servers = os.getenv("AZURE_EVENTHUB_BOOTSTRAP_SERVERS")
    connection_string = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")

    if not bootstrap_servers or not connection_string:
        raise ValueError("Missing Azure Event Hubs configuration in .env")

    return KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        security_protocol = "SASL_SSL",
        sasl_mechanism = "PLAIN",
        sasl_plain_username = "$ConnectionString",
        sasl_plain_password = connection_string,
        value_serializer = lambda value: json.dumps(value).encode("utf-8"),
    )
def main():
    load_dotenv()

    namespace = os.getenv("AZURE_EVENTHUB_NAMESPACE")
    eventhub_name = os.getenv("AZURE_EVENTHUB_NAME")
    bootstrap_servers = os.getenv("AZURE_EVENTHUB_BOOTSTRAP_SERVERS")
    connection_string = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")

    print("Azure Event Hubs config loaded:")
    print(f"Namespace loaded: {namespace is not None}")
    print(f"Event Hub name loaded: {eventhub_name is not None}")
    print(f"Bootstrap servers loaded: {bootstrap_servers is not None}")
    print(f"Connection string loaded: {connection_string is not None}")

    if not eventhub_name:
        raise ValueError("Missing AZURE_EVENTHUB_NAME in .env")

    producer = create_eventhub_producer()

    total_sent = 0

    for city in CITIES:
        api_url = build_url(city, HOURLY_VARIABLES)
        api_response, ingestion_timestamp_utc = fetch_data(api_url)
        city_records = build_records(city, api_response, ingestion_timestamp_utc)
        for record in city_records:
            producer.send(
                topic = eventhub_name,
                value = record
            )
            total_sent += 1



    producer.flush()
    producer.close()

    print(f"Sent {total_sent} AQI records to Azure Event Hub: {eventhub_name}")


if __name__ == "__main__":
    main()