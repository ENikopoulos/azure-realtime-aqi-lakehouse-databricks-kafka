import json
from kafka import KafkaProducer

from producer.fetch_openmeteo import (
    CITIES,
    HOURLY_VARIABLES,
    build_url,
    fetch_data,
    build_records,
)


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "air_quality_raw"


def create_kafka_producer():
    """
    Create a Kafka producer that serializes Python dictionaries as JSON bytes.
    """
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def main():
    producer = create_kafka_producer()
    total_sent = 0

    for city in CITIES:
        api_url = build_url(city, HOURLY_VARIABLES)
        api_response, ingestion_timestamp_utc = fetch_data(api_url)

        records = build_records(city, api_response, ingestion_timestamp_utc)

        for record in records:
            producer.send(TOPIC_NAME, value=record)
            total_sent += 1

    producer.flush()
    producer.close()

    print(f"Sent {total_sent} records to Kafka topic: {TOPIC_NAME}")


if __name__ == "__main__":
    main()