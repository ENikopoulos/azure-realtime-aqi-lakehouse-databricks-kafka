# Imports

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = "data/bronze/air_quality_raw"
SILVER_PATH = "data/silver/air_quality"

def main():
    # SparkSession

    spark = (
        SparkSession
            .builder
            .appName("BronzeToSilver")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Read Parquet

    df = spark.read.parquet(BRONZE_PATH)

    # Cast to correct datatypes

    df = df.select(
        F.col("source"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("city_id"),
        F.col("city"),
        F.col("country"),
        F.to_timestamp(F.col("measurement_timestamp")).alias("measurement_timestamp"),
        F.col("european_aqi"),
        F.col("pm10"),
        F.col("pm2_5"),
        F.col("nitrogen_dioxide"),
        F.to_timestamp(F.col("ingestion_timestamp_utc")).alias("ingestion_timestamp_utc")
    )

    # Filter data

    filtered_df = df.filter(
        (F.col("source") == "openmeteo") &
        (F.col("city_id").isNotNull()) &
        (F.col("measurement_timestamp").isNotNull()) &
        (F.col("european_aqi").isNotNull())
    )

    # Flag data

    flagged_df = filtered_df.withColumns({
        "is_valid_aqi": (F.col("european_aqi").isNotNull()) & (F.col("european_aqi") >= 0),
        "is_valid_pm10": (F.col("pm10").isNotNull()) & (F.col("pm10") >= 0),
        "is_valid_pm2_5": (F.col("pm2_5").isNotNull()) & (F.col("pm2_5") >= 0),
        "is_valid_nitrogen_dioxide": (F.col("nitrogen_dioxide").isNotNull()) & (F.col("nitrogen_dioxide") >= 0),
        "is_not_future_measurement": F.col("measurement_timestamp") <= F.current_timestamp()
    }
    )

    silver_df = flagged_df.filter(
        F.col("is_valid_aqi") &
        F.col("is_valid_pm10") &
        F.col("is_valid_pm2_5") &
        F.col("is_valid_nitrogen_dioxide") &
        F.col("is_not_future_measurement")
    )

    # Deduplication

    dedupe_window = Window.partitionBy(
        "city_id",
        "measurement_timestamp"
    ).orderBy(
        F.desc("ingestion_timestamp_utc")
    )

    deduped_df = silver_df.withColumn(
        "rn", 
        F.row_number().over(dedupe_window)
    ).filter(
        F.col("rn") == 1
    ).drop("rn")

    # Write to silver

    deduped_df.write.mode("overwrite").parquet(SILVER_PATH)

    spark.stop()

if __name__ == "__main__":
    main()