# Imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
from pathlib import Path

# Logger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# Log function

def log_written_files(path):
    """
    Log the Parquet part files written by Spark to an output folder.
    """
    parquet_files = list(Path(path).glob("*.parquet"))

    logger.info("Wrote table to: %s", path)
    logger.info("Number of parquet files written: %s", len(parquet_files))

    for file in parquet_files:
        logger.info("Written file: %s", file)

# Main

def main():

    # Start a spark session

    spark = (
        SparkSession
            .builder
            .appName("SilverToGold")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Read Silver parquet files

    df = spark.read.parquet("data/silver/air_quality")

    # Add date column to data

    df = df.withColumn(
        "measurement_date",
        F.to_date("measurement_timestamp")
    )

    # Daily city AQI table

    daily_city_aqi_df = df.groupBy(
        "city",
        "measurement_date"
    ).agg(
        F.round(F.avg("european_aqi"), 2).alias("avg_european_aqi"),
        F.max("european_aqi").alias("max_european_aqi"),
        F.round(F.avg("pm10"), 2).alias("avg_pm10"),
        F.max("pm10").alias("max_pm10"),
        F.round(F.avg("pm2_5"), 2).alias("avg_pm2_5"),        
        F.max("pm2_5").alias("max_pm2_5"),
        F.round(F.avg("nitrogen_dioxide"), 2).alias("avg_nitrogen_dioxide"),        
        F.max("nitrogen_dioxide").alias("max_nitrogen_dioxide"),
    ).orderBy("city", "measurement_date")

    # Daily city ranking by avg_european_aqi

    daily_city_ranking_df = daily_city_aqi_df.withColumn(
        "aqi_rank",
        F.dense_rank().over(
            Window.partitionBy(
                "measurement_date").orderBy(
                F.col("avg_european_aqi").desc()
            )
        )
    )

    # Data freshness table

    data_freshness_df = df.groupBy("city").agg(
        F.max("ingestion_timestamp_utc").alias("latest_ingestion_timestamp_utc")
    )

    # Data completeness table

    data_completeness_df = df.groupBy(
        "city",
        "measurement_date"
    ).agg(
        F.count("*").alias("actual_records")
    ).withColumn("expected_records", F.lit(24)).withColumn(
        "completeness_pct", F.round(F.col("actual_records") / F.col("expected_records") * 100, 2))

    # Write to gold in parquet files

       # Output paths

    daily_city_aqi_path = "data/gold/daily_city_aqi"
    daily_city_ranking_path = "data/gold/daily_city_ranking"
    data_freshness_path = "data/gold/data_freshness"
    data_completeness_path = "data/gold/data_completeness"

    # Write to gold in parquet files

    daily_city_aqi_df.write.mode("overwrite").parquet(daily_city_aqi_path)
    log_written_files(daily_city_aqi_path)

    daily_city_ranking_df.write.mode("overwrite").parquet(daily_city_ranking_path)
    log_written_files(daily_city_ranking_path)

    data_freshness_df.write.mode("overwrite").parquet(data_freshness_path)
    log_written_files(data_freshness_path)

    data_completeness_df.write.mode("overwrite").parquet(data_completeness_path)
    log_written_files(data_completeness_path)

    # Stop spark session

    spark.stop()

if __name__ == "__main__":
    main()