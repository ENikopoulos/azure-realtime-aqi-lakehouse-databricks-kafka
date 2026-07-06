from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    spark = (
        SparkSession.builder.appName("KafkaToBronzeTest").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")


    kafka_df = (
        spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "air_quality_raw")\
        .option("startingOffsets", "earliest")\
        .load()
    )

    kafka_string_df = kafka_df.select(
        col("value").cast("string").alias("json_string")
    )

    aqi_schema = StructType([
    StructField("source", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("city_id", IntegerType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("measurement_timestamp", StringType()),
    StructField("european_aqi", IntegerType()),
    StructField("pm10", DoubleType()),
    StructField("pm2_5", DoubleType()),
    StructField("nitrogen_dioxide", DoubleType()),
    StructField("ingestion_timestamp_utc", StringType()),
])
    
    parsed_df = kafka_string_df.select(
    from_json(col("json_string"), aqi_schema).alias("data")
)

    final_df = parsed_df.select("data.*").filter(col("source") == "openmeteo")

    query = (
    final_df.writeStream
    .format("console")
    .option("truncate", "false")
    .option("numRows", 20)
    .start()
)
    print("Spark streaming query started successfully")
    print(f"Spark version: {spark.version}")

    query.awaitTermination()

    spark.stop()

if __name__ == "__main__":
    main()