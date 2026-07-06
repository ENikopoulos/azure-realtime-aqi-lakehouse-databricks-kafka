from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder.appName("KafkaToBronzeTest").getOrCreate()
    )

    print("Spark started successfully")
    print(f"Spark version: {spark.version}")

    spark.stop()

if __name__ == "__main__":
    main()