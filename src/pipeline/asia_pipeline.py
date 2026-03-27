from pyspark.sql import SparkSession
from src.ingestion.flight_api import fetch_flight_data
from src.processing.region_split import split_by_region

def run_asia_pipeline():
    print("-"*75)
    print("Starting ASIA pipeline.....")

    spark = SparkSession.builder.appName("CoreSync_Asia") \
        .master("local[5]").getOrCreate()
    
    data = fetch_flight_data()
    _,asia = split_by_region(data)

    rdd = spark.sparkContext.parallelize(asia,3)

    result = rdd.map(lambda f: ("ASIS",f[0])).count()

    print(f"ASIA FLIGHTS processed: {result}")

    spark.stop()