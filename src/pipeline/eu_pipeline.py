from pyspark.sql import SparkSession
from src.ingestion.flight_api import fetch_flight_data
from src.processing.region_split import split_by_region

def run_eu_pipeline():
    print("-"*75)
    print("Starting EU pipeline....")


    spark = SparkSession.builder.appName("CoreSync-Eu") \
                .master("local[5]").getOrCreate()

    data = fetch_flight_data()
    eu,_ = split_by_region(data)

    rdd = spark.sparkContext.parallelize(eu,3)

    # result = rdd.map(lambda f:("EU",f[0])).count()
    

    print(f"EU FLIGHT processed: {result}")

    spark.stop()