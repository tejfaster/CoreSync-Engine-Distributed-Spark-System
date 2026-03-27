from pyspark.sql import SparkSession
from src.ingestion.flight_api import fetch_flight_data
from src.processing.region_split import split_by_region

spark = SparkSession.builder.appName("CoreSync") \
    .master("local[*]").getOrCreate()

sc = spark.sparkContext

data = fetch_flight_data()

eu, asia = split_by_region(data)

rdd = sc.parallelize([("EU",eu),("ASIA",asia)],2)

def process_region(region_data):
    region, flights = region_data
    return (region, len(flights))

result = rdd.map(process_region).collect()

print(result)